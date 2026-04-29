use tokio::time;

#[allow(dead_code)]
pub const SPARK_APP_INACTIVE_TIMEOUT: u64 = 2 * 60 * 60;
pub const SPARK_APP_TIMEOUT: u64 = 10 * 60 * 60;
const MATCH_STR: &str = "app-id";

async fn compute_worker_usage(
    pid: u32,
    ccm_master_area: &str,
    worker_name: &str,
    logger_name: &str,
    max_cores: i64,
    max_memory: i64,
) {
    // Traverses through executor pid's and saves the executor usage for a given process ID.
    // Returns true if worker is running any executors and false otherwise.
    traverse_and_save_executor_usage(
        pid,
        ccm_master_area,
        worker_name,
        logger_name,
        max_cores,
        max_memory,
    )
    .await;
}

async fn traverse_and_save_executor_usage(
    ppid: u32,
    ccm_master_area: &str,
    worker_name: &str,
    logger_name: &str,
    max_cores: i64,
    max_memory: i64,
) {
    // Traverses the process tree and saves usage for processes with a matching "app-id" command line argument.
    let process = match procfs::process::Process::new(ppid as i32) {
        Ok(proc) => proc,
        Err(_) => return,
    };
    let cmd = process.cmdline().unwrap_or_default();
    if let Some(index) = cmd.iter().position(|x| x.contains(MATCH_STR)) {
        let app_id = cmd[index + 1].clone();
        save_executor_usage(
            &app_id,
            ppid,
            ccm_master_area,
            worker_name,
            max_cores,
            max_memory,
        )
        .await;
        return;
    }
    let tasks = match process.tasks() {
        Ok(tasks) => tasks,
        Err(_) => return,
    };
    for task in tasks
        .filter(|x| x.is_ok() && x.as_ref().unwrap().children().is_ok())
        .map(|x| x.unwrap())
    {
        for child in task.children().unwrap() {
            let _ = Box::pin(traverse_and_save_executor_usage(
                child,
                ccm_master_area,
                worker_name,
                logger_name,
                max_cores,
                max_memory,
            ))
            .await;
        }
    }
}

pub async fn get_spark_worker_job(
    job_name: String,
    cluster_home: &str,
    spark_master_url: &str,
    workarea: &str,
    cores: i64,
    memory: i64,
    protocol: &str,
) -> (serde_json::Value, String) {
    let command = format!(
        "{} {} -c {} -m {}G --work-dir {}",
        get_spark_worker_script(cluster_home).await,
        spark_master_url,
        cores,
        memory,
        workarea,
    );
    let spec = serde_json::json!({"cores": cores, "memory": memory});
    (
        serde_json::json!({
            "job_name": job_name,
            "job_type": "spark_worker",
            "lightweight": false,
            "specs": vec![spec],
            "protocol": protocol,
        }),
        command,
    )
}

#[allow(dead_code)]
pub async fn get_spark_log_dir(workarea: &str) -> String {
    let mut log_path = std::path::PathBuf::from(workarea);
    log_path.push("logs");
    log_path.push("worker_logs");
    log_path.to_str().unwrap().to_string()
}

#[allow(dead_code)]
pub async fn get_spark_pid_dir(workarea: &str) -> String {
    let mut pid_path = std::path::PathBuf::from(workarea);
    pid_path.push("worker_pids");
    pid_path.to_str().unwrap().to_string()
}

#[allow(dead_code)]
pub async fn set_spark_env(
    job_name: &str,
    spark_webui_port: &u16,
    cluster_home: &str,
    workarea: &str,
) -> std::collections::HashMap<String, String> {
    let py_binary = super::get_python_binary(cluster_home).await;
    std::collections::HashMap::from([
        (
            String::from("SPARK_LOG_DIR"),
            get_spark_log_dir(workarea).await,
        ),
        (String::from("SPARK_IDENT_STRING"), job_name.to_owned()),
        (
            String::from("SPARK_PID_DIR"),
            get_spark_pid_dir(workarea).await,
        ),
        (
            String::from("SPARK_WORKER_WEBUI_PORT"),
            spark_webui_port.to_string(),
        ),
        (String::from("PYSPARK_PYTHON"), py_binary.clone()),
        (String::from("PYSPARK_DRIVER_PYTHON"), py_binary),
        (String::from("SPARK_NO_DAEMONIZE"), String::from("true")),
    ])
}

async fn get_reqd_executors_from_metrics(metrics: &serde_json::Value, app_id: &str) -> i64 {
    let mut number_of_requested_workers = 0;
    let target_executors_key = &format!(
        "{}.driver.ExecutorAllocationManager.executors.numberTargetExecutors",
        app_id
    );
    let total_executors_key = &format!(
        "{}.driver.ExecutorAllocationManager.executors.numberAllExecutors",
        app_id
    );
    let total_executors = metrics
        .get("gauges")
        .unwrap()
        .get(total_executors_key)
        .map_or(serde_json::json!({"value": 0}), |x| x.clone())
        .get("value")
        .map_or(serde_json::json!(0), |x| x.clone())
        .as_i64()
        .map_or(0, |x| x);
    let target_executors = metrics
        .get("gauges")
        .unwrap()
        .get(target_executors_key)
        .map_or(serde_json::json!({"value": 0}), |x| x.clone())
        .get("value")
        .map_or(serde_json::json!(0), |x| x.clone())
        .as_i64()
        .map_or(0, |x| x);
    number_of_requested_workers += (target_executors - total_executors) as i64;
    std::cmp::max(number_of_requested_workers, 0)
}

#[allow(dead_code)]
pub async fn get_required_executors(url: &str, app_id: &str) -> Result<i64, ()> {
    let mut retry = 0;
    while retry < 2 {
        match super::submit_get_request(&format!("{}/metrics/json/", url)).await {
            Ok(response) => return Ok(get_reqd_executors_from_metrics(&response, app_id).await),
            Err(_) => {
                time::sleep(time::Duration::from_secs(10)).await;
                retry += 1;
            }
        }
    }
    Err(())
}

async fn get_spark_worker_script(cluster_home: &str) -> String {
    let deps_path = std::path::Path::new(cluster_home)
        .parent()
        .unwrap()
        .join("deps");
    format!(
        "{}/python/lib/python3.11/site-packages/pyspark/sbin/start-slave-original.sh",
        deps_path.display().to_string()
    )
}

pub async fn get_driver_metrics(url: &str, id: &str) -> Result<serde_json::Value, ()> {
    let mut total_cores: i64 = 0;
    let mut memory: i64 = 0;
    let mut total_tasks: i64 = 0;
    let mut total_shuffle_read: i64 = 0;
    let mut total_shuffle_write: i64 = 0;
    let executors = match get_executors_data(url, id).await {
        Ok(executors) => executors,
        Err(_) => return Err(()),
    };
    for executor in &executors {
        memory += executor["memoryUsed"].as_i64().unwrap_or(0);
        total_cores += executor["totalCores"].as_i64().unwrap_or(0);
        total_tasks += executor["totalTasks"].as_i64().unwrap_or(0);
        total_shuffle_read += executor["totalShuffleRead"].as_i64().unwrap_or(0);
        total_shuffle_write += executor["totalShuffleWrite"].as_i64().unwrap_or(0);
    }
    return Ok(serde_json::json!({
        "total_cores_used": total_cores,
        "total_memory_used": memory,
        "total_tasks": total_tasks,
        "total_shuffle_read": total_shuffle_read,
        "total_shuffle_write": total_shuffle_write,
        "total_executors": executors.len()-1,
    }));
}

pub async fn get_executors_data(url: &str, id: &str) -> Result<Vec<serde_json::Value>, ()> {
    match super::submit_get_request(&format!("{}/api/v1/applications/{}/allexecutors", url, id))
        .await
    {
        Ok(response) => match response.as_array() {
            Some(executors) => Ok(executors.to_vec()),
            None => Err(()),
        },
        Err(_) => Err(()),
    }
}

pub async fn get_driver_wait_time(
    url: &str,
    id: &str,
    logger_name: &str,
    wait_instant: &tokio::time::Instant,
) -> Option<u64> {
    // Wait for executors to get added to this driver
    // Once executors are added, start monitoring resource usage of this driver
    match get_executors_data(url, id).await {
        Ok(executors) => {
            if executors.len() > 1 {
                let wait_time = time::Instant::now().duration_since(*wait_instant).as_secs();
                log::info!(target: logger_name, "Wait time: {}", wait_time);
                return Some(wait_time);
            }
        }
        Err(_) => {}
    }
    return None;
}

pub async fn clean_spark_work_area(ccm_worker_area: &str, app_id: &str) -> Result<(), String> {
    let path = format!("{}/{}", ccm_worker_area, app_id);
    let mut error = format!("Failed to remove workarea {}", path);
    for _ in 0..2 {
        match tokio::fs::remove_dir_all(&path).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                error = format!("Failed to remove workarea {}. Reason - {}", path, e);
            }
        }
    }
    return Err(error);
}

async fn is_worker_busy(worker_url: &str) -> bool {
    let endpoint = format!("{}/json/", worker_url);
    let response = match super::submit_get_request(&endpoint).await {
        Ok(response) => response,
        Err(_) => return false,
    };
    // Check if the response contains executors
    let executors_array = response.get("executors").map(|v| v.as_array().unwrap());
    executors_array.is_some() & !executors_array.unwrap().is_empty()
}

pub async fn monitor_spark_worker(
    web_url: &str,
    pid: u32,
    ccm_master_area: &str,
    worker_name: &str,
    logger_name: &str,
    max_cores: i64,
    max_memory: i64,
) {
    // Monitor the Spark worker process and save its resource usage.
    // If worker is not busy retries twice.
    let mut retry = 0;
    while retry < 2 {
        time::sleep(time::Duration::from_secs(60)).await;
        if is_worker_busy(web_url).await {
            compute_worker_usage(
                pid,
                ccm_master_area,
                worker_name,
                logger_name,
                max_cores,
                max_memory,
            )
            .await;
            // If worker is busy, reset retry count
            retry = 0;
        } else {
            // If worker is not busy, increment retry count
            retry += 1;
        }
    }
}

async fn save_executor_usage(
    app_id: &str,
    pid: u32,
    ccm_master_area: &str,
    worker_name: &str,
    _max_cores: i64,
    max_memory: i64,
) {
    // Saves the executor usage for a given application ID and process ID.
    // The usage is saved in a JSON file named with the application ID and process ID.
    let file_name = format!("{}/{}/{}_{}.csv", ccm_master_area, app_id, worker_name, pid);
    if !std::path::Path::new(&file_name).exists() {
        let _ = super::create_usage_csv(&file_name).await;
    }
    let mut resource_usage = super::init_usage_data().await;
    let _ = super::monitor_worker(pid, &mut resource_usage).await;

    let csv_lines = &format!(
        "{},{},{},{},{},{},{}\n",
        chrono::prelude::Utc::now().timestamp(),
        pid,
        local_ip_address::local_ip().unwrap(),
        resource_usage.get("memory_usage").unwrap(),
        resource_usage.get("disk_read_bytes").unwrap(),
        resource_usage.get("disk_write_bytes").unwrap(),
        max_memory * 1024 * 1024 * 1024,
    );
    let _ = super::append_data_to_file(&file_name, &csv_lines).await;
}

async fn get_active_jobs(metrics: &serde_json::Value, app_id: &str) -> i64 {
    // Retrieves the number of active stages for a given application ID.
    //
    // # Arguments
    //
    // * `metrics` - Spark metrics JSON response.
    // * `app_id` - App id.
    //
    // # Returns
    //
    // * Number of active stages.
    let active_stages_key = &format!("{}.driver.DAGScheduler.job.activeJobs", app_id);
    metrics
        .get("gauges")
        .unwrap()
        .get(active_stages_key)
        .map_or(0, |x| x["value"].as_i64().unwrap_or(0))
}

#[allow(dead_code)]
pub async fn is_app_active(url: &str, app_id: &str) -> Result<bool, ()> {
    // Retrieves the status of Spark jobs for a given application ID.
    //
    // # Arguments
    //
    // * `url` - Spark App URL.
    // * `app_id` - App id.
    //
    // # Returns
    //
    // * `true` if there are active jobs in the Spark app or app requests for spark workers, `false` otherwise.
    let mut retry = 0;
    let endpoint = format!("{}/metrics/json", url);
    while retry < 2 {
        match super::submit_get_request(&endpoint).await {
            Ok(metrics) => {
                return Ok(get_active_jobs(&metrics, app_id).await > 0
                    || get_reqd_executors_from_metrics(&metrics, app_id).await > 0);
            }
            Err(_) => {
                time::sleep(time::Duration::from_secs(10)).await;
                retry += 1;
            }
        };
    }
    Err(())
}

#[allow(dead_code)]
pub async fn set_port_for_ccm_spark_driver(current_dir: &str) -> Result<u16, String> {
    let ui_port = super::get_unused_port().await;
    let conf_path = format!("{}/.conf/spark_props.conf", current_dir);
    if !std::path::Path::new(&conf_path).exists() {
        return Err(format!(" {} does not exist", conf_path));
    };
    match super::append_data_to_file(&conf_path, &format!("spark.ui.port={}\n", ui_port)).await {
        Ok(_) => Ok(ui_port),
        Err(e) => Err(format!("Failed to write to {}: {}", conf_path, e)),
    }
}

#[allow(dead_code)]
pub async fn get_app_id_from_driver(
    url: &str,
    pid: u32,
) -> Option<String> {
    let pid = pid as i32;
    if let Ok(process) = procfs::process::Process::new(pid) {
        // Retry until the process is alive to get app id
        loop {
            if !process.is_alive() {
                return None;
            }
            match super::submit_get_request(&format!("{}/api/v1/applications", url)).await {
                Ok(response) => {
                    if let Some(apps) = response.as_array() {
                        for app in apps {
                            if let Some(app_id) = app["id"].as_str() {
                                return Some(app_id.to_string());
                            }
                        }
                    }
                }
                Err(_) => {
                    time::sleep(time::Duration::from_secs(10)).await;
                }
            }
        }
    }
    None
}

#[allow(dead_code)]
pub async fn get_spark_inactive_timeout() -> u64 {
    // Returns the Spark application inactive timeout.
    std::env::var("SPARK_APP_INACTIVE_TIMEOUT")
        .map_or(SPARK_APP_INACTIVE_TIMEOUT, |val| {
            val.parse::<u64>().unwrap_or(SPARK_APP_INACTIVE_TIMEOUT)
        })
}

#[allow(dead_code)]
pub async fn get_spark_app_timeout() -> u64 {
    // Returns the Spark application inactive timeout.
    std::env::var("SPARK_APP_TIMEOUT")
        .map_or(SPARK_APP_TIMEOUT, |val| {
            val.parse::<u64>().unwrap_or(SPARK_APP_TIMEOUT)
        })
}

#[allow(dead_code)]
pub async fn fail_spark_app_on_timeout() -> bool {
    // Returns the Spark application inactive timeout.
    std::env::var("SPARK_APP_FAIL_ON_TIMEOUT")
        .map_or(true, |val| {
            val != "false" 
        })
}
