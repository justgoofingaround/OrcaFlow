use log::info;
use redb::Result;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Duration};

use crate::common_utils::{
    append_data_to_file, check_disk_space, clean_spark_work_area, fail_spark_app_on_timeout,
    get_app_id_from_driver, get_driver_metrics, get_driver_wait_time, get_flow_and_task_inst,
    get_hash, get_required_executors, get_spark_app_timeout, get_spark_inactive_timeout,
    get_spark_worker_job, get_unused_port, is_app_active, kill_pid, move_csv_files,
    set_port_for_ccm_spark_driver, LOCAL_DISK_THRESHOLD,
};
use crate::data_types::Status;
use crate::grpc::proto::{Job, SparkApp};
use crate::CONFIG;

pub async fn get_spark_log_dir(work_area: &str) -> String {
    let mut log_path = PathBuf::from(work_area);
    log_path.push("logs");
    log_path.push("worker_logs");
    log_path.to_str().unwrap().to_string()
}

pub async fn get_spark_pid_dir(work_area: &str) -> String {
    let mut pid_path = PathBuf::from(work_area);
    pid_path.push("worker_pids");
    pid_path.to_str().unwrap().to_string()
}

pub async fn set_spark_env(
    job_hash: &u64,
    spark_webui_port: &u16,
    cluster_home: &str,
    work_area: &str,
) -> std::collections::HashMap<String, String> {
    let py_binary = crate::common_utils::get_python_binary(cluster_home).await;
    let java_home = crate::common_utils::get_java_home(cluster_home).await;
    std::collections::HashMap::from([
        (
            String::from("SPARK_LOG_DIR"),
            get_spark_log_dir(work_area).await,
        ),
        (
            String::from("SPARK_PID_DIR"),
            get_spark_pid_dir(work_area).await,
        ),
        (String::from("SPARK_IDENT_STRING"), job_hash.to_string()),
        (
            String::from("SPARK_WORKER_WEBUI_PORT"),
            spark_webui_port.to_string(),
        ),
        (String::from("PYSPARK_PYTHON"), py_binary.clone()),
        (String::from("PYSPARK_DRIVER_PYTHON"), py_binary),
        (String::from("SPARK_NO_DAEMONIZE"), String::from("true")),
        (String::from("JAVA_HOME"), java_home.clone()),
        (String::from("JAVA_ROOT"), java_home.clone()),
        (String::from("JAVA_BINDIR"), format!("{}/bin", java_home)),
        (String::from("JRE_HOME"), java_home),
    ])
}

pub async fn wait(pid: u32, child: Child, mut spark_url: Option<String>, job: Job) {
    let job_name = match job.job_name.as_ref() {
        Some(name) => name,
        None => "spark",
    };
    let json_name = match get_flow_and_task_inst(job_name).await {
        Ok((_, task)) => task,
        Err(_) => job_name.to_string(),
    };
    let logger_name = &CONFIG.get().unwrap().logger_name;
    let spec = job
        .lightweight_spec
        .unwrap_or(job.spec.as_ref().unwrap().clone());
    if job.job_type == "ccm-spark-driver" && spark_url.is_some() {
        let url = spark_url.unwrap();
        // Setting spark url as None as this is not a spark worker job
        spark_url = None;
        // Spawning a new task to monitor spark job
        if let Some(app_id) = get_app_id_from_driver(&url, pid).await {
            let app = crate::grpc::proto::SparkApp {
                job_name: job_name.to_string(),
                id: app_id,
                url: url,
                spec: job.spec,
                cache_dir: job.cache_dir.clone(),
            };
            tokio::spawn(monitor_spark_app(app, Some(pid), false));
        } else {
            log::error!(target: logger_name, "Failed to get app id for spark job with url {}", url);
        }
    }
    let (status, message) = match crate::common_utils::wait(
        child,
        &CONFIG.get().unwrap().logger_name,
        &spark_url,
        &job.cache_dir,
        &json_name,
        &CONFIG.get().unwrap().master_workarea,
        &CONFIG.get().unwrap().logger_name,
        spec.cores,
        spec.memory,
    )
    .await
    {
        Ok(()) => (Status::Success, None),
        Err(message) => (Status::Failed, Some(message)),
    };
    info!(target: &CONFIG.get().unwrap().logger_name, "Worker with pid: {} completed", pid);
    let mut guard = CONFIG.get().unwrap().running_jobs.write().await;
    guard.remove(&job.job_hash);
    info!(target: &CONFIG.get().unwrap().logger_name,
        "Removed job hash: {} from running jobs. Remaining jobs: {:?}",
        job.job_hash, guard
    );
    // Spawn a task to monitor idle state
    if guard.is_empty() {
        tokio::spawn(crate::utils::monitor_idle_state());
    }
    drop(guard);
    super::update_job_status(
        job.job_hash,
        &status,
        message,
        &CONFIG.get().unwrap().master_url,
        &CONFIG.get().unwrap().logger_name,
        CONFIG.get().unwrap().worker_hash,
    )
    .await;
    info!(target: &CONFIG.get().unwrap().logger_name, "Setting worker to idle state.");
}

pub async fn monitor_idle_state() {
    // This code block is responsible for monitoring the execution of jobs in the cluster manager.
    // If a notification is received, it logs a message and exits the monitoring.
    // If the sleep duration elapses, it checks if there are any running jobs.
    // If there are no running jobs, it sends a status to the master for termination .
    let logger_name = &CONFIG.get().unwrap().logger_name;
    let notify = &CONFIG.get().unwrap().notifier;
    let mut notified = notify.notified();
    let running_jobs = std::sync::Arc::clone(&CONFIG.get().unwrap().running_jobs);
    info!(target: logger_name, "Monitoring idle state. Waiting for notifications or timeout.");
    let mut retry = 0;
    while retry < 2 {
        tokio::select! {
            _ = notified => {
                info!(target: logger_name, "Received running state notification. Exiting monitoring.");
                break;
            }
            _ = sleep(Duration::from_secs(150)) => {
                // Retries twice to send message to kill idle worker.
                // If no running jobs are found still, then exit the process
                notified = notify.notified();
                let guard = running_jobs.read().await;
                let is_empty = guard.is_empty();
                drop(guard);
                if is_empty {
                    info!(target: logger_name, "No running jobs. Sending status to master for termination.");
                    let _ = super::kill_idle_worker(CONFIG.get().unwrap().worker_hash,
                    &CONFIG.get().unwrap().master_url,
                    logger_name)
                    .await;
                    retry += 1;
                }
                 else {
                    break;
                }
            }
        }
    }
    if retry == 2 {
        let _ = crate::common_utils::kill_pid(
            std::process::id(),
            &CONFIG.get().unwrap().logger_name,
            true,
        )
        .await;
        std::process::exit(0);
    }
}

pub async fn monitor_spark_app(spark_app: SparkApp, pid: Option<u32>, monitor_app: bool) {
    if spark_app.id.is_empty() {
        log::error!(target: &CONFIG.get().unwrap().logger_name, "Spark app id is empty. Exiting monitoring.");
        return;
    }
    spark_app.monitor_app(pid, monitor_app).await;
}

#[allow(unused_mut)]
pub async fn execute_command(job: &Job) -> Result<(Child, u32, Option<String>), String> {
    // Spawn a child process
    // This will kill the spawned process when the child goes out of scope
    log::info!(target: &CONFIG.get().unwrap().logger_name, "Checking disk space using envs {:?}.", job.envs);
    match check_disk_space(
        job.envs.get("SPARK_LOCAL_DISK").unwrap_or(&format!("/tmp")),
        job.envs
            .get("DISK_THRESHOLD")
            .unwrap_or(&LOCAL_DISK_THRESHOLD.to_string())
            .parse::<u64>()
            .unwrap_or(LOCAL_DISK_THRESHOLD),
        &job.job_type,
        &CONFIG.get().unwrap().protocol,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            return Err(e);
        }
    }
    let guard = CONFIG.get().unwrap().running_jobs.read().await;
    if guard.is_empty() {
        CONFIG.get().unwrap().notifier.notify_waiters();
    }
    drop(guard);
    info!(target: &CONFIG.get().unwrap().logger_name, "Executing {}", job.command);
    let mut env = std::collections::HashMap::from([(
        String::from("num_cores"),
        CONFIG.get().unwrap().max_cores.to_string(),
    )]);
    let current_dir = match job.cache_dir.as_ref() {
        Some(cache_dir) => cache_dir,
        None => &CONFIG.get().unwrap().master_workarea,
    };
    let mut spark_webui_url = None;
    if job.job_type == "spark_worker" {
        let free_port = get_unused_port().await;
        env.extend(
            set_spark_env(
                &job.job_hash,
                &free_port,
                &CONFIG.get().unwrap().cluster_home,
                &CONFIG.get().unwrap().master_workarea,
            )
            .await,
        );
        spark_webui_url = Some(format!(
            "http://{}:{}",
            local_ip_address::local_ip().unwrap(),
            free_port
        ));
    } else if job.job_type == "ccm-spark-driver" {
        let port = match set_port_for_ccm_spark_driver(current_dir).await {
            Ok(port) => port,
            Err(_) => {
                return Err(format!(
                    "Failed to set port for ccm spark driver. Reason - {}",
                    "Unknown error"
                ));
            }
        };
        env.insert(
            String::from("SPARK_LOCAL_HOSTNAME"),
            local_ip_address::local_ip().unwrap().to_string(),
        );
        env.insert(
            String::from("SPARK_LOCAL_IP"),
            local_ip_address::local_ip().unwrap().to_string(),
        );
        if let Some(cache_dir) = job.cache_dir.as_ref() {
            let conf_path = format!("{}/.conf/spark_props.conf", cache_dir);
            let _ = append_data_to_file(
                &conf_path,
                &format!(
                    "spark.driver.bindAddress={}",
                    local_ip_address::local_ip().unwrap().to_string()
                ),
            )
            .await;
        }
        spark_webui_url = Some(format!(
            "http://{}:{}",
            local_ip_address::local_ip().unwrap(),
            port
        ));
    }
    env.extend(job.envs.clone());
    let (stdout, stderr) = match job.job_type.contains("flow-engine") {
        true => (Stdio::null(), Stdio::null()),
        false => (Stdio::piped(), Stdio::piped()),
    };
    let mut child = match Command::new("sh")
        .arg("-c")
        .arg(&job.command)
        .envs(env)
        .stderr(stderr)
        .stdout(stdout)
        .current_dir(current_dir)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return Err(format!(
                "Failed to execute {}. Reason - {}",
                job.command, error
            ));
        }
    };
    let pid = child.id().unwrap();
    let mut guard = CONFIG.get().unwrap().running_jobs.write().await;
    guard.insert(job.job_hash, pid);
    drop(guard);
    info!(target: &CONFIG.get().unwrap().logger_name, "Worker with pid: {} started", pid);
    return Ok((child, pid, spark_webui_url));
}

impl SparkApp {
    pub async fn monitor_app(&self, pid: Option<u32>, monitor_app: bool) {
        // This code block is resposible to monitor the spark application registered.
        // It checks for how many executors are required by the driver and sends the request to the master to allocate workers accordingly
        // Checks the stats of the driver and writes it to json file inside cache directory
        // Checks if any jobs are running and if no jobs are running for more than an hour assumes that the app is stucks and kills the driver
        // TODO: Checks if driver is in waiting state for more than an hour, if yes then kills the pid.
        let logger_name = &CONFIG.get().unwrap().logger_name;
        log::info!(target: logger_name, "Monitor spark app set to - {}.", monitor_app);
        log::info!(target: logger_name, "Monitoring spark application with id - {} and url - {} and with job_name - {}", self.id, self.url, self.job_name);
        let task_inst = match get_flow_and_task_inst(&self.job_name).await {
            Ok((_, task_inst)) => task_inst,
            Err(_) => {
                log::error!(target: logger_name, "Failed to get flow and task instance. Using job name to store metrics.");
                self.job_name.to_owned()
            }
        };
        let task_inst = task_inst.replace("/", "_");
        if let Some(cache_dir) = self.cache_dir.as_ref() {
            let _ = tokio::fs::create_dir_all(cache_dir.to_owned()).await;
        }
        let executors_data_path = format!("{}/{}", &CONFIG.get().unwrap().master_workarea, self.id);
        let _ = tokio::fs::create_dir_all(&executors_data_path).await;
        let mut sleep_time = 30;
        // Wait for executor to be added to worker if workers are already present
        sleep(Duration::from_secs(10)).await;
        let mut wait_time = None;
        let start = tokio::time::Instant::now();
        let mut first_inactive = None;
        let spark_app_timeout = get_spark_app_timeout().await;
        let spark_app_inactive_timeout = get_spark_inactive_timeout().await;
        loop {
            match is_app_active(&self.url, &self.id).await {
                Ok(true) => {
                    if monitor_app && first_inactive.is_some() {
                        log::info!(target: logger_name, "Application is active again.");
                        first_inactive = None;
                    }
                }
                Ok(false) => {
                    if monitor_app && fail_spark_app_on_timeout().await
                        && tokio::time::Instant::now().duration_since(start).as_secs()
                            > spark_app_timeout
                    {
                        log::error!(target: logger_name, "Spark App is inactive for more than {}s. Killing the app.", spark_app_timeout);
                        self.kill_and_update_status(
                            pid,
                            logger_name,
                            format!("Spark App is running for more than {}s.", spark_app_timeout),
                        )
                        .await;
                        log::info!(target: logger_name, "Application is not active. Exiting monitoring.");
                        break;
                    } else if monitor_app && first_inactive.is_none() {
                        first_inactive = Some(tokio::time::Instant::now());
                    } else if monitor_app && tokio::time::Instant::now()
                        .duration_since(first_inactive.unwrap())
                        .as_secs()
                        > spark_app_inactive_timeout
                    {
                        self.kill_and_update_status(
                            pid,
                            logger_name,
                            format!(
                                "Spark App is inactive for more than {}s.",
                                spark_app_inactive_timeout
                            ),
                        )
                        .await;
                        log::info!(target: logger_name, "Application is not active. Exiting monitoring.");
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
            match wait_time {
                Some(_) => {
                    sleep_time = 60;
                    if let Some(cache_dir) = self.cache_dir.as_ref() {
                        match get_driver_metrics(&self.url, &self.id).await {
                            Ok(mut driver_metrics) => {
                                driver_metrics["wait_time"] = serde_json::json!(wait_time.unwrap());
                                match tokio::fs::write(
                                    format!("{}/.monitor/spark_{}.json", cache_dir, task_inst),
                                    serde_json::to_string(&driver_metrics).unwrap(),
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        log::error!(target: logger_name, "Failed to write driver metrics to file. Reason - {}", e);
                                    }
                                }
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }
                }
                None => {
                    wait_time =
                        get_driver_wait_time(&self.url, &self.id, logger_name, &start).await;
                    log::info!(target: logger_name, "Driver wait time: {:?}", wait_time);
                }
            }
            if self.can_request_workers().await {
                match get_required_executors(&self.url, &self.id).await {
                    Ok(required_executors) => {
                        // Request master to add executors
                        log::info!(target: logger_name, "Requesting {} executors", required_executors);
                        match self.request_workers(required_executors).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!(target: logger_name, "Failed to request workers. Reason - {}", e);
                            }
                        }
                    }
                    Err(_) => {
                        log::error!(target: logger_name, "Failed to get required executors. Exiting monitoring.");
                        break;
                    }
                }
            } else {
                log::info!(target: logger_name, "Cannot request workers. Will try adding workers in next iteration.");
            }
            // Check if required executors are 0 and jobs are none.
            sleep(Duration::from_secs(sleep_time)).await;
        }
        // Wait until json files from spark worker have been successfully written
        // Waiting for 60 seconds since spark worker is monitored for every 60 seconds
        sleep(Duration::from_secs(60)).await;
        if let Some(cache_dir) = self.cache_dir.as_ref() {
            if let Err(e) = move_csv_files(
                &executors_data_path,
                &format!("{}/.monitor", cache_dir),
                &logger_name,
            )
            .await
            {
                log::error!(target: logger_name, "Failed to move json files. Reason - {}", e);
            }
        }
        log::info!(target: logger_name, "Removing {}/{}", &CONFIG.get().unwrap().master_workarea, &self.id);
        if let Err(error) =
            clean_spark_work_area(&CONFIG.get().unwrap().master_workarea, &self.id).await
        {
            log::error!(target: logger_name, "{}", error);
        }

        log::info!(target: logger_name, "Exiting monitoring.");
    }
    async fn request_workers(&self, mut required_executors: i64) -> Result<(), String> {
        required_executors = std::cmp::min(required_executors, 10);
        let spec = self.spec.as_ref().unwrap();
        let cores = spec.cores;
        let memory = spec.memory;
        let default_protocol = String::from("default");
        let protocol = spec.protocol.as_ref().unwrap_or(&default_protocol);
        for i in 0..required_executors {
            let job_name = format!(
                "spark_worker_{}_{}_{}",
                &CONFIG.get().unwrap().logger_name,
                i,
                chrono::Utc::now().timestamp()
            );
            let (mut job, command) = get_spark_worker_job(
                job_name,
                &CONFIG.get().unwrap().cluster_home,
                &CONFIG.get().unwrap().spark_master_url,
                &CONFIG.get().unwrap().master_workarea,
                cores,
                memory,
                protocol,
            )
            .await;
            job["command"] = serde_json::json!(command);
            log::info!(target: &CONFIG.get().unwrap().logger_name, "Submitting job: {}", job);
            match self.submit_request(job).await {
                Ok(_) => {}
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
    async fn submit_request(&self, request: serde_json::Value) -> Result<(), String> {
        match crate::common_utils::submit_post_request(
            &request,
            &format!("{}/run_job", CONFIG.get().unwrap().master_url),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
    async fn can_request_workers(&self) -> bool {
        let url = format!(
            "{}/can_request_spark_workers",
            CONFIG.get().unwrap().master_url
        );
        match crate::common_utils::submit_get_request(&url).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
    async fn kill_and_update_status(&self, pid: Option<u32>, logger_name: &str, message: String) {
        let job_hash = get_hash(&self.job_name).await;
        match pid {
            Some(pid) => {
                log::info!(target: logger_name, "Killing pid - {}", pid);
                let _ = kill_pid(pid, logger_name, true).await;
            }
            None => {
                let mut guard = CONFIG.get().unwrap().running_jobs.write().await;
                if let Some(pid) = guard.remove(&job_hash) {
                    log::info!(target: logger_name, "Killing pid - {}", pid);
                    let _ = kill_pid(pid, logger_name, true).await;
                } else {
                    log::error!(target: logger_name, "Failed to get job identifier for job hash - {}", job_hash);
                }
                drop(guard);
            }
        }
        super::update_job_status(
            job_hash,
            &Status::Failed,
            Some(message),
            &CONFIG.get().unwrap().master_url,
            &CONFIG.get().unwrap().logger_name,
            CONFIG.get().unwrap().worker_hash,
        )
        .await;
    }
}
