// use deltalake::datafusion::execution::cache;
use log::{error, info};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::{Child, Command};
use tokio::sync::mpsc::Sender;

use crate::common_utils::{
    append_data_to_file, check_disk_space, get_app_id_from_driver, get_flow_and_task_inst,
    set_port_for_ccm_spark_driver,
};
use crate::dto::{Identifier, Job, JobType, ManagerMessage, Spec, Status};
use crate::utils::get_fail_category;
use crate::CONFIG;

pub fn get_required_spec_index(specs: &Vec<Spec>, current_spec: &Spec) -> Option<usize> {
    let mut reqd_index = None;
    let reqd_orderings = vec![Ordering::Less, Ordering::Equal];
    for (index, spec) in specs.iter().enumerate() {
        if reqd_orderings.contains(&spec.cmp(current_spec)) {
            {
                reqd_index = Some(index);
                break;
            }
        }
    }
    reqd_index
}

pub async fn execute_command(
    command: &str,
    job_name: &str,
    job_type: &str,
    cores: i64,
    mut envs: HashMap<String, String>,
    job_hash: u64,
    cache_dir: &Option<String>,
) -> Result<(Child, u32, Option<String>), ()> {
    // Spawn a child process
    // This will kill the spawned process when the child goes out of scope
    envs.extend(std::collections::HashMap::from([(
        String::from("num_cores"),
        cores.to_string(),
    )]));
    let mut spark_webui_url = None;
    let current_dir = match cache_dir {
        Some(cache_dir) => cache_dir,
        None => &CONFIG.get().unwrap().workarea,
    };
    if job_type == "spark_worker" {
        let spark_port = crate::common_utils::get_unused_port().await;
        envs.extend(
            crate::spark::set_spark_env(
                &job_name,
                &spark_port,
                &CONFIG.get().unwrap().cluster_home,
            )
            .await,
        );
        spark_webui_url = Some(format!(
            "http://{}:{}",
            local_ip_address::local_ip().unwrap(),
            spark_port
        ));
    } else if job_type == "ccm-spark-driver" {
        let port = match set_port_for_ccm_spark_driver(current_dir).await {
            Ok(port) => port,
            Err(_) => {
                return Err(());
            }
        };
        spark_webui_url = Some(format!(
            "http://{}:{}",
            local_ip_address::local_ip().unwrap(),
            port
        ));
        envs.insert(
            String::from("SPARK_LOCAL_HOSTNAME"),
            local_ip_address::local_ip().unwrap().to_string(),
        );
        envs.insert(
            String::from("SPARK_LOCAL_IP"),
            local_ip_address::local_ip().unwrap().to_string(),
        );
        if let Some(cache) = cache_dir {
            let conf_path = format!("{}/.conf/spark_props.conf", cache);
            let _ = append_data_to_file(
                &conf_path,
                &format!(
                    "spark.driver.bindAddress={}",
                    local_ip_address::local_ip().unwrap().to_string()
                ),
            )
            .await;
        }
    }
    let (stdout, stderr) = match job_type.contains("flow-engine") {
        true => (Stdio::null(), Stdio::null()),
        false => (Stdio::piped(), Stdio::piped()),
    };
    let child = match Command::new("sh")
        .arg("-c")
        .arg(command)
        .envs(envs)
        .kill_on_drop(true)
        .stderr(stdout)
        .stdout(stderr)
        .current_dir(current_dir)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            super::update_job_status(
                job_hash,
                Status::Failed,
                false,
                Some(format!("Failed to execute command. Reason - {}", error)),
                JobType::Local,
            )
            .await;
            return Err(());
        }
    };
    let pid = child.id().unwrap();
    return Ok((child, pid, spark_webui_url));
}

pub async fn wait_for_completion(
    child: Child,
    logger_name: &str,
    pid: u32,
    job_hash: u64,
    mut spark_url: Option<String>,
    cache_dir: Option<String>,
    job: Job,
) {
    // Check if worker is running or not
    // if not running break the loop and update worker and job status
    let json_name = match get_flow_and_task_inst(&job.job_name).await {
        Ok((_, task)) => task,
        Err(_) => job.job_name.clone(),
    };
    let spec = job.get_spec().await.clone();
    if job.job_type == "ccm-spark-driver" && spark_url.is_some() {
        let url = spark_url.unwrap();
        // Setting spark url as None as this is not a spark worker job
        spark_url = None;
        // Spawning a new task to monitor spark job
        info!(target: logger_name, "Monitoring spark job with pid {} and url {}", pid, url);
        if let Some(app_id) = get_app_id_from_driver(&url, pid).await {
            let app = crate::utils::proto::SparkApp {
                job_name: job.job_name,
                id: app_id,
                url: url,
                spec: Some(crate::utils::proto::Spec {
                    cores: job.specs[0].cores,
                    memory: job.specs[0].memory,
                    protocol: Some(job.protocol),
                }),
                cache_dir: cache_dir.clone(),
            };
            tokio::spawn(crate::spark::monitor_spark_app(app, Some(pid), false));
        } else {
            error!(target: logger_name, "Failed to get app id for spark job with url {}", url);
        }
    }
    let (flow_message, status) = match crate::common_utils::wait(
        child,
        logger_name,
        &spark_url,
        &cache_dir,
        &json_name,
        &CONFIG.get().unwrap().workarea,
        "local",
        spec.cores,
        spec.memory,
    )
    .await
    {
        Ok(()) => (None, Status::Success),
        Err(message) => (Some(message), Status::Failed),
    };
    // Update the status of the job
    match flow_message {
        Some(message) => {
            super::update_job_status(job_hash, status, true, Some(message), JobType::Local).await
        }
        None => super::update_job_status(job_hash, status, false, None, JobType::Local).await,
    }
    info!(target: logger_name, "Job with pid {} completed execution with status {:?}", pid, status);
}

// getting queued sh job from the queue
pub fn get_queued_jobs(
    specs: &Vec<Spec>,
    spec_job_mapper: &mut HashMap<u64, u64>,
    low_priority_mapper: &mut HashMap<u64, u64>,
    current_spec: &Spec,
) -> Vec<u64> {
    // Get required job from queue
    let tmp_spec = current_spec.clone();
    let mut categories = get_queued_job(specs, spec_job_mapper, tmp_spec);
    if categories.is_empty() || (tmp_spec.cores > 0 && tmp_spec.memory > 0) {
        let mut lower_priority_categories =
            get_queued_job(specs, low_priority_mapper, tmp_spec);
        categories.append(&mut lower_priority_categories);
    }
    categories
}

pub fn get_queued_job(
    specs: &Vec<Spec>,
    spec_job_mapper: &mut HashMap<u64, u64>,
    mut current_spec: Spec,
) -> Vec<u64> {
    let mut categories: Vec<u64> = Vec::new();
    // Get reqd spec which can be run with available resources
    let reqd_index = match get_required_spec_index(specs, &current_spec) {
        Some(index) => index,
        None => return Vec::new(),
    };
    for index in reqd_index..specs.len() {
        let spec = &specs[index];
        match spec_job_mapper.remove(&spec.get_hash()) {
            Some(category) => {
                categories.push(category);
                current_spec.remove(spec);
            }
            None => {
                continue;
            }
        }
    }
    categories
}

pub async fn update_specs(specs: &mut Vec<Spec>, spec: &Spec) {
    // Add specs to queue if not present
    if !specs.contains(&spec) {
        specs.push(spec.clone());
        // Reverse Sort specs vector. This is not async since this is a small vector
        specs.sort_by(|a: &Spec, b: &Spec| b.cmp(a));
    }
}

pub async fn add_job(
    run_job: crate::apis::RunJob,
    job_scheduler_tx: &Sender<ManagerMessage>,
) -> Result<(), String> {
    let job = Job::new(
        run_job.job_name,
        run_job.job_type,
        run_job.protocol.unwrap_or("default".to_string()),
        run_job.specs,
        run_job.lightweight.unwrap_or(false),
        run_job.lightweight_spec,
    );
    let (tx, rx) = tokio::sync::oneshot::channel();
    job_scheduler_tx
        .send(ManagerMessage::AddJob(
            job,
            run_job.command,
            true,
            run_job.cache_dir,
            Some(tx),
        ))
        .await
        .unwrap();
    return rx.await.unwrap_or(Ok(()));
}

pub async fn schedule_sh_job(
    job: Job,
    logger_name: String,
    envs: HashMap<String, String>,
    command: String,
    cache_dir: Option<String>,
) {
    let spec = job.get_spec().await;
    let guard = CONFIG.get().unwrap().protocols_map.read().await;
    let protocol = guard.get("local").unwrap();
    let has_space = check_disk_space(
        protocol.get_local_dir().await,
        protocol.get_disk_threshold().await,
        &job.job_type,
        "local",
    )
    .await;
    match has_space {
        Ok(_) => {}
        Err(message) => {
            CONFIG
                .get()
                .unwrap()
                .job_scheduler_tx
                .send(ManagerMessage::FailJobs(
                    get_fail_category(&job).await,
                    Some(message),
                ))
                .await
                .unwrap();
            drop(guard);
            return;
        }
    }
    drop(guard);
    let job_hash = job.get_hash();
    match super::execute_command(
        &command,
        &job.job_name,
        &job.job_type,
        spec.cores,
        envs,
        job_hash,
        &cache_dir,
    )
    .await
    {
        Ok((child, pid, spark_worker_url)) => {
            CONFIG
                .get()
                .unwrap()
                .job_scheduler_tx
                .send(ManagerMessage::UpdateJobIdentifier(
                    job_hash,
                    Identifier::Pid(pid),
                ))
                .await
                .unwrap();
            tokio::spawn(async move {
                super::wait_for_completion(
                    child,
                    &logger_name,
                    pid,
                    job_hash,
                    spark_worker_url,
                    cache_dir,
                    job,
                )
                .await;
            });
        }
        Err(_) => {
            error!(target: &logger_name, "Failed to execute command");
        }
    }
}
