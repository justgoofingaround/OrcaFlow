use std::sync::atomic::Ordering;
use tokio::time;

use crate::common_utils::{
    clean_spark_work_area, fail_spark_app_on_timeout, get_driver_metrics, get_driver_wait_time,
    get_flow_and_task_inst, get_hash, get_required_executors, get_spark_app_timeout,
    get_spark_inactive_timeout, get_spark_worker_job, is_app_active, kill_pid, move_csv_files,
};
use crate::dto::{Identifier, Job, JobField, JobFieldValue, JobType, ManagerMessage, Status};
use crate::utils::proto::SparkApp;
use crate::CONFIG;

pub async fn monitor_spark_app(spark_app: SparkApp, pid: Option<u32>, monitor_app: bool) {
    spark_app.monitor_app(pid, monitor_app).await;
}

impl SparkApp {
    pub async fn monitor_app(&self, pid: Option<u32>, monitor_app: bool) {
        // This code block is resposible to monitor the spark application registered.
        // It checks for how many executors are required by the driver and sends the request to the master to allocate workers accordingly
        // Checks the stats of the driver and writes it to json file inside cache directory
        // Checks if any jobs are running and if no jobs are running for more than an hour assumes that the app is stucks and kills the driver
        // Checks if driver is in waiting state for more than an hour, if yes then kills the pid.
        let logger_name = "spark_monitor";
        log::info!(target: logger_name, "Monitor spark app set to - {}.", monitor_app);
        log::info!(target: logger_name, "Monitoring spark application with id - {} and url - {}", self.id, self.url);
        let add_spark_worker = CONFIG.get().unwrap().add_spark_worker.clone();
        let task_inst = match get_flow_and_task_inst(&self.job_name).await {
            Ok((_, task_inst)) => task_inst,
            Err(_) => {
                log::error!(target: logger_name, "Failed to get flow and task instance. Using job name to store metrics.");
                self.job_name.to_owned()
            }
        };
        let task_inst = format!("spark_{}", task_inst.replace("/", "_"));
        if let Some(cache_dir) = self.cache_dir.as_ref() {
            let _ = tokio::fs::create_dir_all(cache_dir.to_owned()).await;
        }
        let executors_data_path = format!("{}/{}", &CONFIG.get().unwrap().workarea, self.id);
        let _ = tokio::fs::create_dir_all(&executors_data_path).await;
        let mut sleep_time = 30;
        // Wait for executor to be added to worker if workers are already present
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        let mut wait_time = None;
        let start = time::Instant::now();
        let mut first_inactive = None;
        let spark_app_timeout = get_spark_app_timeout().await;
        let spark_app_inactive_timeout = get_spark_inactive_timeout().await;
        loop {
            match is_app_active(&self.url, &self.id).await {
                Ok(true) => {
                    if first_inactive.is_some() {
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
                    }
                    else if monitor_app && first_inactive.is_none() {
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
                    log::error!(target: logger_name, "Failed to check if app is active.");
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
                                let path = format!("{}/.monitor/{}.json", cache_dir, task_inst);
                                match tokio::fs::write(
                                    path,
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
                }
            }
            if add_spark_worker.load(Ordering::Acquire) {
                match get_required_executors(&self.url, &self.id).await {
                    Ok(required_executors) => {
                        // Request master to add executors
                        match self
                            .request_workers(required_executors, &self.job_name)
                            .await
                        {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            // Check if required executors are 0 and jobs are none.
            time::sleep(time::Duration::from_secs(sleep_time)).await;
        }
        // Wait until json files from spark worker have been successfully written
        // Waiting for 60 seconds since spark worker is monitored for every 60 seconds
        time::sleep(time::Duration::from_secs(60)).await;
        if let Some(cache_dir) = self.cache_dir.as_ref() {
            if let Err(e) = move_csv_files(
                &executors_data_path,
                &format!("{}/.monitor", cache_dir),
                logger_name,
            )
            .await
            {
                log::error!(target: logger_name, "Failed to move json files from {} to {}. Reason - {}",executors_data_path, cache_dir, e);
            }
        }
        if let Err(error) = clean_spark_work_area(&CONFIG.get().unwrap().workarea, &self.id).await {
            log::error!(target: logger_name, "{}", error);
        }
        log::info!(target: logger_name, "Exiting monitoring.");
    }
    async fn request_workers(
        &self,
        mut required_executors: i64,
        job_name: &str,
    ) -> Result<(), String> {
        required_executors = std::cmp::min(required_executors, 10);
        let spec = self.spec.as_ref().unwrap();
        let cores = spec.cores;
        let memory = spec.memory;
        let default_protocol = String::from("default");
        let protocol = spec.protocol.as_ref().unwrap_or(&default_protocol);
        for i in 0..required_executors {
            let job_name = format!(
                "spark_worker_local_{}_{}_{}",
                job_name,
                chrono::Utc::now().timestamp(),
                i
            );
            let (mut job, command) = get_spark_worker_job(
                job_name,
                &CONFIG.get().unwrap().cluster_home,
                &CONFIG.get().unwrap().spark_master_url,
                &CONFIG.get().unwrap().workarea,
                cores,
                memory,
                protocol,
            )
            .await;
            job["status"] = serde_json::json!(crate::dto::Status::New);
            let job: Job = serde_json::from_value(job).unwrap();
            match self.submit_request(job, command).await {
                Ok(_) => {}
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
    async fn submit_request(&self, request: Job, command: String) -> Result<(), String> {
        match CONFIG
            .get()
            .unwrap()
            .job_scheduler_tx
            .send(crate::dto::ManagerMessage::AddJob(
                request, command, true, None, None,
            ))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
    async fn kill_and_update_status(&self, pid: Option<u32>, logger_name: &str, message: String) {
        let scheduler_tx = &CONFIG.get().unwrap().job_scheduler_tx;
        let job_hash = get_hash(&self.job_name).await;
        match pid {
            Some(pid) => {
                log::info!(target: logger_name, "Killing pid - {}", pid);
                let _ = kill_pid(pid, logger_name, true).await;
            }
            None => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = scheduler_tx
                    .send(ManagerMessage::GetJobField(
                        job_hash,
                        JobField::Identifier,
                        tx,
                    ))
                    .await;
                match rx.await {
                    Ok(Some(value)) => {
                        if let JobFieldValue::Identifier(Some(Identifier::Pid(pid))) = value {
                            log::info!(target: logger_name, "Killing pid - {}", pid);
                            kill_pid(pid, logger_name, false).await;
                        } else {
                            log::error!(target: logger_name, "Failed to get job identifier for job hash - {}", job_hash);
                        }
                    }
                    Ok(None) => {
                        log::error!(target: logger_name, "No identifier found for job hash - {}", job_hash);
                    }
                    Err(e) => {
                        log::error!(target: logger_name, "Failed to get job identifier for job hash - {}. Reason - {}", job_hash, e);
                    }
                }
            }
        }
        let _ = scheduler_tx
            .send(ManagerMessage::UpdateJobStatus(
                job_hash,
                Status::Failed,
                true,
                Some(message),
                JobType::Local,
            ))
            .await;
    }
}
