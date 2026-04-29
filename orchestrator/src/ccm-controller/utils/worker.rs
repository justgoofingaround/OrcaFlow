pub mod proto {
    tonic::include_proto!("worker");
}

use log::{error, info};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};

use crate::common_utils::get_worker_error_file_path;
use crate::dto::{Job, ManagerMessage, Status, Worker};
use crate::CONFIG;
use proto::worker_client::WorkerClient;

impl Worker {
    pub fn new(name: &str, protocol: String, categories: Vec<u64>) -> Worker {
        Worker {
            name: name.to_string(),
            identifier: None,
            job_id: None,
            protocol,
            categories,
        }
    }
    pub async fn monitor_worker_using_id(&self, logger_name: &str) -> Status {
        let guard = CONFIG.get().unwrap().protocols_map.read().await;
        let mut queued_time = 0;
        if let crate::dto::FarmConfig::Farm(config) = &guard.get(&self.protocol).unwrap().config {
            queued_time = config.queued_time;
        }
        drop(guard);
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(queued_time as u64),
            self.__monitor_job_id(
                &logger_name,
                vec![
                    Status::Success,
                    Status::Running,
                    Status::Queued,
                    Status::Killed,
                    Status::Failed,
                    Status::Unknown,
                ],
                &self.protocol,
            ),
        )
        .await
        {
            Ok(status) => status,
            Err(_) => {
                error!(target: &logger_name, "Worker {} launch timed out.", self.name);
                return Status::Failed;
            }
        };
        // Wait for job to change from queued
        self.__monitor_job_id(
            &logger_name,
            vec![
                Status::Success,
                Status::Running,
                Status::Failed,
                Status::Killed,
                Status::Unknown,
            ],
            &self.protocol,
        )
        .await
    }
    async fn __monitor_job_id(
        &self,
        logger_name: &str,
        wait_for_status: Vec<Status>,
        protocol: &str,
    ) -> Status {
        // Wait for job to change to status given in wait_for_status list
        // If status is in retry status then we retry
        let mut status = Status::New;
        if let Some(job_id) = &self.job_id {
            while !wait_for_status.contains(&status) {
                let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
                let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
                tokio::select! {
                        _ = interrupt_signal.recv() => {
                            status = Status::Killed;
                            break;
                        }
                        _ = terminate_signal.recv() => {
                            status = Status::Killed;
                            break;
                        }
                        _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
                            let guard = CONFIG.get().unwrap().protocols_map.read().await;
                            if let crate::dto::FarmConfig::Farm(config) = &guard.get(protocol).unwrap().config {
                            status = super::get_job_status_from_id(&job_id, &config.script, logger_name).await;
                            log::info!(target: logger_name, "Job {} status is {}", job_id, status.to_string());}
                            drop(guard);
                        }
                }
            }
        }
        status
    }
    pub async fn monitor_identifier(self, logger_name: &str) {
        self.__monitor_rpc(logger_name).await;
    }
    async fn create_rpc_client(
        &self,
        url: &str,
    ) -> Result<WorkerClient<tonic::transport::Channel>, ()> {
        match tokio::time::timeout(
            Duration::from_secs(60),
            WorkerClient::connect(url.to_string()),
        )
        .await
        {
            Ok(response) => match response {
                Ok(client) => {
                    return Ok(client);
                }
                Err(error) => {
                    error!(
                        target: "worker_manager", "Failed to connect to worker with url: {:?}. Reason - {:?}",
                        url, error
                    );
                    return Err(());
                }
            },
            Err(_) => {
                error!(
                    target: "worker_manager", "Failed to connect to worker with url: {:?}. Reason - timeout",
                    url
                );
                return Err(());
            }
        }
    }
    pub async fn check_worker_rpc(
        &self,
        client: &mut WorkerClient<tonic::transport::Channel>,
    ) -> Result<(), ()> {
        let mut retry = 0;
        while retry < 3 {
            let message = proto::Empty {};
            let request: tonic::Request<proto::Empty> = tonic::Request::new(message);
            match tokio::time::timeout(Duration::from_secs(10), client.heartbeat(request)).await {
                Ok(response) => match response {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(_) => {
                        error!(
                                        "Failed to get status from worker with url: {:?}. Reason - worker not responding",
                                        self.identifier
                                    );
                        retry += 1;
                    }
                },
                Err(_) => {
                    error!(
                        "Failed to get status from worker with url: {:?}. Reason - timeout",
                        self.identifier
                    );
                    retry += 1;
                }
            }
        }
        return Err(());
    }
    #[allow(dead_code, unused_attributes)]
    async fn __monitor_rpc(self, logger_name: &str) {
        // Check if grpc is running or not
        // Sleep for 30 seconds so that grpc can start
        sleep(Duration::from_secs(30)).await;
        let mut retry = 0;
        let mut result_client = self
            .create_rpc_client(self.identifier.as_ref().unwrap())
            .await;
        while retry < 2 {
            log::info!(target: logger_name, "Checking worker status of {}...", self.name);
            match result_client.as_mut() {
                Ok(client) => {
                    if let Err(_) = self.check_worker_rpc(client).await {
                        retry += 1;
                    } else {
                        retry = 0;
                    }
                }
                Err(_) => {
                    retry += 1;
                    result_client = self
                        .create_rpc_client(self.identifier.as_ref().unwrap())
                        .await;
                }
            }
            sleep(Duration::from_secs(300)).await;
        }
        if let Some(job_id) = &self.job_id {
            let guard = CONFIG.get().unwrap().protocols_map.read().await;
            if let crate::dto::FarmConfig::Farm(config) = &guard.get(&self.protocol).unwrap().config
            {
                let status =
                    super::get_job_status_from_id(job_id, &config.script, logger_name).await;
                if [Status::Queued, Status::Running, Status::Unknown].contains(&status) {
                    super::terminate_job(job_id, &config.script, logger_name).await;
                }
            }
            drop(guard);
        }
        let worker_hash = self.get_hash();
        CONFIG
            .get()
            .unwrap()
            .worker_manager_tx
            .send(ManagerMessage::KillWorker(
                worker_hash,
                true,
                Some(format!(
                    "Worker with url {:?} is not responding.",
                    self.identifier
                )),
                true,
            ))
            .await
            .unwrap();
        sleep(Duration::from_secs(3600)).await;
        let path = format!(
            "{}/logs/{}",
            crate::CONFIG.get().unwrap().workarea,
            self.name
        );
        log::info!(target: logger_name, "Removing log directory: {}", path);
        if let Ok(log_path) = std::path::PathBuf::from_str(&path) {
            if log_path.exists() && log_path.is_dir() {
                let _ = tokio::fs::remove_dir_all(log_path).await;
            }
        }
    }
    #[allow(dead_code, unused_attributes)]
    async fn __monitor_pid(&self, _logger_name: &str) {
        // TODO: Add monitoring for sh mode
    }
    pub async fn kill_worker(
        &mut self,
        logger_name: String,
        job_hash: Option<u64>,
        mut force: bool,
    ) {
        // Kill the worker with url
        let worker_url = match &self.identifier {
            Some(url) => url,
            None => {
                force = true;
                &String::new()
            }
        };
        if force {
            self.__kill_worker_using_id(&logger_name).await;
        } else {
            let client = WorkerClient::connect(worker_url.to_owned()).await;
            match client {
                Ok(mut client) => {
                    if let Some(job_hash) = job_hash {
                        let message = proto::JobId { job_hash: job_hash };
                        let request = tonic::Request::new(message);
                        match tokio::time::timeout(
                            Duration::from_secs(60),
                            client.kill_job(request),
                        )
                        .await
                        {
                            Ok(response) => match response {
                                Ok(_) => {
                                    info!(target: &logger_name, "Request successfully sent to {} to kill job {}.", worker_url, job_hash);
                                }
                                Err(_) => {
                                    error!(target: &logger_name, "Failed to kill job with url: {}.", worker_url);
                                    self.__kill_worker_using_id(&logger_name).await;
                                }
                            },
                            Err(_) => {
                                error!(target: &logger_name, "Failed to kill job with url: {}. Reason - timeout", worker_url);
                                self.__kill_worker_using_id(&logger_name).await;
                            }
                        }
                    } else {
                        let request = tonic::Request::new(proto::Empty {});
                        match tokio::time::timeout(
                            Duration::from_secs(60),
                            client.terminate(request),
                        )
                        .await
                        {
                            Ok(response) => match response {
                                Ok(_) => {
                                    info!(target: &logger_name, "Request successfully sent to {} to terminate.", worker_url);
                                }
                                Err(_) => {
                                    error!(target: &logger_name, "Failed to terminate worker with url: {}.", worker_url);
                                    self.__kill_worker_using_id(&logger_name).await;
                                }
                            },
                            Err(_) => {
                                error!(target: &logger_name, "Failed to terminate worker with url: {}. Reason - timeout", worker_url);
                                self.__kill_worker_using_id(&logger_name).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    self.__kill_worker_using_id(&logger_name).await;
                    error!(target: &logger_name, "Failed to connect to worker with url: {}. Reason - {}", worker_url, e);
                }
            }
        }
    }
    async fn __kill_worker_using_id(&self, logger_name: &str) {
        if let Some(job_id) = &self.job_id {
            let guard = CONFIG.get().unwrap().protocols_map.read().await;
            if let crate::dto::FarmConfig::Farm(config) = &guard.get(&self.protocol).unwrap().config
            {
                super::terminate_job(job_id, &config.script, logger_name).await;
                drop(guard);
            }
        } else {
            info!(target: &logger_name, "Job id not found for worker with name: {}", self.name);
        }
    }
    pub async fn submit_job(
        &self,
        job: &Job,
        logger_name: &str,
        envs: Option<HashMap<String, String>>,
        command: &str,
        cache_dir: &Option<String>,
    ) -> Result<(), ()> {
        let mut resubmit = false;
        if let Some(url) = &self.identifier {
            let client = WorkerClient::connect(url.to_owned()).await;
            match client {
                Ok(mut client) => {
                    let mut message = proto::Job {
                        job_hash: job.get_hash(),
                        job_name: Some(job.job_name.clone()),
                        command: command.to_string(),
                        job_type: job.job_type.clone(),
                        category: job.get_category(),
                        envs: envs.unwrap_or(HashMap::new()),
                        cache_dir: cache_dir.to_owned(),
                        spec: Some(proto::Spec {
                            cores: job.specs[0].cores,
                            memory: job.specs[0].memory,
                            protocol: Some(job.protocol.clone()),
                        }),
                        lightweight_spec: None,
                    };
                    if let Some(spec) = job.lightweight_spec.as_ref() {
                        message.lightweight_spec = Some(proto::Spec {
                            cores: spec.cores,
                            memory: spec.memory,
                            protocol: Some(job.protocol.clone()),
                        });
                    }
                    let request = tonic::Request::new(message);
                    match client.run_job(request).await {
                        Ok(_) => {}
                        Err(_) => {
                            resubmit = true;
                        }
                    };
                }
                Err(_) => {
                    error!(target: logger_name, "Failed to connect to worker with url: {}", url);
                    resubmit = true;
                }
            }
        } else {
            resubmit = true;
        }
        if resubmit {
            return Err(());
        }
        Ok(())
    }
    pub fn get_hash(&self) -> u64 {
        super::get_hash(&self.name)
    }
    pub async fn check_job_status(self, logger_name: &str, worker_hash: u64) {
        self.monitor_worker_using_id(logger_name).await;
        CONFIG
            .get()
            .unwrap()
            .worker_manager_tx
            .send(ManagerMessage::UpdateQueued(worker_hash))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        // If error file is present then increment retries of the worker.
        if let Err(message) = self
            .check_worker_connection(&CONFIG.get().unwrap().workarea)
            .await
        {
            CONFIG
                .get()
                .unwrap()
                .job_scheduler_tx
                .send(ManagerMessage::IncrementRetry(
                    self.categories[0],
                    Some(message),
                ))
                .await
                .unwrap();
        }
    }
    async fn check_worker_connection(&self, workarea: &str) -> Result<(), String> {
        let path = get_worker_error_file_path(workarea, &self.name).await;
        if std::path::Path::new(&path).exists() {
            let content = std::fs::read_to_string(&path).unwrap();
            return Err(content);
        }
        Ok(())
    }
}
