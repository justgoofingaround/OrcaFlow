pub mod proto {
    tonic::include_proto!("worker");
}
use log::info;
use proto::{Empty, Job, ReturnStatus, SparkApp, JobId};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response};

use crate::common_utils::{get_hash, kill_pid};
use crate::data_types::Status;
use crate::utils::{self};
use crate::CONFIG;

#[derive(Debug)]
pub struct WorkerService {
    running_jobs: Arc<RwLock<std::collections::HashMap<u64, u32>>>,
}

impl WorkerService {
    pub fn new() -> Self {
        WorkerService {
            running_jobs: Arc::clone(&CONFIG.get().unwrap().running_jobs),
        }
    }
}

#[tonic::async_trait]
impl proto::worker_server::Worker for WorkerService {
    async fn heartbeat(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ReturnStatus>, tonic::Status> {
        let response = ReturnStatus {
            status: "OK".to_string(),
        };
        Ok(Response::new(response))
    }
    async fn terminate(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ReturnStatus>, tonic::Status> {
        let guard = self.running_jobs.read().await;
        for (_, pid) in guard.iter() {
            kill_pid(*pid, &CONFIG.get().unwrap().logger_name, false).await;
        }
        drop(guard);
        std::process::exit(0);
    }
    async fn run_job(
        &self,
        request: Request<Job>,
    ) -> Result<Response<ReturnStatus>, tonic::Status> {
        let job = request.into_inner();
        // Send the command to the executor
        info!(target: &CONFIG.get().unwrap().logger_name, "Job {} submitted in slot of {:?}", job.job_hash, job.category);
        tokio::spawn(spawn_command(job));
        let response = ReturnStatus {
            status: "Job submitted successfully".to_string(),
        };
        Ok(Response::new(response))
    }
    async fn kill_job(
        &self,
        request: Request<JobId>,
    ) -> Result<Response<ReturnStatus>, tonic::Status> {
        let job = request.into_inner();
        // Send the kill signal to the executor
        let guard = self.running_jobs.read().await;
        match guard.get(&job.job_hash) {
            Some(pid) => {
                kill_pid(*pid, &CONFIG.get().unwrap().logger_name, false).await;
            }
            None => {
                drop(guard);
                return Err(tonic::Status::invalid_argument("Job not found"));
            }
        }
        drop(guard);
        let response = ReturnStatus {
            status: "Successfully killed job".to_string(),
        };
        Ok(Response::new(response))
    }
    async fn register_app(
        &self,
        request: Request<SparkApp>,
    ) -> Result<Response<ReturnStatus>, tonic::Status> {
        // Spawn a task to register the app
        let spark_app = request.into_inner();
        let hash = get_hash(&spark_app.job_name).await;
        let guard = self.running_jobs.read().await;
        let pid = match guard.get(&hash) {
            Some(pid) => Some(pid.to_owned()),
            None => None,
        };
        drop(guard);
        tokio::spawn(async move {
            utils::monitor_spark_app(spark_app, pid, true).await;
        });
        let response = ReturnStatus {
            status: "Successfully register app for scaling".to_string(),
        };
        Ok(Response::new(response))
    }
}

pub async fn spawn_command(job: Job) {
    match utils::execute_command(&job).await {
        Ok((child, pid, spark_webui_url)) => {
            tokio::spawn(async move {
                utils::wait(
                    pid,
                    child,
                    spark_webui_url,
                    job,
                )
                .await
            });
        }
        Err(message) => {
            let mut guard = CONFIG.get().unwrap().running_jobs.write().await;
            guard.remove(&job.job_hash);
            drop(guard);
            utils::update_job_status(
                job.job_hash,
                &Status::DisQualified,
                Some(message),
                &CONFIG.get().unwrap().master_url,
                &CONFIG.get().unwrap().logger_name,
                CONFIG.get().unwrap().worker_hash,
            )
            .await;
        }
    }
}
