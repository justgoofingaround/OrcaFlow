use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{atomic, Arc};
use tokio::sync::{mpsc::Sender, RwLock};

use crate::dto::{
    Config, JobField, JobFieldValue, JobType, ManagerMessage, Protocol, SparkJob, Spec, Status,
};
use crate::CONFIG;
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RunJob {
    pub job_name: String,
    pub command: String,
    pub specs: Vec<Spec>,
    pub protocol: Option<String>,
    pub job_type: String,
    pub lightweight: Option<bool>,
    pub lightweight_spec: Option<Spec>,
    pub cache_dir: Option<String>,
}

#[derive(Clone)]
pub struct SharedState {
    pub worker_tx: Sender<ManagerMessage>,
    pub logger: String,
    pub scheduler_tx: Sender<ManagerMessage>,
    pub protocol_map: Arc<RwLock<HashMap<String, Protocol>>>,
    pub config: Arc<RwLock<Config>>,
    pub add_spark_worker: Arc<atomic::AtomicBool>,
}
#[derive(Deserialize, Serialize)]
pub struct GetJobsWithStatus {
    status: String,
}

#[derive(Deserialize, Serialize)]
pub struct GetQueuedJob {
    spec: Spec,
}

#[derive(Deserialize, Serialize)]
pub struct KillJob {
    job_name: String,
}

#[derive(Deserialize, Serialize)]
pub struct GetWorkersOfJob {
    job_name: String,
}

pub async fn heartbeat() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({"message": "Cluster Manager"})))
}

pub async fn run_job(
    State(state): State<SharedState>,
    Json(job): Json<RunJob>,
) -> impl IntoResponse {
    // Add job to job mapper
    info!(target: &state.logger, "Recieved : {:?}", job.job_name);
    if job.job_type == "spark_worker" && !state.add_spark_worker.load(atomic::Ordering::Acquire) {
        info!(target: &state.logger, "Spark worker queue is full. Skipping addition of job");
        return (
            StatusCode::OK,
            Json(json!({"message": "Spark worker queue is full. Skipping addition of job"})),
        );
    }
    // TODO: Add validation for job
    match crate::utils::add_job(job, &state.scheduler_tx).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({"message": "Job added successfully"})),
        ),
        Err(message) => {
            log::error!(target: &state.logger, "Failed to add job. Reason: {}", message);
            return (StatusCode::BAD_REQUEST, Json(json!({"message": message})));
        }
    }
}

pub async fn update_worker(
    State(state): State<SharedState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let worker_hash = match payload.get("worker_hash") {
        Some(worker_hash) => worker_hash.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "worker_hash not provided"})),
            );
        }
    };
    let worker_url = match payload.get("worker_url") {
        Some(worker_url) => worker_url.as_str().unwrap().to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "worker_url not provided"})),
            );
        }
    };
    state
        .worker_tx
        .send(ManagerMessage::UpdateWorker(
            worker_hash,
            None,
            Some(worker_url),
        ))
        .await
        .unwrap();
    (
        StatusCode::OK,
        Json(json!({"message": "Message sent to worker manager"})),
    )
}

pub async fn add_spark_driver(
    State(_): State<SharedState>,
    Json(app): Json<serde_json::Value>,
) -> impl IntoResponse {
    let app = crate::utils::proto::SparkApp {
        job_name: app["job_name"].as_str().unwrap().to_string(),
        id: app["id"].as_str().unwrap().to_string(),
        url: app["url"].as_str().unwrap().to_string(),
        spec: Some(crate::utils::proto::Spec {
            cores: app["spec"]["cores"].as_i64().unwrap(),
            memory: app["spec"]["memory"].as_i64().unwrap(),
            protocol: Some(
                app["spec"]["protocol"]
                    .as_str()
                    .unwrap_or("default")
                    .to_string(),
            ),
        }),
        cache_dir: Some(app["cache_dir"].as_str().unwrap().to_string()),
    };
    tokio::spawn(crate::spark::monitor_spark_app(app, None, true));
    (
        StatusCode::OK,
        Json(json!({"message": "Successfully registered app for scaling"})),
    )
}

pub async fn update_worker_status(
    State(state): State<SharedState>,
    Json(job): Json<serde_json::Value>,
) -> impl IntoResponse {
    let job_hash = match job.get("job_hash") {
        Some(job_hash) => job_hash.as_u64().unwrap(),
        None => 0,
    };
    let status = match job.get("status") {
        Some(status) => Status::from_str(status.as_str().unwrap()).await,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "status not provided"})),
            );
        }
    };
    let worker_hash = match job.get("worker_hash") {
        Some(worker_hash) => worker_hash.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "worker_hash not provided"})),
            );
        }
    };
    let category = match job.get("category") {
        Some(category) => category.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "category not provided"})),
            );
        }
    };
    let message = match job.get("message") {
        Some(msg) => Some(msg.as_str().unwrap().to_string()),
        None => None,
    };
    state
        .worker_tx
        .send(ManagerMessage::UpdateWorkerStatus(
            worker_hash,
            status,
            job_hash,
            category,
            message,
        ))
        .await
        .unwrap();
    (
        StatusCode::OK,
        Json(json!({"message": "Updated worker status"})),
    )
}

pub async fn update_job_status(Json(job): Json<serde_json::Value>) -> impl IntoResponse {
    let job_hash = match job.get("job_hash") {
        Some(job_hash) => job_hash.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "job_hash not provided"})),
            );
        }
    };
    let status = match job.get("status") {
        Some(status) => Status::from_str(status.as_str().unwrap()).await,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "status not provided"})),
            );
        }
    };
    let worker_hash = match job.get("worker_hash") {
        Some(worker_hash) => worker_hash.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "worker_hash not provided"})),
            );
        }
    };
    let update_flow_engine = match job.get("update_flow_engine") {
        Some(update_flow_engine) => update_flow_engine.as_bool().unwrap(),
        None => false,
    };
    let message = match job.get("message") {
        Some(message) => match message.as_str() {
            Some(message) => Some(message.to_string()),
            None => None,
        },
        None => None,
    };
    crate::utils::update_job_status(
        job_hash,
        status,
        update_flow_engine,
        message,
        JobType::Remote(worker_hash),
    )
    .await;

    (
        StatusCode::OK,
        Json(json!({"message": "Updated jobs status"})),
    )
}

pub async fn get_job_status(
    State(state): State<SharedState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let job_hash = match payload.get("job") {
        Some(job) => crate::utils::get_hash(job.as_str().unwrap()),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "job_hash not provided"})),
            );
        }
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .scheduler_tx
        .send(ManagerMessage::GetJobField(job_hash, JobField::Status, tx))
        .await
        .unwrap();
    if let Ok(value) = rx.await {
        if let Some(JobFieldValue::Status(status)) = value {
            (
                StatusCode::OK,
                Json(
                    json!({"message": "Successfully fetched job status", "status": status.to_string()}),
                ),
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "No job found with the given hash"})),
            )
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"message": "Failed to get job status. Reason : tx is dropped."})),
        )
    }
}

pub async fn kill_job(
    State(state): State<SharedState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    //send message to worker manager channel to kill workers
    let job = match payload.get("job") {
        Some(job) => job.as_str().unwrap().to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "job not provided"})),
            );
        }
    };
    let job_hash = crate::utils::get_hash(&job);
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .scheduler_tx
        .send(ManagerMessage::KillJob(job_hash, None, Some(tx)))
        .await
        .unwrap();
    match rx.await {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(json!({"message": "Message sent to kill job"})),
        ),
        Ok(Err(())) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "Job not found or already completed."})),
            )
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"message": "Failed to kill job"})),
        ),
    }
}

pub async fn update_config(
    State(state): State<SharedState>,
    Json(config): Json<Config>,
) -> impl IntoResponse {
    info!(target:&state.logger, "Updating config with {:?}", config);
    let guard = state.config.read().await;
    match config.validate().await {
        Ok(_) => {}
        Err(e) => {
            drop(guard);
            return (StatusCode::BAD_REQUEST, Json(json!({"message": e})));
        }
    }
    drop(guard);
    config.update_config(&state.config).await;

    (StatusCode::OK, Json(json!({"message": "Updated config"})))
}

pub async fn refresh_protocols(
    State(state): State<SharedState>,
    Json(_): Json<serde_json::Value>,
) -> impl IntoResponse {
    let workarea = &CONFIG.get().unwrap().workarea;
    let guard = state.config.read().await;
    match crate::utils::create_protcols(
        &guard.default_protocol.as_ref().unwrap_or(&String::new()),
        &state.protocol_map,
        &workarea,
        false,
    )
    .await
    {
        Ok(_) => {
            let _ = CONFIG
                .get()
                .unwrap()
                .local_job_executor_tx
                .send(ManagerMessage::UpdateLocalFarm)
                .await;
        }
        Err(e) => {
            drop(guard);
            return (StatusCode::BAD_REQUEST, Json(json!({"message": e})));
        }
    };
    drop(guard);
    state
        .scheduler_tx
        .send(ManagerMessage::ResetRetries)
        .await
        .unwrap();
    (
        StatusCode::OK,
        Json(json!({"message": "Refreshed protocols Successfully"})),
    )
}

pub async fn kill_idle_worker(
    State(state): State<SharedState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let worker_hash = match payload.get("worker_hash") {
        Some(worker_hash) => worker_hash.as_u64().unwrap(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"message": "worker_hash not provided"})),
            );
        }
    };
    let _ = state
        .worker_tx
        .try_send(ManagerMessage::KillIdleWorker(worker_hash));
    (
        StatusCode::OK,
        Json(json!({"message": "Killed idle worker"})),
    )
}

pub async fn get_config(State(state): State<SharedState>) -> impl IntoResponse {
    let config_guard = state.config.read().await;
    let protocol_guard = state.protocol_map.read().await;
    let mut config = serde_json::json!({});
    // Local protocol is expected to be present always
    if let crate::dto::FarmConfig::Local(local_config) =
        &protocol_guard.get("local").unwrap().config
    {
        config = serde_json::json!({
                "max_local_cores": local_config.max_local_cores,
                "max_local_memory": local_config.max_local_memory,
                "default_protocol": config_guard.default_protocol.as_ref().unwrap(),
                "local_dir": local_config.local_disk,
                "local_disk_threshold": local_config.disk_threshold,
        })
    };
    drop(config_guard);
    drop(protocol_guard);
    (StatusCode::OK, Json(config))
}

pub async fn can_request_spark_workers(State(state): State<SharedState>) -> impl IntoResponse {
    if state.add_spark_worker.load(atomic::Ordering::Acquire) {
        (
            StatusCode::OK,
            Json(json!({"message": "Can request spark workers"})),
        )
    } else {
        (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"message": "Spark worker queue is full"})),
        )
    }
}

pub async fn get_running_workers_count(
    State(state): State<SharedState>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    let farm = match payload.get("farm") {
        Some(farm) => {
            let farm = farm.as_str().unwrap();
            if farm == "all" {
                None
            } else {
                Some(farm.to_string())
            }
        }
        None => None,
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .worker_tx
        .send(ManagerMessage::GetWorkersCount(tx, farm))
        .await
        .unwrap();
    let count = rx.await.unwrap();
    if let Some(count) = count {
        (
            StatusCode::OK,
            Json(json!({"message": "Successfully fetched running workers count", "count": count})),
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"message": "No running workers found for the given farm"})),
        )
    }
}

pub async fn run_spark_job(
    State(state): State<SharedState>,
    Json(job): Json<SparkJob>,
) -> impl IntoResponse {
    // Add job to job mapper
    info!(target: &state.logger, "Recieved spark job: {:?}", job.name);
    // TODO: Add validation for job
    match job.validate().await {
        Ok(_) => {
            let mut guard = CONFIG.get().unwrap().spark_job_handler.jobs.write().await;
            guard.push_back(job);
            drop(guard);
            CONFIG
                .get()
                .unwrap()
                .spark_job_handler
                .notifier
                .notify_one();
            (
                StatusCode::OK,
                Json(json!({"message": "Job added successfully"})),
            )
        }
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"message": e})));
        }
    }
}
pub async fn kill_all_jobs(State(state): State<SharedState>) -> impl IntoResponse {
    // Add job to job mapper
    info!(target: &state.logger, "Killing All Jobs");
    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .worker_tx
        .send(ManagerMessage::Shutdown(tx))
        .await
        .unwrap();
    let _ = rx.await;
    (
        StatusCode::OK,
        Json(json!({"message": "All jobs killed successfully"})),
    )
}
