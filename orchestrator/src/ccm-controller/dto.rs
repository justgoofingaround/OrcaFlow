use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::RwLock;

pub use crate::common_utils::{Protocol,FarmConfig, DistributedConfig};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum Status {
    New,
    Running,
    Success,
    Failed,
    Killing,
    Killed,
    Queued,
    Unknown,
    Idle,
    DisQualified,
}
impl Status {
    pub fn to_string(&self) -> String {
        match self {
            Status::New => "new".to_string(),
            Status::Running => "running".to_string(),
            Status::Success => "success".to_string(),
            Status::Failed => "failed".to_string(),
            Status::Killing => "killing".to_string(),
            Status::Killed => "killed".to_string(),
            Status::Queued => "queued".to_string(),
            Status::Idle => "idle".to_string(),
            Status::Unknown => "unknown".to_string(),
            Status::DisQualified => "disqualified".to_string(),
        }
    }
    pub async fn from_str(status: &str) -> Status {
        match status {
            "new" => Status::New,
            "running" => Status::Running,
            "success" => Status::Success,
            "failed" => Status::Failed,
            "killing" => Status::Killing,
            "terminated" => Status::Killed,
            "killed" => Status::Killed,
            "queued" => Status::Queued,
            "idle" => Status::Idle,
            "disqualified" => Status::DisQualified,
            _ => Status::Unknown,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Config {
    pub parent_pids: Option<std::collections::VecDeque<u32>>,
    pub flow_engine_url: Option<String>,
    pub default_protocol: Option<String>,
}

#[derive(Debug)]
pub enum ManagerMessage {
    UpdateWorker(u64, Option<String>, Option<String>),
    KillWorker(u64, bool, Option<String>, bool),
    KillJob(u64, Option<u64>, Option<oneshot::Sender<Result<(), ()>>>),
    UpdateJobStatus(u64, Status, bool, Option<String>, JobType),
    AddJob(Job, String, bool, Option<String>, Option<oneshot::Sender<Result<(), String>>>),
    RunQueuedCategory(RunType),
    KillIdleWorker(u64),
    RunJob(Job, String, Option<String>),
    UpdateQueued(u64),
    UpdateWorkerStatus(u64, Status, u64, u64, Option<String>),
    FailJobs(u64, Option<String>),
    UpdateLocalUsage(Spec, oneshot::Sender<Vec<u64>>),
    Shutdown(oneshot::Sender<()>),
    GetWorkersCount(oneshot::Sender<Option<u32>>, Option<String>),
    UpdateJobIdentifier(u64, Identifier),
    GetJobField(u64, JobField, oneshot::Sender<Option<JobFieldValue>>),
    IncrementRetry(u64, Option<String>),
    ResetRetries,
    UpdateLocalFarm,
}

#[derive(Debug)]
pub enum JobFieldValue {
    JobName(String),
    JobType(String),
    Status(Status),
    Protocol(String),
    Specs(Vec<Spec>),
    Lightweight(bool),
    LightweightSpec(Option<Spec>),
    Identifier(Option<Identifier>),
}

#[derive(Debug)]
pub enum JobField {
    JobName,
    JobType,
    Status,
    Protocol,
    Specs,
    Lightweight,
    LightweightSpec,
    Identifier,
}

#[derive(Debug, PartialEq, Eq)]
pub enum JobType {
    Local,
    Remote(u64),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RunType {
    Local(u64),
    Remote(u64),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash, Eq, Copy)]
pub struct Spec {
    pub memory: i64,
    pub cores: i64,
}

// This struct represents a job
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Job {
    pub job_name: String,
    pub job_type: String,
    pub status: Status,
    pub protocol: String,
    pub specs: Vec<Spec>,
    pub lightweight: bool,
    pub lightweight_spec: Option<Spec>,
    pub identifier: Option<Identifier>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum Identifier {
    Pid(u32),
    Worker(u64),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Worker {
    pub name: String,
    pub identifier: Option<String>,
    pub job_id: Option<String>,
    pub protocol: String,
    pub categories: Vec<u64>,
}

#[derive(Parser, Debug)]
pub struct SetupArgs {
    #[clap(long, default_value_t = 40.0)]
    pub max_local_memory: f64,
    #[clap(long, default_value_t = 6.0)]
    pub max_local_cores: f32,
    #[clap(short, long, default_value_t = 5000)]
    pub port: i64,
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub parent_pids: Vec<u32>,
    #[clap(short, long, default_value_t = std::env::current_dir().unwrap().to_str().unwrap().to_string())]
    pub workarea: String,
    #[clap(long, default_value_t = String::new())]
    pub spark_master_url: String,
    #[clap(long, default_value_t = String::new())]
    pub spark_web_url: String,
    #[clap(long)]
    pub default_protocol: String,
    #[clap(long, default_value_t = String::new())]
    pub protocol_path: String,
}

#[derive(Debug)]
pub struct Setup {
    pub spark_master_url: String,
    pub workarea: String,
    pub worker_manager_tx: tokio::sync::mpsc::Sender<ManagerMessage>,
    pub local_job_executor_tx: tokio::sync::mpsc::Sender<ManagerMessage>,
    pub cluster_home: String,
    pub job_scheduler_tx: tokio::sync::mpsc::Sender<ManagerMessage>,
    pub config: Arc<RwLock<Config>>,
    pub add_spark_worker: Arc<std::sync::atomic::AtomicBool>,
    pub spark_job_handler: SparkJobHandler,
    pub protocols_map: Arc<RwLock<std::collections::HashMap<String, Protocol>>>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct SparkJob {
    pub name: String,
    pub workarea: String,
    pub app_cmd: String,
    pub spec: Spec,
    pub executor_spec: Spec,
    pub conf: serde_json::Value,
    pub protocol: String,
}

#[derive(Debug)]
pub struct SparkJobHandler {
    pub jobs: Arc<RwLock<VecDeque<SparkJob>>>,
    pub notifier: Arc<tokio::sync::Notify>,
}
