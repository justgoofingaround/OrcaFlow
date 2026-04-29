use clap::Parser;
use serde::{Deserialize, Serialize};


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
            Status::Unknown => "unknown".to_string(),
            Status::Idle => "idle".to_string(),
            Status::DisQualified => "disqualified".to_string(),
        }
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config{
    pub max_memory: i64,
    pub max_cores: i64,
    pub parent_pids: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Hash, Eq, Copy)]
pub struct Spec {
    pub memory: i64,
    pub cores: i64,
}

// This struct represents a job
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    pub job_hash: u64,
    pub job_name: String,
    pub job_type: String,
    pub category: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Identifier {
   Pid(u32),
   Url(String),
}



#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Worker {
    pub name: String,
    pub status: Status,
    pub identifier: Option<String>,
    pub job_id: Option<String>,
    pub protocol: String,
}


#[derive(Parser, Debug)]
pub struct SetupArgs{
    #[clap(short, long, default_value_t = 180000)]
    pub memory: i64,
    #[clap(short, long, default_value_t = 32)]
    pub cores: i64,
    #[clap(short, long, default_value_t = 5000)]
    pub port: i64,
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub parent_pids: Vec<u32>,
    #[clap(short, long, default_value_t = std::env::current_dir().unwrap().to_str().unwrap().to_string())]
    pub workarea: String,
}