use std::time::Duration;

use clap::Parser;
use tonic::transport::Server;

use crate::common_utils;
use tokio::sync::{Notify, RwLock};
use std::sync::Arc;
use crate::grpc::proto::worker_server;
use crate::grpc::WorkerService;

#[derive(Parser, Debug)]
pub struct SetupArgs {
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub job_names: Vec<String>,
    #[clap(long)]
    pub master_url: String,
    #[clap(long)]
    pub master_workarea: String,
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub categories: Vec<u64>,
    #[clap(long)]
    pub protocol: String,
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub job_types: Vec<String>,
    #[clap(long, value_parser, num_args = 0.., value_delimiter = ' ')]
    pub lightweight: Vec<bool>,
    #[clap(long)]
    pub name: String,
    #[clap(long)]
    pub max_cores: i32,
    #[clap(long)]
    pub max_memory: i32,
    #[clap(long)]
    pub cluster_home: String,
    #[clap(long)]
    pub worker_hash: u64,
    #[clap(long)]
    pub spark_master_url: String,
}

#[derive(Debug)]
pub struct Config {
    pub logger_name: String,
    pub max_cores: i32,
    pub max_memory: i32,
    pub worker_hash: u64,
    pub cluster_home: String,
    pub master_url: String,
    pub master_workarea: String,
    pub running_jobs: Arc<RwLock<std::collections::HashMap<u64, u32>>>,
    pub notifier: Arc<Notify>,
    pub spark_master_url: String,
    pub protocol: String,
}

pub async fn setup_logger(logger_name: &str, work_area: &str) {
    let logger = common_utils::general::create_logging(logger_name, work_area);
    let stdout = log4rs::append::console::ConsoleAppender::builder().build();
    let config = log4rs::Config::builder()
        .appender(log4rs::config::Appender::builder().build("stdout", Box::new(stdout)))
        .appender(log4rs::config::Appender::builder().build(logger_name, Box::new(logger)))
        .logger(
            log4rs::config::Logger::builder()
                .appender(logger_name)
                .build(logger_name, log::LevelFilter::Info),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Info),
        )
        .unwrap();
    let _ = log4rs::init_config(config).unwrap();
}

pub async fn setup_rpc(addr: core::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .timeout(Duration::from_secs(10))
        .add_service(worker_server::WorkerServer::new(WorkerService::new()))
        .serve(addr)
        .await?;
    Ok(())
}
