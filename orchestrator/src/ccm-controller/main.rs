
use std::sync:: OnceLock;

mod apis;
mod utils;
mod setup;
mod dto;
mod worker_manager;
mod spark;
mod local_job_executor;
mod job_scheduler;

#[path = "../common_utils/mod.rs"]
mod common_utils;

pub static CONFIG: OnceLock<dto::Setup> = OnceLock::new();

#[tokio::main]
async fn main() {
    setup::setup().await;
}
