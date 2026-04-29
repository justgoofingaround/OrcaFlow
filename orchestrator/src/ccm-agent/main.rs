use clap::Parser;
use local_ip_address;
use std::sync::OnceLock;

#[path = "../common_utils/mod.rs"]
mod common_utils;
#[path = "../data_types/mod.rs"]
mod data_types;
mod grpc;
mod setup;
mod utils;
use common_utils::{
    append_data_to_file, get_unused_port, get_worker_error_file_path, is_worker_qualified,
};

pub static CONFIG: OnceLock<setup::Config> = OnceLock::new();
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_args = setup::SetupArgs::parse();
    let mut system = sysinfo::System::new();
    match is_worker_qualified(
        setup_args.max_cores as i64,
        setup_args.max_memory as i64,
        &mut system,
        &setup_args.master_url,
        &setup_args.master_workarea,
    )
    .await
    {
        Ok(_) => {}
        Err(message) => {
            let _ = append_data_to_file(
                &get_worker_error_file_path(&setup_args.master_workarea, &setup_args.name).await,
                &message,
            )
            .await;
            std::process::exit(1);
        }
    }
    setup::setup_logger(&setup_args.name, &setup_args.master_workarea).await;
    let mut worker_url: String = format!(
        "{}:{}",
        local_ip_address::local_ip().unwrap(),
        get_unused_port().await
    );
    let addr = worker_url.parse()?;
    CONFIG
        .set(setup::Config {
            logger_name: setup_args.name,
            max_cores: setup_args.max_cores,
            max_memory: setup_args.max_memory,
            worker_hash: setup_args.worker_hash,
            cluster_home: setup_args.cluster_home,
            master_url: setup_args.master_url,
            master_workarea: setup_args.master_workarea,
            running_jobs: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            notifier: std::sync::Arc::new(tokio::sync::Notify::new()),
            spark_master_url: setup_args.spark_master_url,
            protocol: setup_args.protocol,
        })
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = setup::setup_rpc(addr).await;
    });
    std::env::set_var("DDASH_CCM_AGENT_URL", &worker_url);
    worker_url = format!("http://{}", worker_url);
    match utils::update_worker(
        setup_args.worker_hash,
        &worker_url,
        &CONFIG.get().unwrap().master_url,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            log::error!(target: &CONFIG.get().unwrap().logger_name, "Error in sending request to master: {}. Exiting ccm-agent", e);
            std::process::exit(1);
        }
    };
    tokio::spawn(crate::utils::monitor_idle_state());
    handle.await?;
    log::info!(target: &CONFIG.get().unwrap().logger_name, "ccm-agent exiting.");
    Ok(())
}
