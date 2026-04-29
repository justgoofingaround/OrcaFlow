use log::error;

use crate::data_types::Status;

pub async fn update_job_status(
    job_hash: u64,
    status: &Status,
    message: Option<String>,
    master_url: &str,
    logger_name: &str,
    worker_hash: u64,
) {
    let endpoint = format!("{}/update_job_status", master_url);
    let update_flow_engine = match status {
        Status::Killed | Status::Failed => true,
        _ => false,
    };
    let payload = serde_json::json!({
        "status": status.to_string(),
        "job_hash": job_hash,
        "update_flow_engine": update_flow_engine,
        "message": message,
        "worker_hash": worker_hash,
    });
    // let payload = serde_json::json!(worker);
    match crate::common_utils::submit_post_request(&payload, &endpoint).await {
        Ok(_) => {}
        Err(e) => {
            error!(target: logger_name, "Error in sending request to master: {}", e);
        }
    }
}

pub async fn update_worker(worker_hash: u64, url: &str, master_url: &str) -> Result<(), String> {
    let endpoint = format!("{}/update_worker", master_url);
    let payload = serde_json::json!({
        "worker_hash": worker_hash,
        "worker_url": url,
    });
    match crate::common_utils::submit_post_request(&payload, &endpoint).await {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn kill_idle_worker(
    worker_hash: u64,
    master_url: &str,
    logger_name: &str,
) -> Result<(), ()> {
    let endpoint = format!("{}/kill_idle_worker", master_url);
    let payload = serde_json::json!({
        "worker_hash": worker_hash,
    });
    match crate::common_utils::submit_post_request(&payload, &endpoint).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(target: logger_name, "Error in sending request to master: {}", e);
            Err(())
        }
    }
}

// pub async fn monitor_master() {
//     let mut retry = 0;
//     while retry < 1 {
//         tokio::time::sleep(tokio::time::Duration::from_secs(120)).await;
//         match crate::common_utils::submit_get_request(&crate::CONFIG.get().unwrap().master_url)
//             .await
//         {
//             Ok(_) => {
//                 retry = 0;
//             }
//             Err(e) => {
//                 retry += 1;
//                 error!(target: &crate::CONFIG.get().unwrap().logger_name, "Error in sending request to master: {}", e);
//             }
//         }
//     }
//     kill_pid(
//         std::process::id(),
//         &crate::CONFIG.get().unwrap().logger_name,
//         false,
//     )
//     .await;
// }
