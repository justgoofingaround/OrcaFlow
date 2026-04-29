use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::dto::{ManagerMessage, Spec};
use crate::utils;

#[allow(unused_assignments)]
pub async fn job_actor(
    mut rx: Receiver<ManagerMessage>,
    worker_manager_tx: Sender<ManagerMessage>,
) {
    let protocol_map = Arc::clone(&crate::CONFIG.get().unwrap().protocols_map);
    let mut current_spec = Spec {
        memory: 0,
        cores: 0,
    };
    let mut local_spec = Spec {
        memory: 0,
        cores: 0,
    };
    let guard = protocol_map.read().await;
    if let Some(local) = guard.get("local") {
        if let crate::dto::FarmConfig::Local(config) = &local.config {
            local_spec.memory = config.max_local_memory;
            local_spec.cores = config.max_local_cores;
        }
    }
    drop(guard);
    let mut spec_job_mapper: HashMap<u64, u64> = HashMap::new();
    let logger_name = "local_job_executor";
    let mut specs: Vec<crate::dto::Spec> = Vec::new();
    let mut parent_pids = VecDeque::new();
    let mut low_priority_mapper: HashMap<u64, u64> = HashMap::new();
    while let Some(message) = rx.recv().await {
        log::info!(target: logger_name, "Received message: {:?}", message);
        match message {
            ManagerMessage::RunJob(job, command, cache_dir) => {
                job.add_job_to_queue(
                    logger_name,
                    &mut parent_pids,
                    local_spec.memory,
                    local_spec.cores,
                    &mut spec_job_mapper,
                    &mut low_priority_mapper,
                    &mut specs,
                    &worker_manager_tx,
                    &mut current_spec,
                    command,
                    &protocol_map,
                    cache_dir,
                )
                .await;
            }
            ManagerMessage::UpdateLocalUsage(spec, tx) => {
                current_spec.remove(&spec);
                // Send message to job scheduler since job is completed
                // this loop will check the queued jobs that can be scheduled
                // If a job is present which can be scheduled with the available resources it will be scheduled
                // Compute resource usage of all the parent processes(Usually EDA process)
                if spec_job_mapper.is_empty() && low_priority_mapper.is_empty() {
                    tx.send(vec![]).unwrap_or_else(|_| {
                        log::error!(target: logger_name, "Failed to send empty vector on tx");
                    });
                    continue;
                };
                let spec = crate::utils::compute_resource_usage(
                    &mut parent_pids,
                    local_spec.memory,
                    local_spec.cores,
                    logger_name,
                    &current_spec,
                )
                .await;
                let categories = utils::get_queued_jobs(
                    &specs,
                    &mut spec_job_mapper,
                    &mut low_priority_mapper,
                    &spec,
                );
                tx.send(categories).unwrap_or_else(|_| {
                    log::error!(target: logger_name, "Failed to send categories on tx");
                });
            }
            ManagerMessage::UpdateLocalFarm => {
                let guard = protocol_map.read().await;
                if let Some(local) = guard.get("local") {
                    if let crate::dto::FarmConfig::Local(config) = &local.config {
                        local_spec.memory = config.max_local_memory;
                        local_spec.cores = config.max_local_cores;
                    }
                }
                drop(guard);
            }
            _ => {}
        }
    }
}
