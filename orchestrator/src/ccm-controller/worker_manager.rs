use log::{error, info, warn};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

use crate::dto::{Identifier, JobType, ManagerMessage, RunType, Status, Worker};
use crate::utils;
use crate::CONFIG;

#[allow(unused_mut, unused_assignments)]
pub async fn worker_manager(mut rx: Receiver<ManagerMessage>) {
    let protocol_map = Arc::clone(&CONFIG.get().unwrap().protocols_map);
    let table_path = format!(
        "{}/worker.redb",
        utils::get_or_create_db_dir(&CONFIG.get().unwrap().workarea).await
    );
    let db = match utils::get_db(&table_path) {
        Ok(db) => db,
        Err(e) => {
            panic!("Error opening database: {}", e);
        }
    };
    let mut write_txn = utils::get_write_transaction(&db);
    let config = CONFIG.get().unwrap().config.clone();
    // All the logic to update workers will be here.
    let worker_script_path = std::path::PathBuf::from(std::env::current_exe().unwrap())
        .parent()
        .unwrap()
        .join("ccm-agent")
        .to_str()
        .unwrap()
        .to_owned();
    let scheduler_tx = CONFIG.get().unwrap().job_scheduler_tx.clone();
    let mut idle_workers_map: HashMap<u64, VecDeque<u64>> = HashMap::new();
    let mut queued_workers_in_grid: HashMap<u64, u64> = HashMap::new();
    let mut queued_jobs_in_scheduler: HashMap<String, VecDeque<u64>> = HashMap::new();
    let logger_name = "worker_manager";
    // This mapper will keep track of running workers and their jobs
    // This is used to see what jobs a worker is running
    let mut running_worker_job_mapper: HashMap<String, HashMap<u64, Vec<u64>>> = HashMap::new();

    while let Some(message) = rx.recv().await {
        info!(target: logger_name, "Received message: {:?}", message);
        match message {
            // This code block handles various messages received by the worker manager.
            // It processes the `RunJob` message to launch a job, `KillWorker` message to forcefully kill a worker,
            // `KillJob` message to kill a job, and `AddWorker` message to add or update a worker in the map of workers.
            // The code performs tasks such as getting the launcher script, checking if the protocol exists,
            // finding an idle worker, queuing jobs, creating worker commands, updating job statuses,
            // and spawning worker tasks.
            // It also maintains maps for idle workers, running workers, queued workers, and running worker-job mappings.
            // The code handles different completion statuses of workers and updates the maps accordingly.
            // Overall, it manages the execution and control flow of jobs and workers in the cluster.
            ManagerMessage::RunJob(job, command, cache_dir) => {
                info!(target: logger_name, "Running job: {}", job.job_name);
                let ts = chrono::prelude::Utc::now().timestamp();
                // Get launcher script from mapper
                // if it does not exist then skip launching the job
                // Get the default protocol if job protocol is default
                let mut job_protocol = job.protocol.clone();
                let config_guard = config.read().await;
                if job_protocol == "default" {
                    job_protocol = config_guard.default_protocol.as_ref().unwrap().to_string();
                }
                if job_protocol == "local" {
                    job.queue_job(command, cache_dir, false).await;
                    drop(config_guard);
                    continue;
                }
                let job_hash = job.get_hash();
                let guard = protocol_map.read().await;
                // Unwrapping config since job protocol cannot be local
                let (farm_config, envs) = match guard.get(&job_protocol) {
                    Some(protocol) => (
                        utils::get_farm_config(&protocol.config).unwrap(),
                        protocol.get_envs().await,
                    ),
                    None => {
                        let message = format!("Launcher not found for protocol: {} used by job: {}. Skipping launching of this job", job.protocol, job.job_name);
                        crate::utils::update_job_status(
                            job_hash,
                            Status::Failed,
                            true,
                            Some(message),
                            JobType::Remote(0),
                        )
                        .await;
                        drop(config_guard);
                        drop(guard);
                        continue;
                    }
                };
                drop(config_guard);
                let job_category: u64 = job.get_category();
                // If we get a idle worker then submit the job to the worker
                if let Ok(worker_hash) = utils::submit_job_to_idle_worker(
                    &mut idle_workers_map,
                    job_category,
                    &mut write_txn,
                    &job,
                    logger_name,
                    &mut running_worker_job_mapper,
                    Some(envs),
                    job_hash,
                    &command,
                    &cache_dir,
                )
                .await
                {
                    let _ = scheduler_tx
                        .send(ManagerMessage::UpdateJobIdentifier(
                            job_hash,
                            Identifier::Worker(worker_hash),
                        ))
                        .await;
                    continue;
                };
                // send message to job scheduler to add this job to queue
                let jobs = job.get_dummy_jobs().await;
                job.queue_job(command, cache_dir, false).await;
                if let Some(queued_jobs) = queued_jobs_in_scheduler.get_mut(&job_protocol) {
                    queued_jobs.push_back(job_category);
                } else {
                    let mut queued_jobs = VecDeque::new();
                    queued_jobs.push_back(job_category);
                    queued_jobs_in_scheduler.insert(job_protocol.clone(), queued_jobs);
                }
                if queued_workers_in_grid.contains_key(&job_category) {
                    continue;
                }
                let running_workers = running_worker_job_mapper
                    .get(&job_protocol)
                    .unwrap_or(&HashMap::new())
                    .len() as i8;
                if running_workers >= farm_config.max_workers {
                    info!(target: logger_name, "Max workers reached for protocol: {}", job_protocol);
                    continue;
                }
                let mut worker_name = format!(
                    "{}_{}_{}",
                    ts,
                    job_protocol,
                    utils::get_len_of_table(&mut write_txn)
                );
                let worker_hash = utils::get_hash(&worker_name);
                match jobs
                    .get(0)
                    .unwrap()
                    .get_command(
                        &worker_name,
                        &worker_script_path,
                        &farm_config.script,
                        jobs.get(1).unwrap(),
                        worker_hash,
                    )
                    .await
                {
                    Ok(command) => {
                        let mut categories = Vec::new();
                        for job in &jobs {
                            // Add worker to workers map
                            categories.push(job.get_category());
                        }
                        let worker = Worker::new(&worker_name, job_protocol.clone(), categories);
                        write_txn = utils::write_to_worker_db(worker_hash, &worker, write_txn, &db);
                        queued_workers_in_grid.insert(job_category, worker_hash);
                        match running_worker_job_mapper.get_mut(&job_protocol) {
                            Some(workers) => {
                                workers.insert(worker_hash, vec![]);
                            }
                            None => {
                                let mapper = HashMap::from([(worker_hash, vec![])]);
                                running_worker_job_mapper.insert(job_protocol.clone(), mapper);
                            }
                        }
                        tokio::spawn(utils::spawn_workers(
                            worker_hash,
                            worker_name,
                            command,
                            logger_name,
                            job_protocol,
                            jobs.get(0).unwrap().get_category(),
                        ));
                    }
                    Err(_) => {
                        error!(target: logger_name, "Failed to create worker command");
                    }
                }
                drop(guard);
            }
            ManagerMessage::KillWorker(worker_hash, _force, message, update_flow_engine) => {
                // Kill the worker
                // This is used to forcefully kill the worker
                if let Some(mut worker) = utils::remove_from_worker_db(worker_hash, &mut write_txn)
                {
                    utils::kill_worker(
                        worker_hash,
                        &mut running_worker_job_mapper,
                        &mut worker,
                        &scheduler_tx,
                        logger_name,
                        update_flow_engine,
                        message,
                        true,
                        &mut queued_jobs_in_scheduler,
                    )
                    .await;
                }
            }
            ManagerMessage::KillJob(job_hash, option_hash, _) => {
                // Kill the job
                match option_hash {
                    Some(worker_hash) => {
                        if let Some(mut worker) =
                            utils::read_from_worker_db(worker_hash, &mut write_txn)
                        {
                            if let Some(workers) =
                                running_worker_job_mapper.get_mut(&worker.protocol)
                            {
                                if let Some(jobs) = workers.get_mut(&worker_hash) {
                                    if jobs.contains(&job_hash) {
                                        jobs.retain(|x| x != &job_hash);
                                        worker
                                            .kill_worker(
                                                logger_name.to_string(),
                                                Some(job_hash),
                                                false,
                                            )
                                            .await;
                                    }
                                };
                            }
                        }
                    }
                    None => {
                        warn!(target: logger_name, "No worker found for job: {}", job_hash);
                    }
                }
            }
            ManagerMessage::UpdateWorker(worker_hash, job_id, url) => {
                // Update worker with job id and url
                if let Some(mut worker) = utils::read_from_worker_db(worker_hash, &mut write_txn) {
                    if let Some(job_id) = job_id {
                        worker.job_id = Some(job_id);
                    }
                    // Registering worker after it started in the grid
                    // Making dummy jobs as idle and sending message to job scheduler to run the queued category
                    if let Some(url) = url {
                        worker.identifier = Some(url);
                        for category in worker.categories.iter() {
                            utils::add_to_idle_workers_map(
                                &mut idle_workers_map,
                                worker_hash,
                                category,
                            )
                            .await;
                            scheduler_tx
                                .send(ManagerMessage::RunQueuedCategory(RunType::Remote(
                                    category.to_owned(),
                                )))
                                .await
                                .unwrap();
                            if let Some(queued_hash) = queued_workers_in_grid.get_mut(&category) {
                                // Send message to job scheduler to run queued category
                                if worker_hash == *queued_hash {
                                    let mut queued_category = category.to_owned();
                                    queued_workers_in_grid.remove(&category);
                                    utils::update_queued_workers_and_schedule_job(
                                        queued_category,
                                        &mut queued_jobs_in_scheduler,
                                        &worker.protocol,
                                        &scheduler_tx,
                                        logger_name,
                                    )
                                    .await;
                                }
                            } else {
                                warn!(target: logger_name, "Worker {} not found in queued workers", worker_hash);
                            }
                        }
                    }
                    write_txn = utils::write_to_worker_db(worker_hash, &worker, write_txn, &db);
                    if worker.job_id.is_some() && worker.identifier.is_some() {
                        tokio::spawn(async move {
                            worker.monitor_identifier(logger_name).await;
                        });
                    }
                }
            }
            ManagerMessage::UpdateWorkerStatus(
                worker_hash,
                status,
                job_hash,
                category,
                message,
            ) => {
                // Updates the worker status
                // If worker is idle then add it to idle workers map
                if let Some(worker) = utils::read_from_worker_db(worker_hash, &mut write_txn) {
                    match status {
                        // Update running worker mapper
                        Status::Running => {}
                        Status::DisQualified => {
                            // Remove job from running jobs but dont add disqualified category to idle jobs
                            if let Some(workers) =
                                running_worker_job_mapper.get_mut(&worker.protocol)
                            {
                                if let Some(jobs) = workers.get_mut(&worker_hash) {
                                    jobs.retain(|x| x != &job_hash);
                                };
                            }
                        }
                        Status::Idle => {
                            // Update running jobs in worker
                            // Worker is killed only if it is not running any jobs or else worker is set to idle
                            if let Some(workers) =
                                running_worker_job_mapper.get_mut(&worker.protocol)
                            {
                                if let Some(jobs) = workers.get_mut(&worker_hash) {
                                    jobs.retain(|x| x != &job_hash);
                                    utils::add_to_idle_workers_map(
                                        &mut idle_workers_map,
                                        worker_hash,
                                        &category,
                                    )
                                    .await;
                                };
                            };
                        }
                        Status::Failed => {
                            let category = worker.categories[0];
                            scheduler_tx
                                .send(ManagerMessage::IncrementRetry(category, message))
                                .await
                                .unwrap();

                            for category in &worker.categories {
                                if let Some(queued_hash) = queued_workers_in_grid.get(&category) {
                                    // Send message to job scheduler to run queued category
                                    if worker_hash == *queued_hash {
                                        info!(target: logger_name, "Removing worker {} from queued workers of category {}", worker_hash, category);
                                        utils::update_queued_workers_and_schedule_job(
                                            category.to_owned(),
                                            &mut queued_jobs_in_scheduler,
                                            &worker.protocol,
                                            &scheduler_tx,
                                            logger_name,
                                        )
                                        .await;
                                        queued_workers_in_grid.remove(&category);
                                    }
                                } else {
                                    warn!(target: logger_name, "Worker {} not found in queued workers", worker_hash);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            ManagerMessage::UpdateQueued(worker_hash) => {
                // Remove worker from queued workers
                if let Some(worker) = utils::read_from_worker_db(worker_hash, &mut write_txn) {
                    for category in &worker.categories {
                        if let Some(queued_hash) = queued_workers_in_grid.get(&category) {
                            // Send message to job scheduler to run queued category
                            if worker_hash == *queued_hash {
                                info!(target: logger_name, "Removing worker {} from queued workers of category {}", worker_hash, category);
                                utils::update_queued_workers_and_schedule_job(
                                    category.to_owned(),
                                    &mut queued_jobs_in_scheduler,
                                    &worker.protocol,
                                    &scheduler_tx,
                                    logger_name,
                                )
                                .await;
                                queued_workers_in_grid.remove(&category);
                            }
                        } else {
                            warn!(target: logger_name, "Worker {} not found in queued workers", worker_hash);
                        }
                    }
                }
            }
            ManagerMessage::KillIdleWorker(worker_hash) => {
                // Skip killing of idle worker if the worker is running any jobs
                let mut kill_worker = false;
                if let Some(worker) = utils::read_from_worker_db(worker_hash, &mut write_txn) {
                    if let Some(workers) = running_worker_job_mapper.get_mut(&worker.protocol) {
                        if let Some(jobs) = workers.get(&worker_hash) {
                            if jobs.len() > 0 {
                                continue;
                            };
                        };
                    };
                    // If worker is still idle then kill the worker
                    // If worker is not idle mapper then worker is not idle. Skip killing
                    for category in &worker.categories {
                        if let Some(mut idle_workers) = idle_workers_map.get_mut(&category) {
                            if idle_workers.contains(&worker_hash) {
                                // Remove worker from idle workers
                                idle_workers.retain(|x| x != &worker_hash);
                                // Kill the worker
                                kill_worker = true;
                            }
                        }
                    }
                } else {
                    warn!(target: logger_name, "Worker not found for: {}", worker_hash);
                }
                if kill_worker {
                    if let Some(mut worker) =
                        utils::remove_from_worker_db(worker_hash, &mut write_txn)
                    {
                        if let Some(workers) = running_worker_job_mapper.get_mut(&worker.protocol) {
                            workers.remove(&worker_hash);
                        }
                        worker
                            .kill_worker(logger_name.to_string(), None, false)
                            .await;
                        for category in worker.categories {
                            utils::update_queued_workers_and_schedule_job(
                                category,
                                &mut queued_jobs_in_scheduler,
                                &worker.protocol,
                                &scheduler_tx,
                                logger_name,
                            )
                            .await;
                        }
                    }
                }
            }
            ManagerMessage::Shutdown(tx) => {
                while let Some(mut worker) = utils::pop_from_worker_db(&mut write_txn) {
                    worker
                        .kill_worker(logger_name.to_string(), None, false)
                        .await;
                }
                tx.send(()).unwrap();
            }
            ManagerMessage::GetWorkersCount(tx, option_protocol) => {
                let mut count = None;
                if let Some(protocol) = option_protocol {
                    if let Some(running_workers) = running_worker_job_mapper.get(&protocol) {
                        count = Some(running_workers.len() as u32);
                    }
                } else {
                    count = Some(utils::get_len_of_table(&mut write_txn) as u32);
                }
                tx.send(count).unwrap();
            }
            _ => {}
        }
    }
}
