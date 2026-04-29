use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

use crate::common_utils::delete_file;
use crate::dto::{JobType, ManagerMessage, RunType, Status};
use crate::utils::{
    add_job_to_queue, add_spark_worker, commit_to_job_db, drop_job_from_queue,
    fail_jobs_of_category, get_db, get_fail_category, get_job_from_db, get_max_retry_count,
    get_or_create_db_dir, get_write_transaction, increment_retries, log_job_status, schedule_job,
};
use crate::CONFIG;

pub const MAX_QUEUED_SPARK_JOBS: i64 = 10;

pub async fn scheduler(mut rx: Receiver<ManagerMessage>) {
    let protocols = Arc::clone(&CONFIG.get().unwrap().protocols_map);
    let table_path = format!(
        "{}/job.redb",
        get_or_create_db_dir(&CONFIG.get().unwrap().workarea).await
    );
    let db = match get_db(&table_path) {
        Ok(db) => db,
        Err(e) => {
            panic!("Error opening database: {}", e);
        }
    };
    let completion_status = [
        Status::Success,
        Status::Failed,
        Status::Killed,
        Status::DisQualified,
    ];
    let config = CONFIG.get().unwrap().config.clone();
    let mut queue: HashMap<u64, VecDeque<u64>> = HashMap::new();
    let local_job_executor_tx = CONFIG.get().unwrap().local_job_executor_tx.clone();
    let mut running_spark_workers = HashSet::new();
    let mut running_spark_apps = 0;
    let mut spark_worker_queue: HashMap<u64, VecDeque<u64>> = HashMap::new();
    let mut running_queue: HashMap<u64, VecDeque<u64>> = HashMap::new();
    let logger_name = "job_scheduler";
    let worker_manager_tx = CONFIG.get().unwrap().worker_manager_tx.clone();
    let mut write_txn = get_write_transaction(&db);
    let spark_monitor = std::sync::Arc::clone(&CONFIG.get().unwrap().add_spark_worker);
    let mut category_retries: HashMap<u64, i8> = HashMap::new();
    while let Some(message) = rx.recv().await {
        log::info!(target: logger_name, "Received message: {:?}", message);
        match message {
            ManagerMessage::AddJob(job, command, can_schedule, cache_dir, option_tx) => {
                let spark_worker_spec = job.specs.get(0).unwrap().clone();
                let app_name = job.job_name.clone();
                let is_spark_driver = job.job_type.contains("spark-driver");
                let response = add_job_to_queue(
                    &mut queue,
                    job,
                    can_schedule,
                    &mut spark_worker_queue,
                    &local_job_executor_tx,
                    &mut running_spark_workers,
                    &mut running_spark_apps,
                    &config,
                    &spark_monitor,
                    command,
                    &mut write_txn,
                    cache_dir,
                    &mut running_queue,
                    &category_retries,
                )
                .await;
                if let Some(tx) = option_tx {
                    let _ = tx.send(response);
                };
                if is_spark_driver && can_schedule {
                    add_spark_worker(
                        &mut queue,
                        true,
                        &mut spark_worker_queue,
                        &local_job_executor_tx,
                        &mut running_spark_workers,
                        &mut running_spark_apps,
                        &config,
                        &spark_monitor,
                        &mut write_txn,
                        &spark_worker_spec,
                        &app_name,
                        &mut running_queue,
                        &category_retries,
                    )
                    .await;
                }
                let _ = write_txn.commit();
                write_txn = get_write_transaction(&db);
            }
            ManagerMessage::RunQueuedCategory(run_type) => {
                let (category, tx) = match run_type {
                    RunType::Local(category) => (category, &local_job_executor_tx),
                    RunType::Remote(category) => (category, &worker_manager_tx),
                };
                schedule_job(
                    &mut queue,
                    &mut spark_worker_queue,
                    category,
                    &mut write_txn,
                    logger_name,
                    tx,
                    &local_job_executor_tx,
                    &protocols,
                    &spark_monitor,
                    &config,
                    &mut running_spark_workers,
                    &mut running_spark_apps,
                    &mut running_queue,
                    &category_retries,
                )
                .await;
            }
            ManagerMessage::UpdateJobStatus(
                job_hash,
                mut status,
                update_flow_engine,
                mut message,
                job_type,
            ) => {
                if let Some((mut job, command, cache_dir)) =
                    get_job_from_db(job_hash, &mut write_txn)
                {
                    let category = job.get_category();
                    if job.status != status && !completion_status.contains(&job.status) {
                        if status == Status::DisQualified {
                            let fail_category = get_fail_category(&job).await;
                            job.identifier = None;
                            let mut response = Ok(());
                            println!("{:?}", response);
                            (write_txn, response) = increment_retries(
                                fail_category,
                                message,
                                logger_name,
                                &mut queue,
                                &mut running_queue,
                                write_txn,
                                &db,
                                &mut category_retries,
                            )
                            .await;
                            match response {
                                Ok(()) => {
                                    let _ = add_job_to_queue(
                                        &mut queue,
                                        job.clone(),
                                        false,
                                        &mut spark_worker_queue,
                                        &local_job_executor_tx,
                                        &mut running_spark_workers,
                                        &mut running_spark_apps,
                                        &config,
                                        &spark_monitor,
                                        command.clone(),
                                        &mut write_txn,
                                        cache_dir.clone(),
                                        &mut running_queue,
                                        &category_retries,
                                    )
                                    .await;
                                }
                                Err(()) => {}
                            }
                        }
                        // Update to flow engine if update flow engine is true or job status is killing
                        // Check if job identifier exists when
                        else if update_flow_engine || job.status == Status::Killing {
                            // If job's previous state was killing, then update the status as killed and send message as successfully killed.
                            if job.status == Status::Killing {
                                status = Status::Killed;
                                message = Some("Job killed successfully".to_string());
                            }
                            job.status = status;
                            let guard = config.read().await;
                            log_job_status(
                                &guard.flow_engine_url,
                                &job,
                                &message,
                                logger_name,
                                &cache_dir,
                            )
                            .await;
                            drop(guard);
                        }
                        job.status = status;
                        if completion_status.contains(&job.status) {
                            running_queue
                                .get_mut(&category)
                                .map(|jobs| jobs.retain(|&x| x != job_hash));
                            if job.job_type == String::from("spark_worker") {
                                running_spark_workers.remove(&job_hash);
                                if running_spark_workers.is_empty() {
                                    spark_monitor.store(true, std::sync::atomic::Ordering::Release);
                                }
                            } else if job.job_type.contains("spark-driver") {
                                running_spark_apps -= 1;
                                // Empty spark worker queue if there are no running spark apps
                                if running_spark_apps == 0 {
                                    log::info!(target: logger_name, "Clearing spark worker queue.");
                                    for (_, jobs) in spark_worker_queue.iter_mut() {
                                        while let Some(job_hash) = jobs.pop_back() {
                                            running_spark_workers.remove(&job_hash);
                                        }
                                    }
                                    spark_worker_queue.clear();
                                    spark_monitor.store(true, std::sync::atomic::Ordering::Release);
                                }
                            }

                            let (tx, categories) = match job_type {
                                JobType::Local => {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let _ = local_job_executor_tx
                                        .send(ManagerMessage::UpdateLocalUsage(
                                            job.get_spec().await.clone(),
                                            tx,
                                        ))
                                        .await;

                                    (&local_job_executor_tx, rx.await.unwrap_or_else(|_| {
                                        log::error!(target: logger_name, "Failed to receive categories from local job executor");
                                        vec![]
                                    }))
                                }
                                JobType::Remote(worker_hash) => {
                                    // Update worker slot as disqualified if job is disqualified
                                    if job.status != Status::DisQualified {
                                        // Reset retries if job is completed without disqualification
                                        if let Some(errors) =
                                            category_retries.get_mut(&get_fail_category(&job).await)
                                        {
                                            let _ = delete_file(&format!(
                                                "{}/{}.log",
                                                &CONFIG.get().unwrap().workarea,
                                                category
                                            ))
                                            .await;
                                            *errors = 0;
                                        }
                                        let _ = worker_manager_tx
                                            .send(ManagerMessage::UpdateWorkerStatus(
                                                worker_hash,
                                                Status::Idle,
                                                job_hash,
                                                category,
                                                None,
                                            ))
                                            .await;
                                    } else {
                                        let _ = worker_manager_tx
                                            .send(ManagerMessage::UpdateWorkerStatus(
                                                worker_hash,
                                                Status::DisQualified,
                                                job_hash,
                                                category,
                                                None,
                                            ))
                                            .await;
                                    }
                                    (&worker_manager_tx, vec![])
                                }
                            };
                            if categories.is_empty() {
                                schedule_job(
                                    &mut queue,
                                    &mut spark_worker_queue,
                                    category,
                                    &mut write_txn,
                                    logger_name,
                                    tx,
                                    &local_job_executor_tx,
                                    &protocols,
                                    &spark_monitor,
                                    &config,
                                    &mut running_spark_workers,
                                    &mut running_spark_apps,
                                    &mut running_queue,
                                    &category_retries,
                                )
                                .await;
                            } else {
                                for category in categories {
                                    schedule_job(
                                        &mut queue,
                                        &mut spark_worker_queue,
                                        category,
                                        &mut write_txn,
                                        logger_name,
                                        tx,
                                        &local_job_executor_tx,
                                        &protocols,
                                        &spark_monitor,
                                        &config,
                                        &mut running_spark_workers,
                                        &mut running_spark_apps,
                                        &mut running_queue,
                                        &category_retries,
                                    )
                                    .await;
                                }
                            }
                            write_txn =
                                commit_to_job_db(&job, &command, &cache_dir, write_txn, &db);
                        }
                    }
                }
            }
            ManagerMessage::FailJobs(category, message) => {
                category_retries.insert(category, get_max_retry_count().await);
                write_txn = fail_jobs_of_category(
                    category,
                    logger_name,
                    &mut queue,
                    &mut running_queue,
                    message,
                    write_txn,
                    &db,
                )
                .await;
                log::info!(target: logger_name, "Failed jobs of category {}", category);
            }
            ManagerMessage::UpdateJobIdentifier(job_hash, identifier) => {
                if let Some((mut job, command, cache_dir)) =
                    get_job_from_db(job_hash, &mut write_txn)
                {
                    if let crate::dto::Identifier::Pid(_) = identifier {
                        // Submit a new job if identifier is a pid
                        schedule_job(
                            &mut queue,
                            &mut spark_worker_queue,
                            job.get_category(),
                            &mut write_txn,
                            logger_name,
                            &local_job_executor_tx,
                            &local_job_executor_tx,
                            &protocols,
                            &spark_monitor,
                            &config,
                            &mut running_spark_workers,
                            &mut running_spark_apps,
                            &mut running_queue,
                            &category_retries,
                        )
                        .await;
                    }
                    job.identifier = Some(identifier);
                    job.status = Status::Running;
                    write_txn = commit_to_job_db(&job, &command, &cache_dir, write_txn, &db);
                    // For cases where job just got scheduled when it message was sent as killed.
                    // If job is marked as killed and job is not killed yet, then kill the job.
                    if [Status::Killed, Status::Killing].contains(&job.status) {
                        // Marking job as killing so that message is sent to flow engine after successful kill.
                        job.status = Status::Killing;
                        let _ = job.kill(logger_name).await;
                    }
                }
            }
            ManagerMessage::GetJobField(job_hash, field, tx) => {
                let mut value = None;
                if let Some((job, _, _)) = get_job_from_db(job_hash, &mut write_txn) {
                    value = Some(job.return_field(&field).await)
                }
                tx.send(value).unwrap_or_else(|_| {
                    log::error!(target: logger_name, "Failed to send job identifier for job {}", job_hash);
                });
            }

            ManagerMessage::KillJob(job_hash, _, tx) => {
                if let Some((mut job, command, cache_dir)) =
                    get_job_from_db(job_hash, &mut write_txn)
                {
                    if [Status::Success, Status::Failed, Status::Killed].contains(&job.status) {
                        log::warn!(target: logger_name, "Job {} is already completed.", job.job_name);
                        // tx is expected to be passed always. Hence directly unwrapping it here.
                        tx.unwrap().send(Err(()))
                            .unwrap_or_else(|_| {
                                log::error!(target: logger_name, "Failed to send kill confirmation for job {}", job_hash);
                            });
                        continue;
                    }
                    job.status = Status::Killing;
                    if let Err(_) = job.kill(logger_name).await {
                        // Drop job from queue if job does not have any identifier set
                        if let Ok(_) =
                            drop_job_from_queue(&mut queue, job_hash, job.get_category()).await
                        {
                            let guard = config.read().await;
                            if let Some(flow_engine_url) = guard.flow_engine_url.as_ref() {
                                // Update the flow engine to reflect the kill
                                job.status = Status::Killed;
                                job.update_flow_engine(
                                    flow_engine_url,
                                    logger_name,
                                    Some("Job killed Successfully".to_string()),
                                )
                                .await;
                            }
                            drop(guard);
                        }
                    }
                    tx.unwrap().send(Ok(()))
                    .unwrap_or_else(|_| {
                            log::error!(target: logger_name, "Failed to send kill confirmation for job {}", job_hash);
                        });
                    write_txn = commit_to_job_db(&job, &command, &cache_dir, write_txn, &db);
                } else {
                    tx.unwrap().send(Err(()))
                            .unwrap_or_else(|_| {
                                log::error!(target: logger_name, "Failed to send kill confirmation for job {}", job_hash);
                            });
                }
            }
            ManagerMessage::IncrementRetry(category, message) => {
                (write_txn, _) = increment_retries(
                    category,
                    message,
                    logger_name,
                    &mut queue,
                    &mut running_queue,
                    write_txn,
                    &db,
                    &mut category_retries,
                )
                .await;
            }
            ManagerMessage::ResetRetries => {
                for category in category_retries.keys() {
                    let _ = delete_file(&format!(
                        "{}/{}.log",
                        &CONFIG.get().unwrap().workarea,
                        category
                    ))
                    .await;
                }
                category_retries.clear();
            }
            _ => {}
        }
    }
}
