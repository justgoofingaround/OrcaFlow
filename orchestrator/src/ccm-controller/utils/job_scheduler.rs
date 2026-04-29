use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{atomic, Arc};
use tokio::sync::{mpsc::Sender, RwLock};

use crate::common_utils::{get_spark_worker_job, log_message, read_file};
use crate::dto::{Job, ManagerMessage, Protocol, Spec};
use crate::job_scheduler::MAX_QUEUED_SPARK_JOBS;
use crate::CONFIG;

#[allow(unused_assignments)]
pub async fn get_job_to_schedule(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    spark_worker_queue: &mut HashMap<u64, VecDeque<u64>>,
    category: u64,
    running_spark_apps: &i64,
    write_txn: &mut redb::WriteTransaction,
) -> Option<(Job, String, Option<String>)> {
    // This function is responsible for fetching the required job from queue.
    // It checks if there are any pending jobs in the queue for the given category.
    // If there are, it pops and returns the next job.
    // If there are no pending jobs, it checks if there are any running Spark applications.
    // If there are, it checks if there are any pending jobs in the Spark worker queue for the given category.
    // If there are, it pops and returns the next job.
    // If there are no pending jobs in both queues, it returns None.
    if let Some(jobs) = queue.get_mut(&category) {
        if let Some(job_hash) = jobs.pop_front() {
            return get_job_from_db(job_hash, write_txn);
        } else {
            queue.remove(&category);
        }
    }
    if *running_spark_apps != 0 {
        if let Some(jobs) = spark_worker_queue.get_mut(&category) {
            if let Some(job_hash) = jobs.pop_front() {
                return get_job_from_db(job_hash, write_txn);
            } else {
                spark_worker_queue.remove(&category);
            }
        }
    }
    None
}

pub async fn add_job_to_queue(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    mut job: Job,
    can_schedule: bool,
    spark_worker_queue: &mut HashMap<u64, VecDeque<u64>>,
    local_job_executor_tx: &Sender<ManagerMessage>,
    running_spark_workers: &mut HashSet<u64>,
    running_spark_apps: &mut i64,
    config: &Arc<RwLock<crate::dto::Config>>,
    spark_monitor: &Arc<atomic::AtomicBool>,
    command: String,
    write_txn: &mut redb::WriteTransaction,
    cache_dir: Option<String>,
    running_queue: &mut HashMap<u64, VecDeque<u64>>,
    category_retries: &HashMap<u64, i8>,
) -> Result<(), String> {
    let config_guard = config.read().await;
    let default_protocol = config_guard.default_protocol.as_ref().unwrap();
    if job.protocol == String::from("default") && default_protocol == "local" {
        job.protocol = String::from("local");
    }
    let job_type = job.job_type.clone();
    let job_hash = job.get_hash();
    let category = job.get_category();
    if can_schedule {
        if job.protocol == "local" {
            let (cores, memory) = super::get_local_cores_and_memory().await.unwrap();
            let max_local_spec = Spec {
                cores: cores,
                memory: memory,
            };
            if job.specs[0].cmp(&max_local_spec) == Ordering::Greater {
                drop(config_guard);
                return Err(format!(
                    "Job {} has requested for more resources than the max local spec",
                    job.job_name
                ));
            }
        }
        if let Err(message) =
            check_category_retries(category_retries, get_fail_category(&job).await).await
        {
            drop(config_guard);
            return Err(message);
        }
        if let Some(_) = get_job_from_db(job_hash, write_txn) {
            drop(config_guard);
            return Err(format!("Job {} already exists.", job.job_name));
        }
        job.status = crate::dto::Status::Queued;
        let data = serialize(&job, &command, &cache_dir);
        let _ = super::write_to_db(job_hash, data, write_txn);
    } else {
        // Drop job from running queue if already present
        if let Some(jobs) = running_queue.get_mut(&category) {
            jobs.retain(|&x| x != job_hash);
        }
    }
    drop(config_guard);
    if job.job_type == String::from("spark_worker") && spark_worker_queue.contains_key(&category) {
        if let Some(jobs) = spark_worker_queue.get_mut(&category) {
            if jobs.contains(&job_hash) {
                return Ok(());
            }
            if jobs.len() >= MAX_QUEUED_SPARK_JOBS as usize {
                let _ = super::remove_from_db(job.get_hash(), write_txn);
                spark_monitor.store(false, atomic::Ordering::Release);
                jobs.retain(|&x| x != job_hash);
                running_spark_workers.remove(&job_hash);
                return Ok(());
            }
            jobs.push_back(job_hash);
        }
    } else if job.job_type != String::from("spark_worker") && queue.contains_key(&category) {
        if let Some(jobs) = queue.get_mut(&category) {
            jobs.push_back(job_hash);
        }
    } else {
        if can_schedule {
            let _ = local_job_executor_tx
                .send(ManagerMessage::RunJob(job, command, cache_dir))
                .await;
            if let Some(jobs) = running_queue.get_mut(&category) {
                jobs.push_back(job_hash);
            } else {
                running_queue.insert(category, VecDeque::from(vec![job_hash]));
            }
        } else {
            if job_type == String::from("spark_worker") {
                if *running_spark_apps != 0 {
                    spark_worker_queue.insert(category, VecDeque::from(vec![job_hash]));
                } else {
                    let _ = super::remove_from_db(job.get_hash(), write_txn);
                    running_spark_workers.remove(&job_hash);
                    return Ok(());
                }
            } else {
                queue.insert(category, VecDeque::from(vec![job_hash]));
            }
        }
    }
    if can_schedule {
        if job_type == String::from("spark_worker") {
            running_spark_workers.insert(job_hash);
        } else if job_type.contains("spark-driver") {
            *running_spark_apps += 1;
        }
    }
    Ok(())
}

pub async fn add_spark_worker(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    can_schedule: bool,
    spark_worker_queue: &mut HashMap<u64, VecDeque<u64>>,
    local_job_executor_tx: &Sender<ManagerMessage>,
    running_spark_workers: &mut HashSet<u64>,
    running_spark_apps: &mut i64,
    config: &Arc<RwLock<crate::dto::Config>>,
    spark_monitor: &Arc<atomic::AtomicBool>,
    write_txn: &mut redb::WriteTransaction,
    spec: &Spec,
    job_name: &str,
    running_queue: &mut HashMap<u64, VecDeque<u64>>,
    category_retries: &HashMap<u64, i8>,
) {
    // This code block checks if the number of running Spark applications is greater than the number of running Spark workers.
    // If it is, a new Spark worker job is created with a unique name based on the current timestamp.
    // The function then retrieves the analytics specification and obtains the Spark worker job.
    // Finally, the job is added to the job queue along with other necessary parameters.
    let len_spark_workers = running_spark_workers.len() as i64;
    if *running_spark_apps > len_spark_workers {
        let spark_job_name = format!(
            "spark_worker_from_scheduler_{}_{}",
            chrono::prelude::Utc::now().timestamp(),
            job_name,
        );
        // let mut (spark_worker, command) =
        //     get_spark_worker_job(spark_job_name, spec, worker_script_path).await;
        let (mut spark_worker, command) = get_spark_worker_job(
            spark_job_name,
            &CONFIG.get().unwrap().cluster_home,
            &CONFIG.get().unwrap().spark_master_url,
            &CONFIG.get().unwrap().workarea,
            spec.cores,
            spec.memory,
            "default",
        )
        .await;
        spark_worker["status"] = serde_json::json!(crate::dto::Status::Queued);
        let spark_worker: Job = serde_json::from_value(spark_worker).unwrap();
        let _ = add_job_to_queue(
            queue,
            spark_worker,
            can_schedule,
            spark_worker_queue,
            local_job_executor_tx,
            running_spark_workers,
            running_spark_apps,
            config,
            &spark_monitor,
            command,
            write_txn,
            None,
            running_queue,
            category_retries,
        )
        .await;
    }
}

pub fn serialize(job: &Job, command: &str, cache_dir: &Option<String>) -> Vec<u8> {
    bincode::serialize(&(job, command, cache_dir)).unwrap()
}

pub fn deserialize(data: &Vec<u8>) -> (Job, String, Option<String>) {
    bincode::deserialize(data).unwrap()
}

pub async fn schedule_job(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    spark_worker_queue: &mut HashMap<u64, VecDeque<u64>>,
    category: u64,
    write_txn: &mut redb::WriteTransaction,
    logger_name: &str,
    tx: &Sender<ManagerMessage>,
    local_job_executor_tx: &Sender<ManagerMessage>,
    protocols: &Arc<RwLock<HashMap<String, Protocol>>>,
    spark_monitor: &Arc<atomic::AtomicBool>,
    config: &Arc<RwLock<crate::dto::Config>>,
    running_spark_workers: &mut HashSet<u64>,
    running_spark_apps: &mut i64,
    running_queue: &mut HashMap<u64, VecDeque<u64>>,
    category_retries: &HashMap<u64, i8>,
) {
    if let Some((job, command, cache_dir)) = get_job_to_schedule(
        queue,
        spark_worker_queue,
        category,
        running_spark_apps,
        write_txn,
    )
    .await
    {
        let mut envs = HashMap::new();
        let protocol_guard = protocols.read().await;

        let mut job_protocol = &job.protocol;
        let config_guard = config.read().await;
        if job.protocol == String::from("default") {
            job_protocol = config_guard.default_protocol.as_ref().unwrap();
        }
        if let Some(protocol) = protocol_guard.get(job_protocol.as_str()) {
            envs.extend(protocol.get_envs().await);
        }
        drop(config_guard);
        drop(protocol_guard);
        let is_spark_worker = job.job_type == String::from("spark_worker");
        let is_spark_driver = job.job_type.contains("spark-driver");
        let category = job.get_category();
        let job_hash = job.get_hash();
        let job_name = job.job_name.clone();
        let spec = job.specs.get(0).unwrap().clone();
        if let Some(jobs) = running_queue.get_mut(&category) {
            jobs.push_back(job_hash);
        } else {
            running_queue.insert(category, VecDeque::from(vec![job_hash]));
        }
        let _ = tx
            .send(ManagerMessage::RunJob(job, command, cache_dir))
            .await;
        if is_spark_worker
            && spark_worker_queue
                .get(&category)
                .unwrap_or(&VecDeque::new())
                .is_empty()
        {
            log::info!(target: logger_name, "No more spark workers to run. Setting spark monitor to true");
            spark_monitor.store(true, atomic::Ordering::Release);
        } else if is_spark_driver {
            add_spark_worker(
                queue,
                true,
                spark_worker_queue,
                local_job_executor_tx,
                running_spark_workers,
                running_spark_apps,
                config,
                spark_monitor,
                write_txn,
                &spec,
                &job_name,
                running_queue,
                category_retries,
            )
            .await;
        }
    } else {
        log::info!(target: logger_name, "No jobs to run for category {}", category);
    }
}

pub async fn drop_job_from_queue(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    job_hash: u64,
    category: u64,
) -> Result<(), ()> {
    // This function is responsible for removing a job from the queue.
    // It checks if the job is present in the queue and removes it if found.
    // If the job is not found in the queue, it logs a warning message.
    if let Some(jobs) = queue.get_mut(&category) {
        jobs.retain(|&x| x != job_hash);
        return Ok(());
    }
    log::warn!(target: "job_scheduler", "Job with hash {} not found in queue", job_hash);
    Err(())
}

pub fn get_job_from_db(
    job_hash: u64,
    write_txn: &mut redb::WriteTransaction,
) -> Option<(Job, String, Option<String>)> {
    // This function is responsible for fetching a job from the database.
    // It retrieves the job data using the provided job hash and returns it as an Option.
    if let Some(data) = super::read_from_db(job_hash, write_txn) {
        let (job, command, cache_dir) = deserialize(&data);
        Some((job, command, cache_dir))
    } else {
        None
    }
}

pub fn commit_to_job_db(
    job: &Job,
    command: &str,
    cache_dir: &Option<String>,
    mut write_txn: redb::WriteTransaction,
    db: &redb::Database,
) -> redb::WriteTransaction {
    // This function is responsible for writing a job to the database.
    // It serializes the job data and writes it to the database using the provided job hash.
    let job_hash = job.get_hash();
    let data = serialize(job, command, cache_dir);
    let _ = super::write_to_db(job_hash, data, &mut write_txn);
    let _ = write_txn.commit();
    super::get_write_transaction(db)
}

pub fn write_to_job_db(
    job: &Job,
    command: &str,
    cache_dir: &Option<String>,
    write_txn: &mut redb::WriteTransaction,
) {
    // This function is responsible for writing a job to the database.
    // It serializes the job data and writes it to the database using the provided job hash.
    let job_hash = job.get_hash();
    let data = serialize(job, command, cache_dir);
    let _ = super::write_to_db(job_hash, data, write_txn);
}

pub async fn fail_jobs_of_category(
    category: u64,
    logger_name: &str,
    scheduled_queue: &mut HashMap<u64, VecDeque<u64>>,
    running_queue: &mut HashMap<u64, VecDeque<u64>>,
    mut message: Option<String>,
    mut write_txn: redb::WriteTransaction,
    db: &redb::Database,
) -> redb::WriteTransaction {
    log::warn!(target: logger_name, "Failing all jobs in category {}", category);
    let log_path = format!("{}/{}.log", CONFIG.get().unwrap().workarea, category);
    if let Some(msg) = message.as_mut() {
        msg.push_str("\n");
        log_message(&msg, &log_path).await;
    }
    let guard = CONFIG.get().unwrap().config.read().await;
    let flow_engine_url = guard.flow_engine_url.clone();
    drop(guard);
    let message = match read_file(&log_path).await {
        Ok(file_content) => {
            Some(format!("{}\n{}", "Maximum number of retries have been reached for jobs with this spec and farm. Please check the following errors:", file_content))
        }
        Err(_) => {
            Some(format!("Maximum number of retries have been reached for jobs with this spec and farm. Please check with R&D."))
        }
    };
    // Remove all the queued jobs
    fail_jobs_from_queue(
        scheduled_queue,
        category,
        &message,
        &mut write_txn,
        logger_name,
        false,
        &flow_engine_url,
    )
    .await;
    // Kill all the running jobs
    fail_jobs_from_queue(
        running_queue,
        category,
        &message,
        &mut write_txn,
        logger_name,
        true,
        &flow_engine_url,
    )
    .await;
    let _ = write_txn.commit();
    let write_txn = super::get_write_transaction(db);
    write_txn
}

async fn fail_jobs_from_queue(
    queue: &mut HashMap<u64, VecDeque<u64>>,
    category: u64,
    message: &Option<String>,
    write_txn: &mut redb::WriteTransaction,
    logger_name: &str,
    kill_job: bool,
    flow_engine_url: &Option<String>,
) {
    log::info!(target: logger_name, "Failing jobs in queue {:?} of category {}", queue, category);
    if let Some(jobs) = queue.get_mut(&category) {
        while let Some(job_hash) = jobs.pop_front() {
            if let Some((mut job, command, cache_dir)) = get_job_from_db(job_hash, write_txn) {
                if kill_job {
                    // Not handling result since all the jobs will be in running state for running queue
                    let _ = job.kill(logger_name).await;
                }
                job.status = crate::dto::Status::Failed;
                log_job_status(flow_engine_url, &job, message, logger_name, &cache_dir).await;
                write_to_job_db(&job, &command, &cache_dir, write_txn);
            }
        }
    }
}

pub async fn increment_retries(
    category: u64,
    mut message: Option<String>,
    logger_name: &str,
    queue: &mut HashMap<u64, VecDeque<u64>>,
    running_queue: &mut HashMap<u64, VecDeque<u64>>,
    mut write_txn: redb::WriteTransaction,
    db: &redb::Database,
    category_retries: &mut HashMap<u64, i8>,
) -> (redb::WriteTransaction, Result<(), ()>) {
    let log_path = format!("{}/{}.log", CONFIG.get().unwrap().workarea, category);
    if let Some(msg) = message.as_mut() {
        msg.push_str("\n");
        let _ = log_message(&msg, &log_path).await;
    }
    let retry_count = match category_retries.get_mut(&category) {
        Some(retries) => {
            *retries += 1;
            *retries
        }
        None => {
            category_retries.insert(category, 1);
            1
        }
    };
    if retry_count >= get_max_retry_count().await {
        write_txn = fail_jobs_of_category(
            category,
            logger_name,
            queue,
            running_queue,
            None,
            write_txn,
            &db,
        )
        .await;
        return (write_txn, Err(()));
    }
    (write_txn, Ok(()))
}

pub async fn get_max_retry_count() -> i8 {
    std::env::var("MAX_CCM_RETRIES")
        .unwrap_or_else(|_| "5".to_string())
        .parse()
        .unwrap_or(5)
}

pub async fn get_fail_category(job: &Job) -> u64 {
    if job.job_type == "spark_worker" {
        job.get_lighweight_category()
    } else {
        job.get_category()
    }
}

async fn check_category_retries(
    category_retries: &HashMap<u64, i8>,
    category: u64,
) -> Result<(), String> {
    if *category_retries.get(&category).unwrap_or(&0) >= get_max_retry_count().await {
        let log_path = format!("{}/{}.log", CONFIG.get().unwrap().workarea, category);
        let msg = match read_file(&log_path).await {
            Ok(file_content) => {
                format!("{}\n{}", "Maximum number of retries have been reached for jobs with this spec and protocol. Please check the following errors:", file_content)
            }
            Err(_) => {
                format!("Maximum number of retries have been reached for jobs with this spec and protocol. Please check with R&D.")
            }
        };
        Err(msg)
    } else {
        Ok(())
    }
}

pub async fn log_job_status(
    flow_engine_url: &Option<String>,
    job: &Job,
    message: &Option<String>,
    logger_name: &str,
    cache_dir: &Option<String>,
) {
    if flow_engine_url.is_some() & job.job_type.contains("flow-agent") {
        job.update_flow_engine(
            flow_engine_url.as_ref().unwrap(),
            logger_name,
            message.clone(),
        )
        .await;
    } else {
        // Save error message to log file if the job is not a flow-agent job
        if let Some(log_dir) = cache_dir.as_ref() {
            let log_path = format!("{}/cluster_manager.log", log_dir);
            if let Some(msg) = message {
                let fail_msg = format!(
                    "\n{}\n. Job {} completed with status {}.",
                    msg,
                    job.job_name,
                    job.status.to_string()
                );
                log_message(&fail_msg, &log_path).await;
            }
        }
    }
}
