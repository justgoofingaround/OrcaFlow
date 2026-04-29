use log::{error, warn};
use redb::{Database, WriteTransaction};
use std::collections::{HashMap, VecDeque};
use sysinfo::{Pid, System};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::dto::{Job, JobType, ManagerMessage, RunType, Spec, Status, Worker, FarmConfig, DistributedConfig};
use crate::CONFIG;

pub async fn compute_resource_usage(
    pids: &mut VecDeque<u32>,
    max_memory: i64,
    max_cpus: i64,
    logger_name: &str,
    current_spec: &Spec,
) -> Spec {
    // Function to compute resource usage of pids
    let mut cpu_memory = current_spec.memory;
    let mut cpus_used = current_spec.cores;
    if !pids.is_empty() {
        let mut memory = cpu_memory as f64;
        let mut cpus = cpus_used as f32;
        let mut system = System::new();
        for _ in 0..2 {
            system
                .refresh_processes_specifics(sysinfo::ProcessRefreshKind::everything().with_cpu());
        }
        for index in 0..pids.len() {
            match pids.get(index) {
                Some(pid) => {
                    let pid = Pid::from_u32(pid.to_owned());
                    match system.process(pid) {
                        Some(process) => {
                            // calculating total cpu and memory used by all the pids
                            memory += (process.memory() as f64) * 2e-9;
                            cpus += process.cpu_usage() / (100 as f32);
                        }
                        None => {
                            // if pid is not running removing pid from queue
                            warn!(target: logger_name, "Worker with pid: {} is not running", pid);
                            pids.remove(index);
                        }
                    }
                }
                None => {
                    warn!(target: logger_name, "Skipping {} from pids queue since it does not exist.", index)
                }
            }
        }
        cpu_memory = memory as i64;
        cpus_used = cpus as i64;
    }
    Spec {
        memory: (max_memory - cpu_memory) as i64,
        cores: (max_cpus - cpus_used) as i64,
    }
}

#[allow(unused_assignments)]
pub async fn spawn_workers(
    worker_hash: u64,
    name: String,
    worker_command: String,
    logger_name: &str,
    protocol: String,
    category: u64,
) -> Result<(), ()> {
    // Spawn a child process
    // This will kill the spawned process when the child goes out of scope
    let mut str_output = String::new();
    match Command::new("sh")
        .arg("-c")
        .arg(&worker_command)
        .kill_on_drop(true)
        .output()
        .await
    {
        Ok(output) => {
            str_output = String::from_utf8(output.stdout).unwrap();
            match output.status.code() {
                Some(code) => {
                    if code != 0 {
                        let mut message = str_output.clone();
                        message += &format!("\n{}", String::from_utf8(output.stderr).unwrap());
                        message += &format!(
                            "\nGot non zero exit status {} while executing {}",
                            code, worker_command
                        );
                        // Add job as failed
                        CONFIG
                            .get()
                            .unwrap()
                            .worker_manager_tx
                            .send(ManagerMessage::KillWorker(
                                worker_hash,
                                true,
                                Some(format!(
                                    "Got non zero exit status {} while executing {}",
                                    code, worker_command
                                )),
                                true,
                            ))
                            .await
                            .unwrap();
                        CONFIG
                            .get()
                            .unwrap()
                            .job_scheduler_tx
                            .send(ManagerMessage::FailJobs(category, Some(message)))
                            .await
                            .unwrap();
                        return Err(());
                    }
                }
                None => {
                    let mut message = str_output.clone();
                    message += &format!("\n{}", String::from_utf8(output.stderr).unwrap());
                    message += &format!("\nFailed to execute {}", worker_command);
                    CONFIG
                        .get()
                        .unwrap()
                        .worker_manager_tx
                        .send(ManagerMessage::KillWorker(
                            worker_hash,
                            true,
                            Some(format!("Failed to execute {}", worker_command)),
                            true,
                        ))
                        .await
                        .unwrap();
                    CONFIG
                        .get()
                        .unwrap()
                        .job_scheduler_tx
                        .send(ManagerMessage::FailJobs(category, Some(message)))
                        .await
                        .unwrap();
                    return Err(());
                }
            }
        }
        Err(error) => {
            let message = format!("Failed to execute command. Reason - {}", error);
            CONFIG
                .get()
                .unwrap()
                .worker_manager_tx
                .send(ManagerMessage::KillWorker(
                    worker_hash,
                    true,
                    Some(format!(
                        "Failed to execute {}. Reason - {}",
                        worker_command, error
                    )),
                    true,
                ))
                .await
                .unwrap();
            CONFIG
                .get()
                .unwrap()
                .job_scheduler_tx
                .send(ManagerMessage::FailJobs(category, Some(message)))
                .await
                .unwrap();

            return Err(());
        }
    };
    let job_id = super::get_job_id(&str_output, &logger_name).await;
    let worker = Worker {
        name: name,
        identifier: None,
        job_id: job_id.clone(),
        protocol: protocol,
        categories: vec![category],
    };
    // Update job id of worker
    CONFIG
        .get()
        .unwrap()
        .worker_manager_tx
        .send(ManagerMessage::UpdateWorker(worker_hash, job_id, None))
        .await
        .unwrap();
    // Checking status of job id
    // Updating job at index 0 since it is the required job. Job at index 0 is either a spark worker or a dummy job.
    worker.check_job_status(logger_name, worker_hash).await;
    Ok(())
}

pub async fn update_job_status(
    job_hash: u64,
    status: Status,
    update_flow_engine: bool,
    message: Option<String>,
    job_type: JobType,
) {
    let _ = CONFIG
        .get()
        .unwrap()
        .job_scheduler_tx
        .send(ManagerMessage::UpdateJobStatus(
            job_hash,
            status,
            update_flow_engine,
            message,
            job_type,
        ))
        .await;
}

pub async fn create_worker_command(
    command: &str,
    job_name: &str,
    logger_name: &str,
) -> Option<String> {
    // Create a shell file to execute command
    let path = format!(
        "{}/worker_commands/{}.sh",
        crate::CONFIG.get().unwrap().workarea,
        job_name
    );
    let shell_path = std::path::PathBuf::from(path);
    if !shell_path.parent().unwrap().is_dir() {
        match tokio::fs::create_dir_all(shell_path.parent().unwrap()).await {
            Ok(_) => {}
            Err(e) => {
                error!(target: logger_name, "Failed to create directory. Reason - {}", e);
                return None;
            }
        }
    }
    if !shell_path.is_file() {
        match tokio::fs::File::create(&shell_path).await {
            Ok(_) => {}
            Err(e) => {
                error!(target: logger_name, "Failed to create shell file. Reason - {}", e);
                return None;
            }
        };
    }
    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .open(&shell_path)
        .await
    {
        Ok(file) => file,
        Err(e) => {
            error!("Failed to open shell file. Reason - {}", e);
            return None;
        }
    };
    let result = match tokio::io::AsyncWriteExt::write_all(&mut file, command.as_bytes()).await {
        Ok(_) => Some(shell_path.to_str().unwrap().to_string()),
        Err(e) => {
            error!(target: logger_name, "Failed to write to shell file. Reason - {}", e);
            None
        }
    };
    let _ = file.flush().await;
    result
}

pub async fn submit_job_to_idle_worker(
    idle_workers_map: &mut HashMap<u64, VecDeque<u64>>,
    job_category: u64,
    write_txn: &mut WriteTransaction,
    job: &Job,
    logger_name: &str,
    running_worker_job_mapper: &mut HashMap<String, HashMap<u64, Vec<u64>>>,
    envs: Option<HashMap<String, String>>,
    job_hash: u64,
    command: &str,
    cache_dir: &Option<String>,
) -> Result<u64, ()> {
    if let Some(idle_workers) = idle_workers_map.get_mut(&job_category) {
        if let Some(idle_worker) = idle_workers.pop_front() {
            if let Some(worker) = read_from_worker_db(idle_worker, write_txn) {
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(10),
                    worker.submit_job(&job, logger_name, envs, command, cache_dir),
                )
                .await
                {
                    Ok(response) => match response {
                        Ok(_) => {
                            let worker_hash = worker.get_hash();
                            update_running_worker_job_mapper(
                                running_worker_job_mapper,
                                &worker,
                                job_hash,
                                worker_hash,
                            );
                            return Ok(worker_hash);
                        }
                        Err(_) => {
                            error!(target: logger_name, "Failed to submit {} to worker {}", job.job_name, worker.name);
                        }
                    },
                    Err(_) => {
                        error!(target: logger_name, "Failed to submit job to worker. Reason - Timeout");
                    }
                }
            }
        }
    }
    Err(())
}

pub fn update_running_worker_job_mapper(
    running_worker_job_mapper: &mut HashMap<String, HashMap<u64, Vec<u64>>>,
    worker: &Worker,
    job_hash: u64,
    worker_hash: u64,
) {
    // Updates the running worker job mapper with the given worker's information.
    // # Arguments
    // * `running_worker_job_mapper` - A mutable reference to the running worker job mapper.
    // * `worker` - The worker whose information needs to be updated in the mapper.
    match running_worker_job_mapper.get_mut(&worker.protocol) {
        Some(worker_map) => match worker_map.get_mut(&worker_hash) {
            Some(jobs) => {
                jobs.push(job_hash);
            }
            None => {
                worker_map.insert(worker_hash, vec![job_hash]);
            }
        },
        None => {
            let mapper = HashMap::from([(worker_hash, vec![job_hash])]);
            running_worker_job_mapper.insert(worker.protocol.clone(), mapper);
        }
    }
}

pub async fn kill_worker(
    worker_hash: u64,
    running_worker_job_mapper: &mut HashMap<String, HashMap<u64, Vec<u64>>>,
    worker: &mut Worker,
    scheduler_tx: &tokio::sync::mpsc::Sender<crate::dto::ManagerMessage>,
    logger_name: &str,
    update_flow_engine: bool,
    message: Option<String>,
    update_job_status: bool,
    queued_workers_in_scheduler: &mut HashMap<String, VecDeque<u64>>,
) {
    // This code block is responsible for killing  worker in the worker manager.
    // It first checks if there are any workers associated with the given protocol.
    // If there are, it removes the specified worker from the list of workers.
    // Then, for each job associated with the removed worker, it checks if the job is present in the workers map.
    // If it is, it marks the worker's status as "Killed" and performs additional actions.
    // If there are no queued workers of the same category as the killed worker, it sends a message to the job scheduler to run the queued category.
    // If specified, it updates the job status to "Failed" and performs other necessary operations.
    // Finally, if the removed worker is the first job in the list, it spawns a new asynchronous task to kill the worker.
    if let Some(workers) = running_worker_job_mapper.get_mut(&worker.protocol) {
        if let Some(jobs) = workers.remove(&worker_hash) {
            for job in &jobs {
                if update_job_status {
                    super::update_job_status(
                        job.to_owned(),
                        Status::Failed,
                        update_flow_engine,
                        message.clone(),
                        JobType::Remote(worker_hash),
                    )
                    .await;
                }
            }
        }
    }
    for category in &worker.categories {
        update_queued_workers_and_schedule_job(
            category.to_owned(),
            queued_workers_in_scheduler,
            &worker.protocol,
            &scheduler_tx,
            logger_name,
        )
        .await;
    }
    worker
        .kill_worker(logger_name.to_string(), None, false)
        .await;
}

pub async fn add_to_idle_workers_map(
    idle_workers_map: &mut HashMap<u64, VecDeque<u64>>,
    worker_hash: u64,
    category: &u64,
) {
    if let Some(idle_workers) = idle_workers_map.get_mut(category) {
        idle_workers.push_back(worker_hash);
    } else {
        idle_workers_map.insert(category.clone(), VecDeque::from([worker_hash]));
    }
}

pub fn worker_serialize(worker: &Worker) -> Vec<u8> {
    // Serialize worker to bytes
    bincode::serialize(worker).unwrap()
}

pub fn worker_deserialize(data: &[u8]) -> Worker {
    // Deserialize worker from bytes
    bincode::deserialize(data).unwrap()
}

pub async fn update_queued_workers_and_schedule_job(
    queued_category: u64,
    queued_workers_in_scheduler: &mut HashMap<String, VecDeque<u64>>,
    protocol: &str,
    scheduler_tx: &tokio::sync::mpsc::Sender<crate::dto::ManagerMessage>,
    logger_name: &str,
) {
    let mut categories = vec![queued_category];
    // Get the queued jobs in scheduler
    if let Some(workers) = queued_workers_in_scheduler.get_mut(protocol) {
        if let Some(category) = workers.pop_front() {
            if category != queued_category {
                categories.push(category);
            }
        }
    }
    for category in categories {
        scheduler_tx
            .send(ManagerMessage::RunQueuedCategory(RunType::Remote(category)))
            .await
            .unwrap();
        log::info!(target: logger_name, "Running queued category: {}", category);
    }
}

pub fn write_to_worker_db(
    worker_hash: u64,
    worker: &Worker,
    mut write_txn: WriteTransaction,
    db: &Database,
) -> WriteTransaction {
    let _ = super::write_to_db(worker_hash, worker_serialize(worker), &mut write_txn);
    let _ = write_txn.commit();
    super::get_write_transaction(db)
}

pub fn read_from_worker_db(
    worker_hash: u64,
    write_txn: &mut WriteTransaction,
) -> Option<Worker> {
    if let Some(worker) = super::read_from_db(worker_hash, write_txn) {
        return Some(worker_deserialize(&worker));
    }
    None
}

pub fn remove_from_worker_db(
    worker_hash: u64,
    write_txn: &mut WriteTransaction,
) -> Option<Worker> {
    if let Ok(Some(worker)) = super::remove_from_db(worker_hash, write_txn) {
        Some(worker_deserialize(&worker))
    } else {
        None
    }
}

pub fn pop_from_worker_db(write_txn: &mut WriteTransaction) -> Option<Worker> {
    if let Ok(Some(worker)) = super::pop_from_db(write_txn) {
        Some(worker_deserialize(&worker))
    } else {
        None
    }
}

pub fn get_farm_config(config: &FarmConfig) -> Option<&DistributedConfig> {
    match config {
        FarmConfig::Farm(config) => Some(&config),
        FarmConfig::Local(_) => None,
    }
}
