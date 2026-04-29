use log::{error, info};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

pub mod proto {
    tonic::include_proto!("flow_mgmt");
}
use crate::common_utils::get_flow_and_task_inst;
use crate::dto::{
    Identifier, Job, JobField, JobFieldValue, ManagerMessage, Protocol, Spec, Status,
};
use crate::CONFIG;
use proto::{
    flow_management_client::FlowManagementClient, FlowInst, State, TaskInst, TaskInstUpdate,
};

impl Status {
    pub fn to_flow_engine_state(&self) -> State {
        match self {
            Status::Queued => State::Queued,
            Status::Running => State::Running,
            Status::Failed => State::Failed,
            Status::Killed => State::Killed,
            Status::DisQualified => State::Failed,
            _ => State::Running,
        }
    }
}

impl Job {
    pub fn new(
        job_name: String,
        job_type: String,
        protocol: String,
        specs: Vec<Spec>,
        lightweight: bool,
        lightweight_spec: Option<Spec>,
    ) -> Job {
        Job {
            job_name,
            job_type,
            status: crate::dto::Status::New,
            protocol: protocol,
            specs: specs,
            lightweight: lightweight,
            lightweight_spec: lightweight_spec,
            identifier: None,
        }
    }
    pub async fn create_worker_command(&self, command: &str) -> Option<String> {
        super::create_worker_command(command, &self.job_name, &self.job_type).await
    }
    pub async fn get_command(
        &self,
        worker_name: &str,
        worker_script: &str,
        launcher: &str,
        second_job: &Job,
        worker_hash: u64,
    ) -> Result<String, String> {
        let mut job_types = self.job_type.clone();
        let mut categories = format!("{}", self.get_category());
        let mut job_names = self.job_name.clone();
        let cluster_url = match std::env::var("MASTER_URL") {
            Ok(val) => val,
            Err(_) => {
                return Err("Failed to get cluster url".to_string());
            }
        };
        categories = format!("{} {}", categories, second_job.get_category());
        job_names = format!("{} {}", job_names, second_job.job_name);
        job_types = format!("{} {}", job_types, second_job.job_type);
        let command = format!("{} submit --cores {} --memory {} --command '{} --job-names {} --master-url {} --master-workarea {} --categories {} --protocol {} --job-types {} --lightweight {} {} --name {} --cluster-home {} --worker-hash {} --spark-master-url {} --max-cores {} --max-memory {}'" ,
            launcher,
            self.specs[0].cores,
            self.specs[0].memory,
            worker_script,
            job_names,
            cluster_url,
            CONFIG.get().unwrap().workarea,
            categories,
            self.protocol,
            job_types,
            &self.lightweight,
            &second_job.lightweight,
            worker_name,
            &CONFIG.get().unwrap().cluster_home,
            worker_hash,
            &CONFIG.get().unwrap().spark_master_url,
            &self.specs[0].cores,
            &self.specs[0].memory,);
        return Ok(command);
    }
    pub fn get_lighweight_category(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.protocol.hash(&mut hasher);
        hasher.finish()
    }
    pub fn get_category(&self) -> u64 {
        if self.lightweight {
            self.get_lighweight_category()
        } else {
            let mut hasher = DefaultHasher::new();
            self.protocol.hash(&mut hasher);
            self.specs.hash(&mut hasher);
            hasher.finish()
        }
    }
    pub async fn submit_job_to_worker(
        &self,
        worker_manager_tx: &Sender<ManagerMessage>,
        command: String,
        cache_dir: Option<String>,
    ) {
        // Send message to worker manager to schedule the job
        let _ = worker_manager_tx
            .send(ManagerMessage::RunJob(self.clone(), command, cache_dir))
            .await;
    }
    pub async fn update_flow_engine(
        &self,
        flow_engine_url: &str,
        logger_name: &str,
        message: Option<String>,
    ) {
        if !self.job_type.contains("flow-agent") {
            return;
        }
        let client = FlowManagementClient::connect(flow_engine_url.to_string()).await;
        let (flow_inst, task_inst) = match get_flow_and_task_inst(&self.job_name).await {
            Ok((flow_inst, task_inst)) => (flow_inst, task_inst),
            Err(e) => {
                error!(target: &logger_name,
                    "Failed to get flow and task instance from job name {}. Error: {}",
                    self.job_name,
                    e
                );
                return;
            }
        };
        match client {
            Ok(mut client) => {
                let message = TaskInstUpdate {
                    task_inst: Some(TaskInst {
                        name: task_inst.to_string(),
                        flow_inst: Some(FlowInst {
                            name: flow_inst.to_string(),
                        }),
                    }),
                    downstream_tasks: Vec::new(),
                    state: self.status.to_flow_engine_state().into(),
                    message: message,
                };
                let request = tonic::Request::new(message);
                match client.update_task_inst(request).await {
                    Ok(response) => {
                        info!(target: &logger_name,
                            "Updated status of {} to flow engine with status {:?}. Response: {:?}",
                            self.job_name, self.status, response
                        );
                    }
                    Err(e) => {
                        error!(target: &logger_name,
                            "Failed to update status of {} to flow engine. Error: {}",
                            self.job_name,
                            e
                        );
                    }
                };
            }
            Err(_) => {
                error!(target: &logger_name,
                    "Failed to connect to flow engine. Skipping updation of status of {}",
                    self.job_name
                );
            }
        }
        log::info!(target: logger_name, "exit 10");
    }
    async fn get_second_job(&self) -> Job {
        // This will get a complementary job for the current job
        // For a spark driver type of job  this will return a spark worker job
        // For a lightweight job this will return a dummy non lightweight job which will be set to idle upon start up
        // For a non lightweight job this will return a dummy lightweight job which will be set to idle upon start up
        if self.lightweight {
            //Add a dummy non lightweight job
            let job_name = String::from("dummy_ccm_job");
            return Job::new(
                job_name,
                String::new(),
                self.protocol.to_string(),
                self.specs.clone(),
                false,
                None,
            );
        } else {
            //Add a dummy lightweight job
            let job_name = String::from("dummy_ccm_job");
            return Job::new(
                job_name,
                String::new(),
                self.protocol.to_string(),
                self.specs.clone(),
                true,
                None,
            );
        }
    }
    pub async fn add_job_to_queue(
        self,
        logger_name: &str,
        parent_pids: &mut VecDeque<u32>,
        max_memory: i64,
        max_cpus: i64,
        spec_job_mapper: &mut HashMap<u64, u64>,
        low_priority_mapper: &mut HashMap<u64, u64>,
        specs: &mut Vec<Spec>,
        worker_manager_tx: &Sender<ManagerMessage>,
        current_spec: &mut Spec,
        command: String,
        protocol_map: &Arc<RwLock<HashMap<String, Protocol>>>,
        cache_dir: Option<String>,
    ) {
        // If primary queue is empty then check the following:
        // if protocol is local or protocol is default and secondary queue is empty check the current spec and schedule.
        // if the job's spec does not fit in the current spec and protocol is local, check if job with same spec hash exists in queue and if it doesn't add the category to queue
        // if the job's spec does not fit in the current spec and protocol is default, push the job to farm scheduling
        let job_spec = self.get_spec().await;
        if spec_job_mapper.is_empty()
            && (self.protocol == String::from("local")
                || (low_priority_mapper.is_empty() && self.protocol == String::from("default")))
            && [Ordering::Equal, Ordering::Less].contains(
                &job_spec.cmp(
                    &super::compute_resource_usage(
                        parent_pids,
                        max_memory,
                        max_cpus,
                        logger_name,
                        current_spec,
                    )
                    .await,
                ),
            )
        {
            // Schedule job only if there are no jobs in queue and spec is less than or equal to current spec
            current_spec.add(job_spec).await;
            let guard = CONFIG.get().unwrap().config.read().await;
            let envs = crate::utils::get_envs(
                protocol_map,
                &self.protocol,
                guard.default_protocol.as_ref().unwrap(),
            )
            .await;
            drop(guard);
            super::schedule_sh_job(self, logger_name.to_string(), envs, command, cache_dir).await;
            return;
        }
        // Send job to farm if protocol is not local
        if self.protocol != String::from("local") {
            self.submit_job_to_worker(worker_manager_tx, command, cache_dir)
                .await;
        // Add job to sh queue if protocol is local
        } else {
            let spec_hash = job_spec.get_hash();
            super::update_specs(specs, job_spec).await;
            if self.job_type != "spark_worker" {
                log::info!(target: logger_name, "Job {} with protocol {} added to local queue", self.job_name, self.protocol);
                spec_job_mapper.insert(spec_hash, self.get_category());
            } else {
                log::info!(target: logger_name, "Job {} with protocol {} added to low priority queue", self.job_name, self.protocol);
                low_priority_mapper.insert(spec_hash, self.get_category());
            }
            self.queue_job(command, cache_dir, false).await;
        }
    }

    pub async fn queue_job(self, command: String, cache_dir: Option<String>, can_schedule: bool) {
        CONFIG
            .get()
            .unwrap()
            .job_scheduler_tx
            .send(ManagerMessage::AddJob(
                self,
                command,
                can_schedule,
                cache_dir,
                None,
            ))
            .await
            .unwrap();
    }

    pub fn get_hash(&self) -> u64 {
        super::get_hash(&self.job_name)
    }

    pub async fn get_dummy_jobs(&self) -> Vec<Job> {
        let dummy_job = Job::new(
            String::from("dummy_ccm_job"),
            String::new(),
            self.protocol.clone(),
            self.specs.clone(),
            self.lightweight,
            None,
        );
        vec![dummy_job, self.get_second_job().await]
    }
    pub async fn get_spec(&self) -> &Spec {
        self.lightweight_spec
            .as_ref()
            .unwrap_or(self.specs.get(0).unwrap())
    }
    pub async fn kill(&self, logger_name: &str) -> Result<(), ()> {
        if let Some(identifier) = &self.identifier {
            match identifier {
                Identifier::Pid(pid) => {
                    crate::common_utils::kill_pid(*pid, logger_name, false).await;
                }
                Identifier::Worker(worker_hash) => {
                    CONFIG
                        .get()
                        .unwrap()
                        .worker_manager_tx
                        .send(ManagerMessage::KillJob(
                            self.get_hash(),
                            Some(*worker_hash),
                            None,
                        ))
                        .await
                        .unwrap();
                }
            }
            Ok(())
        } else {
            Err(())
        }
    }
    pub async fn return_field(&self, field: &JobField) -> JobFieldValue {
        match field {
            JobField::JobName => JobFieldValue::JobName(self.job_name.clone()),
            JobField::JobType => JobFieldValue::JobType(self.job_type.clone()),
            JobField::Status => JobFieldValue::Status(self.status.clone()),
            JobField::Protocol => JobFieldValue::Protocol(self.protocol.clone()),
            JobField::Specs => JobFieldValue::Specs(self.specs.clone()),
            JobField::Lightweight => JobFieldValue::Lightweight(self.lightweight),
            JobField::LightweightSpec => {
                JobFieldValue::LightweightSpec(self.lightweight_spec.clone())
            }
            JobField::Identifier => JobFieldValue::Identifier(self.identifier.clone()),
        }
    }
}
