use std::sync::Arc;

use crate::common_utils::append_data_to_file;
use crate::dto::{Job, JobField, ManagerMessage, SparkJob};
use crate::utils::get_hash;
use crate::CONFIG;

pub async fn scheduler() {
    let job_queue = Arc::clone(&CONFIG.get().unwrap().spark_job_handler.jobs);
    let notifier = Arc::clone(&CONFIG.get().unwrap().spark_job_handler.notifier);
    let scheduler_tx = &CONFIG.get().unwrap().job_scheduler_tx;
    loop {
        notifier.notified().await;
        let mut guard = job_queue.write().await;
        while let Some(job) = guard.pop_front() {
            job.schedule(scheduler_tx).await;
        }
        drop(guard);
    }
}

impl SparkJob {
    pub async fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Job name cannot be empty".to_string());
        }
        if self.app_cmd.is_empty() {
            return Err("App command cannot be empty".to_string());
        }
        if !std::path::Path::new(&self.workarea).exists() {
            tokio::fs::create_dir_all(&self.workarea)
                .await
                .map_err(|e| format!("Failed to create workarea: {}", e))?;
        }
        self.spec
            .validate()
            .await
            .map_err(|e| format!("Spec validation failed: {}", e))?;
        self.executor_spec
            .validate()
            .await
            .map_err(|e| format!("Executor spec validation failed: {}", e))?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        CONFIG
            .get()
            .unwrap()
            .job_scheduler_tx
            .send(ManagerMessage::GetJobField(
                get_hash(&self.name),
                JobField::JobName,
                tx,
            ))
            .await
            .unwrap();
        if let Some(_) = rx.await.unwrap_or(None) {
            return Err("Job with the same name already exists".to_string());
        }
        Ok(())
    }
    async fn update_conf(&mut self) {
        self.conf["spark.dynamicAllocation.shuffleTracking.enabled"] = serde_json::json!("true");
        self.conf["spark.dynamicAllocation.enabled"] = serde_json::json!("true");
        self.conf["spark.executor.memory"] =
            serde_json::json!(format!("{}g", self.executor_spec.memory));
        self.conf["spark.executor.cores"] =
            serde_json::json!(&self.executor_spec.cores.to_string());
    }
    async fn save_conf(&mut self) -> Result<String, ()> {
        let conf_path = format!("{}/.conf/spark_props.conf", self.workarea);
        let conf_dir = std::path::Path::new(&conf_path).parent().unwrap();
        if !conf_dir.exists() {
            match tokio::fs::create_dir_all(conf_dir).await {
                Ok(_) => {}
                Err(_) => {
                    return Err(());
                }
            }
        }
        let mut conf_str = String::new();
        for (key, value) in self.conf.as_object().unwrap() {
            conf_str.push_str(&format!("{}={}\n", key, value.as_str().unwrap_or("")));
        }
        match tokio::fs::write(&conf_path, conf_str).await {
            Ok(_) => Ok(conf_path),
            Err(_) => Err(()),
        }
    }
    async fn get_cmd(&mut self, conf_path: String) -> Result<String, ()> {
        let mut cmd = String::new();
        cmd.push_str(&format!(
            "spark-submit --master {} ",
            &CONFIG.get().unwrap().spark_master_url
        ));
        cmd.push_str("--deploy-mode client ");
        cmd.push_str(&format!("--properties-file {} ", conf_path));
        cmd.push_str(&format!("--name {} ", self.name));
        cmd.push_str(&format!("--driver-memory {}g ", self.spec.memory));
        cmd.push_str(&format!("--driver-cores {} ", self.spec.cores));
        cmd.push_str(&self.app_cmd);
        Ok(cmd)
    }
    pub async fn schedule(mut self, scheduler_tx: &tokio::sync::mpsc::Sender<ManagerMessage>) {
        // TODO: Add error handling when failing to create and save conf
        self.update_conf().await;
        let conf_path = match self.save_conf().await {
            Ok(path) => path,
            Err(_) => "".to_string(),
        };
        let cmd = match self.get_cmd(conf_path).await {
            Ok(cmd) => cmd,
            Err(_) => "".to_string(),
        };
        let job = Job::new(
            self.name,
            "ccm-spark-driver".to_string(),
            self.protocol,
            vec![self.executor_spec],
            true,
            Some(self.spec),
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        scheduler_tx
            .send(ManagerMessage::AddJob(
                job,
                cmd,
                true,
                Some(self.workarea.clone()),
                Some(tx),
            ))
            .await
            .unwrap();
        match rx.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                let file_path = format!("{}/cluster_manager.log", self.workarea);
                let _ = append_data_to_file(&file_path, &e).await;
            }
            // Ignorning this case since this will happen when tx is dropped
            Err(_) => {}
        }
    }
}
