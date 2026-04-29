use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;
use redb::{ReadableTable, ReadableTableMetadata};
use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};

use crate::dto::{Config, Spec};

pub const TABLE_DEF: redb::TableDefinition<u64, Vec<u8>> = redb::TableDefinition::new("jobs");


fn create_logging(logger_name: &str) -> RollingFileAppender {
    // Setup Rotating logging of logger_name
    let log_line_pattern = "{d(%Y-%m-%d %H:%M:%S)} | {({l}):5.5} | {f}:{L} — {m}{n}";
    let trigger_size = 30000000;
    let trigger = Box::new(SizeTrigger::new(trigger_size));
    let roller_pattern = &format!("logs/{logger_name}/{logger_name}_{}.gz", "{}");
    let log_path = &format!("logs/{logger_name}/{logger_name}.log");
    let roller_count = 5;
    let roller_base = 1;
    let roller = Box::new(
        FixedWindowRoller::builder()
            .base(roller_base)
            .build(roller_pattern, roller_count)
            .unwrap(),
    );

    RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(log_line_pattern)))
        .build(log_path, Box::new(CompoundPolicy::new(trigger, roller)))
        .unwrap()
}

pub fn setup_logging() {
    // Setup server, worker_manager and local_job_executor logging
    let worker_manager_logger = create_logging("worker_manager");
    let local_job_executor_logger = create_logging("local_job_executor");
    let server_logger = create_logging("server");
    let spark_monitor = create_logging("spark_monitor");
    let job_scheduler = create_logging("job_scheduler");

    let stdout = ConsoleAppender::builder().build();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("worker_manager", Box::new(worker_manager_logger)))
        .appender(
            Appender::builder().build("local_job_executor", Box::new(local_job_executor_logger)),
        )
        .appender(Appender::builder().build("server", Box::new(server_logger)))
        .appender(Appender::builder().build("spark_monitor", Box::new(spark_monitor)))
        .appender(Appender::builder().build("job_scheduler", Box::new(job_scheduler)))
        .logger(
            Logger::builder()
                .appender("worker_manager")
                .build("worker_manager", LevelFilter::Info),
        )
        .logger(
            Logger::builder()
                .appender("local_job_executor")
                .build("local_job_executor", LevelFilter::Info),
        )
        .logger(
            Logger::builder()
                .appender("server")
                .build("server", LevelFilter::Info),
        )
        .logger(
            Logger::builder()
                .appender("spark_monitor")
                .build("spark_monitor", LevelFilter::Info),
        )
        .logger(
            Logger::builder()
                .appender("job_scheduler")
                .build("job_scheduler", LevelFilter::Info),
        )
        .build(Root::builder().appender("stdout").build(LevelFilter::Error))
        .unwrap();

    // You can use handle to change logger config at runtime
    let _ = log4rs::init_config(config).unwrap();
}

fn get_message(message: Option<&serde_json::Value>) -> String {
    match message {
        Some(message) => message.to_string(),
        None => String::from("Failed to send request"),
    }
}

async fn parse_response(
    api_call: Result<reqwest::Response, reqwest::Error>,
) -> Result<serde_json::Value, String> {
    match api_call {
        Ok(response) => {
            let status = response.status();
            let result: serde_json::Value = match response.json().await {
                Ok(result) => result,
                Err(e) => {
                    let message = format!("Failed to parse response: {}", e);
                    return Err(message);
                }
            };
            match status {
                reqwest::StatusCode::OK => return Ok(result),
                _ => {
                    return Err(get_message(result.get("message")));
                }
            };
        }
        Err(e) => {
            return Err(e.to_string());
        }
    };
}

pub async fn submit_get_request(endpoint: &str) -> Result<serde_json::Value, String> {
    let client = reqwest::Client::new();
    parse_response(
        client
            .get(endpoint)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await,
    )
    .await
}

pub async fn get_cluster_home() -> String {
    match std::env::var("CLUSTER_HOME") {
        Ok(cluster_home) => return cluster_home,
        Err(_) => {
            let dash_home = std::path::PathBuf::from(std::env::var("DASH_HOME").unwrap());
            let dash_home =
                dash_home.join("visualization/installer/analytics_server/orchestrator");
            return dash_home.to_str().unwrap().to_string();
        }
    }
}

pub async fn signal_handler() {
    let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
    let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = interrupt_signal.recv() => {}
        _ = terminate_signal.recv() => {}
    };
    let logger = "worker_manager";
    let ppid = std::process::id();
    let local_kill_task = tokio::task::spawn(async move {
        crate::common_utils::kill_pid(ppid, logger, true).await;
    });
    let (tx, rx) = tokio::sync::oneshot::channel();
    let _ = crate::CONFIG
        .get()
        .unwrap()
        .worker_manager_tx
        .send(crate::dto::ManagerMessage::Shutdown(tx))
        .await;
    let _ = rx.await;
    let _ = local_kill_task.await;
    let _ = tokio::fs::remove_dir_all(
        super::get_or_create_db_dir(&crate::CONFIG.get().unwrap().workarea).await,
    )
    .await;
    std::process::exit(0);
}

impl Spec {
    pub async fn new() -> Spec {
        // Create a new Spec
        Spec {
            cores: 0,
            memory: 0,
        }
    }
    pub async fn add(&mut self, spec: &Spec) {
        // Add the cores and memory of the spec to the current spec
        self.cores += spec.cores;
        self.memory += spec.memory;
    }
    pub fn remove(&mut self, spec: &Spec) {
        // Subtract the cores and memory of the spec from the current spec
        self.cores -= spec.cores;
        self.memory -= spec.memory;
    }
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
    pub fn cmp(&self, other: &Spec) -> Ordering {
        if self.cores > other.cores {
            return Ordering::Greater;
        } else if self.memory > other.memory {
            return Ordering::Greater;
        } else if self.cores == other.cores && self.memory == other.memory {
            return Ordering::Equal;
        }
        Ordering::Less
    }
    pub async fn validate(&self) -> Result<(), String> {
        if self.cores <= 0 {
            return Err("Cores cannot be less than or equal to 0".to_string());
        }
        if self.memory <= 0 {
            return Err("Memory cannot be less than or equal to 0".to_string());
        }
        Ok(())
    }
}

impl Config {
    pub async fn new() -> Config {
        // Create a new Config
        Config {
            default_protocol: None,
            flow_engine_url: None,
            parent_pids: None,
        }
    }
    pub async fn validate(&self) -> Result<(), String> {
        let guard = crate::CONFIG.get().unwrap().protocols_map.read().await;
        if let Some(default_protocol) = self.default_protocol.as_ref() {
        if let None = guard.get(default_protocol) {
            drop(guard);
            return Err(format!(
                "Default protocol {} not found in the farm protocols.",
                default_protocol
            ));
        }};
        drop(guard);
        Ok(())
    }
    pub async fn update_config(self, config: &Arc<tokio::sync::RwLock<Config>>) {
        // Update the config
        let mut guard = config.write().await;
        if let Some(default_protocol) = self.default_protocol {
            guard.default_protocol = Some(default_protocol);
        }
        if let Some(parent_pids) = self.parent_pids {
            guard.parent_pids = Some(parent_pids);
        }
        if let Some(flow_engine_url) = self.flow_engine_url {
            guard.flow_engine_url = Some(flow_engine_url);
        }
        drop(guard);
    }
}

pub fn get_hash(name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

pub fn get_db(table_path: &str) -> Result<redb::Database, redb::DatabaseError> {
    // Create a new Database
    redb::Database::builder()
        .set_cache_size(0)
        .create(table_path)
}

impl Spec {
    pub fn get_hash(&self) -> u64 {
        // Get the hash of the spec
        let mut hasher = DefaultHasher::new();
        self.cores.hash(&mut hasher);
        self.memory.hash(&mut hasher);
        hasher.finish()
    }
}

pub async fn get_or_create_db_dir(workarea: &str) -> String {
    // Create the directory if it does not exist
    let db_dir = format!("{}/db", workarea);
    if !std::path::Path::new(&db_dir).exists() {
        match tokio::fs::create_dir_all(&db_dir).await {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to create directory: {}", e);
            }
        }
    }
    db_dir
}

pub fn get_write_transaction(db: &redb::Database) -> redb::WriteTransaction {
    db.begin_write().unwrap()
}

pub fn read_from_db(key: u64, write_txn: &redb::WriteTransaction) -> Option<Vec<u8>> {
    {
        let table = write_txn.open_table(TABLE_DEF).unwrap();
        let result = table.get(key).unwrap();
        if let Some(value_guard) = result {
            return Some(value_guard.value().clone());
        }
    }
    None
}

pub fn write_to_db(
    key: u64,
    data: Vec<u8>,
    write_txn: &mut redb::WriteTransaction,
) -> Result<(), redb::Error> {
    {
        let mut table = write_txn.open_table(TABLE_DEF)?;
        table.insert(key, &data)?;
    }
    Ok(())
}

pub fn get_len_of_table(write_txn: &mut redb::WriteTransaction) -> u64 {
    let table = write_txn.open_table(TABLE_DEF).unwrap();
    table.len().unwrap_or(0)
}

pub fn remove_from_db(
    key: u64,
    write_txn: &mut redb::WriteTransaction,
) -> Result<Option<Vec<u8>>, redb::StorageError> {
    let mut table = write_txn.open_table(TABLE_DEF).unwrap();
    let response = match table.remove(key) {
        Ok(Some(value)) => Ok(Some(value.value())),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    };
    drop(table);
    response
}

pub fn pop_from_db(
    write_txn: &mut redb::WriteTransaction,
) -> Result<Option<Vec<u8>>, redb::StorageError> {
    let mut table = write_txn.open_table(TABLE_DEF).unwrap();
    let response = match table.pop_last() {
        Ok(Some(value)) => Ok(Some(value.1.value())),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    };
    drop(table);
    response
}
