use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use super::LOCAL_DISK_THRESHOLD;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Protocol {
    pub name: String,
    pub config: FarmConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct DistributedConfig {
    pub max_workers: i8,
    pub queued_time: i64,
    pub local_disk: String,
    pub disk_threshold: u64,
    pub script: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct LocalConfig {
    pub local_disk: String,
    pub disk_threshold: u64,
    pub max_local_memory: i64,
    pub max_local_cores: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum FarmConfig {
    Local(LocalConfig),
    Farm(DistributedConfig),
}

impl FarmConfig {
    async fn update(
        &mut self,
        farm_dir: &str,
        default_protocol: &str,
        ignore_error: bool,
    ) -> Result<(), String> {
        match self {
            FarmConfig::Local(config) => {
                config
                    .update(farm_dir, default_protocol, ignore_error)
                    .await
            }
            FarmConfig::Farm(config) => {
                config
                    .update(farm_dir, default_protocol, ignore_error)
                    .await
            }
        }
    }
    async fn get_envs(&self) -> HashMap<String, String> {
        match self {
            FarmConfig::Local(config) => config.get_envs().await,
            FarmConfig::Farm(config) => config.get_envs().await,
        }
    }
    async fn get_local_dir(&self) -> &str {
        match self {
            FarmConfig::Local(config) => config.get_local_dir().await,
            FarmConfig::Farm(config) => config.get_local_dir().await,
        }
    }
    async fn get_disk_threshold(&self) -> u64 {
        match self {
            FarmConfig::Local(config) => config.get_disk_threshold().await,
            FarmConfig::Farm(config) => config.get_disk_threshold().await,
        }
    }
}

#[allow(dead_code)]
impl Protocol {
    pub async fn new(name: &str, script: &str) -> Protocol {
        if name == "local" {
            Protocol {
                name: name.to_string(),
                config: FarmConfig::Local(LocalConfig {
                    local_disk: "/tmp".to_string(),
                    disk_threshold: LOCAL_DISK_THRESHOLD,
                    max_local_memory: 60,
                    max_local_cores: 6,
                }),
            }
        } else {
            Protocol {
                name: name.to_string(),
                config: FarmConfig::Farm(DistributedConfig {
                    max_workers: 100,
                    queued_time: 600,
                    local_disk: "/tmp".to_string(),
                    disk_threshold: LOCAL_DISK_THRESHOLD,
                    script: script.to_string(),
                }),
            }
        }
    }
    pub async fn update(
        &mut self,
        farm_dir: &str,
        default_protocol: &str,
        ignore_error: bool,
    ) -> Result<(), String> {
        self.config
            .update(farm_dir, default_protocol, ignore_error)
            .await
    }
    pub async fn get_envs(&self) -> HashMap<String, String> {
        self.config.get_envs().await
    }
    pub async fn get_local_dir(&self) -> &str {
        return self.config.get_local_dir().await;
    }
    pub async fn get_disk_threshold(&self) -> u64 {
        return self.config.get_disk_threshold().await;
    }
}

impl DistributedConfig {
    pub async fn update(
        &mut self,
        farm_dir: &str,
        _default_protocol: &str,
        ignore_error: bool,
    ) -> Result<(), String> {
        let script_path = format!("{}/{}", farm_dir, "farm_script");
        let config_path = format!("{}/{}", farm_dir, "config.json");
        let script_pathbuf = PathBuf::from_str(&script_path).unwrap();
        let config_pathbuf = PathBuf::from_str(&config_path).unwrap();
        if !script_pathbuf.exists() {
            return Err(format!("{} not found", script_path));
        } else if !config_pathbuf.exists() {
            return Err(format!("{} not found", config_path));
        };
        self.script = script_path;
        if let Err(err) = self.__update_config(&config_pathbuf, ignore_error).await {
            return Err(err);
        }
        Ok(())
    }
    async fn get_local_dir(&self) -> &str {
        return &self.local_disk;
    }
    async fn get_disk_threshold(&self) -> u64 {
        return self.disk_threshold;
    }
    async fn __update_config(
        &mut self,
        config_path: &PathBuf,
        ignore_error: bool,
    ) -> Result<(), String> {
        match tokio::fs::read_to_string(config_path).await {
            Ok(config) => {
                let config: serde_json::Value = match serde_json::from_str(&config) {
                    Ok(config) => config,
                    Err(err) => {
                        return Err(format!("Failed to parse config file. Reason - {}", err))
                    }
                };
                self.max_workers = config
                    .get("max_workers")
                    .unwrap_or(&serde_json::json!(self.max_workers))
                    .as_i64()
                    .unwrap() as i8;
                self.queued_time = config
                    .get("submit_timeout")
                    .unwrap_or(&serde_json::json!(self.queued_time))
                    .as_i64()
                    .unwrap();
                self.local_disk = config
                    .get("local_disk")
                    .unwrap_or(&serde_json::json!(self.local_disk))
                    .as_str()
                    .unwrap()
                    .to_string();
                self.disk_threshold = config
                    .get("local_disk_threshold")
                    .unwrap_or(&serde_json::json!(self.disk_threshold))
                    .as_u64()
                    .unwrap();
                if !ignore_error {
                    if !PathBuf::from_str(&self.local_disk).unwrap().is_dir() {
                        return Err(format!("{} is not a valid directory", self.local_disk));
                    }
                }
            }
            Err(err) => return Err(format!("Failed to read config file. Reason - {}", err)),
        };
        Ok(())
    }
    async fn get_envs(&self) -> HashMap<String, String> {
        return HashMap::from([
            (String::from("SPARK_LOCAL_DIRS"), self.local_disk.clone()),
            (String::from("SPARK_LOCAL_DISK"), self.local_disk.clone()),
            (
                String::from("DISK_THRESHOLD"),
                self.disk_threshold.to_string(),
            ),
        ]);
    }
}

impl LocalConfig {
    pub async fn update(
        &mut self,
        farm_dir: &str,
        default_protocol: &str,
        ignore_error: bool,
    ) -> Result<(), String> {
        let config_path = format!("{}/{}", farm_dir, "config.json");
        let config_pathbuf = PathBuf::from_str(&config_path).unwrap();
        if !config_pathbuf.exists() {
            return Err(format!("{} not found", config_path));
        };
        if let Err(err) = self
            .__update_config(&config_pathbuf, default_protocol, ignore_error)
            .await
        {
            return Err(err);
        }
        Ok(())
    }
    async fn get_local_dir(&self) -> &str {
        return &self.local_disk;
    }
    async fn get_disk_threshold(&self) -> u64 {
        return self.disk_threshold;
    }
    async fn __update_config(
        &mut self,
        config_path: &PathBuf,
        default_protocol: &str,
        ignore_error: bool,
    ) -> Result<(), String> {
        match tokio::fs::read_to_string(config_path).await {
            Ok(config) => {
                let config: serde_json::Value = match serde_json::from_str(&config) {
                    Ok(config) => config,
                    Err(err) => {
                        return Err(format!("Failed to parse config file. Reason - {}", err))
                    }
                };
                self.max_local_cores = config
                    .get("max_local_cores")
                    .unwrap_or(&serde_json::json!(self.max_local_cores))
                    .as_i64()
                    .unwrap();
                self.max_local_memory = config
                    .get("max_local_memory")
                    .unwrap_or(&serde_json::json!(self.max_local_memory))
                    .as_i64()
                    .unwrap();
                self.local_disk = config
                    .get("local_disk")
                    .unwrap_or(&serde_json::json!(self.local_disk))
                    .as_str()
                    .unwrap()
                    .to_string();
                self.disk_threshold = config
                    .get("local_disk_threshold")
                    .unwrap_or(&serde_json::json!(self.disk_threshold))
                    .as_u64()
                    .unwrap();
                if !ignore_error {
                    if !PathBuf::from_str(&self.local_disk).unwrap().is_dir() {
                        return Err(format!("{} is not a valid directory", self.local_disk));
                    }
                }
                if !ignore_error {
                    return self.validate(default_protocol).await;
                }
                Ok(())
            }
            Err(err) => Err(format!("Failed to read config file. Reason - {}", err)),
        }
    }
    async fn validate(&self, default_protocol: &str) -> Result<(), String> {
        if self.disk_threshold < 1 {
            return Err("Disk threshold cannot be less than 1".to_string());
        }
        if let Err(message) =
            super::check_disk_space(&self.local_disk, self.disk_threshold, "spark", "local").await
        {
            return Err(message);
        }
        let mut system = sysinfo::System::new();
        if let Some(system_cores) = system.physical_core_count() {
            if self.max_local_cores > system_cores as i64 {
                return Err(format!(
                    "Max cores cannot be greater than system cores: {}",
                    system_cores
                ));
            } else if self.max_local_cores == 0 && default_protocol == "local" {
                return Err("Max cores cannot be 0 when default protocol is local".to_string());
            }
        }
        system.refresh_memory();
        let system_memory = (system.total_memory() as f64) * 1e-9;
        if self.max_local_memory > system_memory as i64 {
            return Err(format!(
                "Max memory cannot be greater than system memory: {}",
                system_memory
            ));
        } else if self.max_local_memory == 0 && default_protocol == "local" {
            return Err("Max memory cannot be 0 when default protocol is local".to_string());
        }
        Ok(())
    }
    async fn get_envs(&self) -> HashMap<String, String> {
        return HashMap::from([
            (String::from("SPARK_LOCAL_DIRS"), self.local_disk.clone()),
            (String::from("SPARK_LOCAL_DISK"), self.local_disk.clone()),
            (
                String::from("DISK_THRESHOLD"),
                self.disk_threshold.to_string(),
            ),
        ]);
    }
}
