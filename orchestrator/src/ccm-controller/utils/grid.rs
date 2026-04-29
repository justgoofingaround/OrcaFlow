use log::error;
use regex::Regex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::RwLock;

use crate::dto::{Protocol, Status};

pub async fn get_job_status_from_id(job_id: &str, launcher: &str, logger_name: &str) -> Status {
    let status_command = format!("{} status --job_id {}", launcher, job_id);
    match Command::new("sh")
        .arg("-c")
        .arg(&status_command)
        .output()
        .await
    {
        Ok(output) => {
            let output = String::from_utf8(output.stdout).unwrap();
            let pattern = Regex::new(r"([a-zA-Z]+)\n$").unwrap();
            match pattern.captures(&output) {
                Some(capture) => {
                    let status = capture.get(1).map_or("failed", |m| m.as_str());
                    return Status::from_str(status).await;
                }
                None => {
                    error!(target: logger_name, "Failed to get status from output: {}", output);
                    return Status::Failed;
                }
            }
        }
        Err(e) => {
            error!(target: logger_name, "Failed to execute command. Reason - {}", e);
            return Status::Failed;
        }
    }
}

pub async fn terminate_job(job_id: &str, launcher: &str, logger_name: &str) {
    let kill_command = format!("{} terminate --job_id {}", launcher, job_id);
    match Command::new("sh")
        .arg("-c")
        .arg(&kill_command)
        .status()
        .await
    {
        Ok(status) => {
            if status.code().unwrap_or(1) != 0 {
                error!(target: logger_name, "Failed to terminate job using {}.", kill_command);
            }
        }
        Err(e) => {
            error!(target: logger_name, "Failed to execute command. Reason - {}", e);
        }
    }
}

pub async fn get_job_id(output: &str, logger_name: &str) -> Option<String> {
    let pattern = Regex::new(r"\n(\S+)\n$").unwrap();
    match pattern.captures(output) {
        Some(capture) => Some(capture.get(1).map_or("-1", |m| m.as_str()).to_string()),
        None => {
            error!(target: logger_name, "Failed to get job id from output: {}", output);
            None
        }
    }
}

pub async fn create_protcols(
    default_protocol: &str,
    protocol_map: &Arc<RwLock<HashMap<String, Protocol>>>,
    workarea: &str,
    ignore_error: bool,
) -> Result<(), String> {
    let local_dir = format!("{}/farm_protocols", workarea);
    let path = PathBuf::from_str(&local_dir).unwrap();
    if  !path.is_dir() {
        return Err(format!("{} is not a valid directory.", local_dir));
    }
    let mut guard = protocol_map.write().await;
    let mut default_set = !guard.is_empty();
    let path = PathBuf::from_str(&local_dir).unwrap();
    let mut read_dirs = match tokio::fs::read_dir(path).await {
        Ok(read_dirs) => read_dirs,
        Err(err) => {
            drop(guard);
            return Err(format!("Failed to check directory. Reason - {}", err));
        }
    };
    loop {
        if let Some(entry) = match read_dirs.next_entry().await {
            Ok(entry) => entry,
            Err(err) => {
                drop(guard);
                return Err(format!("Failed to read directory. Reason - {}", err));
            }
        } {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                let mut protocol = Protocol::new(&name, "").await;
                match protocol
                    .update(path.to_str().unwrap(), default_protocol, ignore_error)
                    .await
                {
                    Ok(_) => {}
                    Err(err) => {
                        drop(guard);
                        return Err(format!("Failed to update protocol. Reason - {}", err));
                    }
                };
                if name == default_protocol {
                    default_set = true;
                }
                guard.insert(name.to_string(), protocol);
            }
        } else {
            break;
        }
    }
    eprintln!("{:?}", guard.keys());
    drop(guard);
    if !default_set && default_protocol != "local" {
        return Err(format!("Failed to set {} as default protocol. Please provide a valid default protocol to be set.", default_protocol));
    }
    Ok(())
}

async fn __get_envs(
    protocol_map: &Arc<RwLock<HashMap<String, Protocol>>>,
    protocol: &str,
) -> HashMap<String, String> {
    let guard = protocol_map.read().await;
    let envs = match guard.get(protocol) {
        Some(protocol) => protocol.get_envs().await,
        None => HashMap::new(),
    };
    drop(guard);
    envs
}

pub async fn get_envs(
    protocol_map: &Arc<RwLock<HashMap<String, Protocol>>>,
    protocol: &str,
    default_protocol: &str,
) -> HashMap<String, String> {
    if protocol == "default" {
        __get_envs(protocol_map, default_protocol).await
    } else {
        __get_envs(&protocol_map, protocol).await
    }
}

pub async fn get_local_cores_and_memory() -> Option<(i64, i64)> {
    let mut result = None;
    let guard = crate::CONFIG.get().unwrap().protocols_map.read().await;
    if let crate::dto::FarmConfig::Local(config) = &guard.get("local").unwrap().config {
        result = Some((config.max_local_cores, config.max_local_memory));
    }
    drop(guard);
    result
}
