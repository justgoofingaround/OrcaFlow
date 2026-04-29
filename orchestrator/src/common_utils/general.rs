use log::info;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::{
    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
};
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::encode::pattern::PatternEncoder;
use serde::Serialize;
use sysinfo::System;
use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[allow(dead_code)]
pub fn create_logging(logger_name: &str, work_area: &str) -> RollingFileAppender {
    // Setup Rotating logging of logger_name
    let log_line_pattern = "{d(%Y-%m-%d %H:%M:%S)} | {({l}):5.5} | {f}:{L} — {m}{n}";
    let trigger_size = 30000000;
    let trigger = Box::new(SizeTrigger::new(trigger_size));
    let roller_pattern = &format!("{}/logs/{logger_name}/{logger_name}_{}.gz", work_area, "{}");
    let log_path = &format!("{}/logs/{logger_name}/{logger_name}.log", work_area);
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

#[allow(dead_code)]
fn get_message(message: Option<&serde_json::Value>) -> String {
    match message {
        Some(message) => message.to_string(),
        None => String::from("Failed to send request"),
    }
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub async fn submit_post_request(
    payload: &impl Serialize,
    endpoint: &str,
) -> Result<serde_json::Value, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    parse_response(client.post(endpoint).json(payload).send().await).await
}

#[allow(dead_code)]
pub async fn submit_get_request(endpoint: &str) -> Result<serde_json::Value, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    parse_response(client.get(endpoint).send().await).await
}

#[allow(dead_code)]
pub async fn get_unused_port() -> u16 {
    let ip_address = local_ip_address::local_ip().unwrap();
    let url = format!("{}:{}", ip_address, 0);
    let listener = tokio::net::TcpListener::bind(url).await.unwrap();
    listener.local_addr().unwrap().port()
}

#[allow(dead_code)]
pub async fn kill_pid(pid: u32, logger_name: &str, skip_parent_id: bool) {
    info!(target: logger_name, "Killing pid: {}", pid);
    let mut pids = VecDeque::new();
    if !skip_parent_id {
        pids.push_back(pid as i32);
    }
    let _ = get_child_process(pid, &mut pids).await;
    while let Some(pid) = pids.pop_front() {
        let mut sys = System::new();
        let target_pid = sysinfo::Pid::from_u32(pid as u32);
        sys.refresh_process(target_pid);
        if let Some(process) = sys.process(target_pid) {
            info!(target: logger_name, "Killing process: {} with pid: {}", process.name(), pid);
            match process.kill_with(sysinfo::Signal::Term) {
                Some(status) => {
                    if status {
                        info!(target: logger_name, "Process with pid {} killed successfully", pid);
                    } else {
                        log::warn!(target: logger_name, "Failed to kill process with pid {}. Sending Sigkill.", pid);
                        process.kill();
                    }
                }
                None => {
                    log::warn!(target: logger_name, "Failed to kill process with pid {}", pid);
                }
            }
        } else {
            log::warn!(target: logger_name, "Process with pid {} not found", pid);
        }
    }
}

#[allow(dead_code)]
pub async fn get_child_process(ppid: u32, pids: &mut VecDeque<i32>) -> Result<(), ()> {
    let all_processes = match procfs::process::Process::new(ppid as i32) {
        Ok(all_processes) => all_processes,
        Err(_) => return Err(()),
    };
    let tasks = match all_processes.tasks() {
        Ok(tasks) => tasks,
        Err(_) => return Err(()),
    };
    for task in tasks
        .filter(|x| x.is_ok() && x.as_ref().unwrap().children().is_ok())
        .map(|x| x.unwrap())
    {
        for child in task.children().unwrap() {
            pids.push_back(child as i32);
            let _ = Box::pin(get_child_process(child, pids)).await;
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub async fn get_python_binary(cluster_home: &str) -> String {
    let deps_path = std::path::Path::new(cluster_home)
        .parent()
        .unwrap()
        .join("deps");
    format!("{}/python/bin/python", deps_path.display().to_string())
}

#[allow(dead_code)]
pub async fn get_java_home(cluster_home: &str) -> String {
    let deps_path = std::path::Path::new(cluster_home)
        .parent()
        .unwrap()
        .join("deps");
    format!("{}/java", deps_path.display().to_string())
}

#[allow(dead_code)]
pub async fn get_hash(name: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

#[allow(dead_code)]
pub async fn get_flow_and_task_inst(job_name: &str) -> Result<(String, String), String> {
    let mut result: Vec<&str> = job_name.split("/").collect();
    if result.len() < 2 {
        return Err(format!(
            "Job {} not in required format. Skipping updation of state in flow engine",
            job_name
        ));
    }
    let task_inst_vec = result.split_off(result.len() - 2);
    let flow_inst = result.join("/");
    let task_inst = task_inst_vec[0].to_owned();
    Ok((flow_inst, task_inst))
}

#[allow(dead_code)]
pub async fn move_csv_files(
    src_dir: &str,
    dest_dir: &str,
    logger_name: &str,
) -> Result<(), std::io::Error> {
    let csv = std::ffi::OsStr::new("csv");
    let entries = Vec::from_iter(
        std::fs::read_dir(src_dir)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension() == Some(csv)),
    );
    for entry in entries {
        if let Some(file_name) = entry.file_name() {
            let dest_path = format!("{}/{}", dest_dir, file_name.to_str().unwrap());
            if let Err(e) = fs::rename(&entry, &dest_path).await {
                log::warn!(target: logger_name, "Failed to move file {}: {}", entry.display(), e);
            } else {
                log::info!(target: logger_name, "Moved file {} to {}", entry.display(), dest_path);
            }
        }
    }
    Ok(())
}

pub async fn append_data_to_file(file_path: &str, data: &str) -> std::io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(file_path)
        .await?;
    file.write_all(data.as_bytes()).await?;
    Ok(())
}

pub async fn log_message(message: &str, log_path: &str) {
    let current_time = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let fail_msg = format!("\n[{}] {}", current_time, message,);
    let _ = append_data_to_file(log_path, &fail_msg).await;
}

pub async fn read_file(file_path: &str) -> std::io::Result<String> {
    let content = fs::read_to_string(file_path).await?;
    Ok(content)
}

pub async fn delete_file(file_path: &str) -> std::io::Result<()> {
    fs::remove_file(file_path).await
}
