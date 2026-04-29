use log::warn;
use procfs::process::Process;
use procfs::WithCurrentSystemInfo;
use std::collections::HashMap;

use super::{append_data_to_file, kill_pid};

pub async fn monitor_process(
    process: &Process,
    memory_usage: &mut f64,
    _cpu_usage: &mut f64,
    disk_read_bytes: &mut f64,
    disk_write_bytes: &mut f64,
) -> Result<(), ()> {
    if let Ok(memory) = get_memory_usage(process).await {
        *memory_usage += memory;
    } else {
        return Err(());
    }
    // calculating total cpu, memory used and i/o operations used by pid
    if let Ok((read_bytes, write_bytes)) = get_disk_io(process).await {
        *disk_read_bytes += read_bytes;
        *disk_write_bytes += write_bytes;
    } else {
        return Err(());
    }
    Ok(())
}

async fn get_memory_usage(process: &Process) -> Result<f64, ()> {
    if let Ok(stat) = process.stat() {
        if !is_process_alive(&stat).await {
            return Err(());
        }
        Ok(stat.rss_bytes().get() as f64)
    } else {
        Err(())
    }
}

async fn get_disk_io(process: &Process) -> Result<(f64, f64), ()> {
    if let Ok(io) = process.io() {
        Ok((io.read_bytes as f64, io.write_bytes as f64))
    } else {
        Err(())
    }
}

async fn is_process_alive(stat: &procfs::process::Stat) -> bool {
    stat.state != 'Z' && stat.state != 'X'
}

#[allow(dead_code)]
pub async fn monitor_child_process(
    ppid: u32,
    memory_usage: &mut f64,
    cpu_usage: &mut f64,
    disk_read_bytes: &mut f64,
    disk_write_bytes: &mut f64,
) -> Result<(), ()> {
    let all_processes = match Process::new(ppid as i32) {
        Ok(all_processes) => all_processes,
        Err(_) => return Err(()),
    };
    if let Err(_) = monitor_process(
        &all_processes,
        memory_usage,
        cpu_usage,
        disk_read_bytes,
        disk_write_bytes,
    )
    .await
    {
        return Err(());
    }

    let tasks = match all_processes.tasks() {
        Ok(tasks) => tasks,
        Err(_) => return Err(()),
    };
    for task in tasks
        .filter(|x| x.is_ok() && x.as_ref().unwrap().children().is_ok())
        .map(|x| x.unwrap())
    {
        for child in task.children().unwrap() {
            let _ = Box::pin(monitor_child_process(
                child,
                memory_usage,
                cpu_usage,
                disk_read_bytes,
                disk_write_bytes,
            ))
            .await;
        }
    }
    Ok(())
}

pub async fn monitor_worker(pid: u32, resource_usage: &mut HashMap<String, f64>) -> Result<(), ()> {
    let mut memory_usage = 0.0;
    let mut cpu_usage = 0.0;
    let mut disk_read_bytes = 0.0;
    let mut disk_write_bytes = 0.0;
    match super::monitor_child_process(
        pid,
        &mut memory_usage,
        &mut cpu_usage,
        &mut disk_read_bytes,
        &mut disk_write_bytes,
    )
    .await
    {
        Ok(_) => {}
        Err(_) => {}
    };
    *resource_usage.get_mut("memory_usage").unwrap() = memory_usage;
    *resource_usage.get_mut("disk_read_bytes").unwrap() = disk_read_bytes;
    *resource_usage.get_mut("disk_write_bytes").unwrap() = disk_write_bytes;
    Ok(())
}

pub async fn wait(
    mut child: tokio::process::Child,
    logger_name: &str,
    spark_worker_url: &Option<String>,
    cache_dir: &Option<String>,
    csv_name: &str,
    ccm_master_area: &str,
    worker_name: &str,
    max_cores: i64,
    max_memory: i64,
) -> Result<(), String> {
    let pid = child.id().unwrap();
    let mut status = Ok(());
    if let Some(url) = spark_worker_url {
        super::monitor_spark_worker(
            url,
            pid,
            ccm_master_area,
            worker_name,
            logger_name,
            max_cores,
            max_memory,
        )
        .await;
        kill_pid(pid, logger_name, true).await;
        let _ = child.kill().await;
    } else {
        let monitor_path = match cache_dir {
            Some(path) => format!("{}/.monitor", path),
            None => "/.monitor".to_string(),
        };
        let _ = tokio::fs::create_dir_all(&monitor_path).await;
        let csv_name = csv_name.replace("/", "_");
        let csv_path = format!("{}/{}.csv", monitor_path, csv_name);
        create_usage_csv(&csv_path).await;
        let monitor_handle = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            if let Ok(process) = procfs::process::Process::new(pid as i32) {
                while process.is_alive() {
                    let mut resource_usage = init_usage_data().await;
                    let ip_address = local_ip_address::local_ip().unwrap();
                    let _ = monitor_worker(pid, &mut resource_usage).await;
                    let csv_lines = format!(
                        "{},{},{},{},{},{},{}\n",
                        chrono::prelude::Utc::now().timestamp(),
                        pid,
                        ip_address,
                        resource_usage.get("memory_usage").unwrap(),
                        resource_usage.get("disk_read_bytes").unwrap(),
                        resource_usage.get("disk_write_bytes").unwrap(),
                        max_memory * 1024 * 1024 * 1024,
                    );
                    let _ = super::append_data_to_file(&csv_path, &csv_lines).await;
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                }
            }
        });
        match child.wait_with_output().await {
            Ok(output) => {
                let _ = append_data_to_file(
                    &format!("{}/stderr", monitor_path),
                    &String::from_utf8_lossy(&output.stderr),
                )
                .await;
                let _ = append_data_to_file(
                    &format!("{}/stdout", monitor_path),
                    &String::from_utf8_lossy(&output.stdout),
                )
                .await;
                if !output.status.success() {
                    match output.status.code() {
                        Some(code) => {
                            status = Err(format!(
                                "Job with pid: {} returned status code {}",
                                pid, code
                            ));
                        }
                        None => {
                            status = Err(format!("Job with pid: {} was terminated by signal", pid));
                        }
                    }
                }
            }
            Err(_) => {}
        };
        monitor_handle.abort();
    }
    status
}

pub async fn create_usage_csv(csv_path: &str) {
    let header =
        "time,pid,ip_address,memory_usage(bytes),disk_read(bytes),disk_write(bytes),max_allowed_memory(bytes)\n";
    match tokio::fs::write(csv_path, header).await {
        Ok(_) => {}
        Err(e) => {
            warn!("Failed to create csv file: {}", e);
        }
    };
}

pub async fn init_usage_data() -> HashMap<String, f64> {
    HashMap::from([
        ("disk_read_bytes".to_string(), 0.0),
        ("disk_write_bytes".to_string(), 0.0),
        ("memory_usage".to_string(), 0.0),
    ])
}
