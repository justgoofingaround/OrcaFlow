pub const LOCAL_DISK_THRESHOLD: u64 = 50;

#[allow(dead_code)]
pub async fn is_worker_qualified(
    cores: i64,
    memory: i64,
    system: &mut sysinfo::System,
    master_url: &str,
    workarea: &str,
) -> Result<(), String> {
    let host_ip = local_ip_address::local_ip().unwrap();
    let mut message = String::new();
    if let Err(e) = check_for_master_conn(
        master_url
    ).await {
        message.push_str(&e);
    }
    let workarea_path = std::path::Path::new(workarea);
    if !workarea_path.is_dir() {
        message.push_str(&format!(
            "\nRestricted network access detected on host {}. Unable to connect to master at {}. Please contact IT for assistance.",
            workarea, host_ip
        ));}
    system.refresh_memory();
    if (system.available_memory() as i64) < (memory * 1024 * 1024 * 1024) {
        message.push_str(&format!(
            "\nAvailable memory in {} is less than {}GB",
            host_ip, memory
        ));
    }
    if (system.physical_core_count().unwrap_or(0) as i64) < cores {
        message.push_str(&format!(
            "\nAvailable cores in {} is less than {}",
            host_ip, cores
        ));
    }
    if message.len() > 0 {
        Err(message)
    } else {
        Ok(())
    }
}

pub async fn check_disk_space(disk_name: &str, threshold: u64, job_type: &str, _farm: &str) -> Result<(), String> {
    // Skip if job is not a spark job
    if !job_type.contains("spark") {
        return Ok(());
    }
    let host_ip = local_ip_address::local_ip().unwrap();
    let mount_point = match find_mountpoint::find_mountpoint(std::path::Path::new(disk_name)) {
        Ok(mp) => mp,
        Err(e) => {
            return Err(format!(
                "Error finding mount point for disk {} on host {}: {:?}",
                disk_name, host_ip, e
            ));
        }
    };
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let disks: Vec<&sysinfo::Disk> = disks
        .iter()
        .filter(|disk| disk.mount_point() == mount_point)
        .collect();
    if disks.len() == 0 {
        return Err(format!("Disk {} not found in {}", disk_name, host_ip));
    } else if disks[0].available_space() < threshold * 1024 * 1024 * 1024 {
        Err(format!(
            "Available space in disk {} in host {} is less than {}GB.
            Please clean up the local disk space inside {} or set a new local disk using the 'update_farm -farm <FARM> -config local_disk -value <NEW LOCAL DISK>' command",
            disk_name, host_ip, threshold, disk_name
        ))
    } else {
        Ok(())
    }
}

pub async fn get_worker_error_file_path(
    workarea: &str,
    worker_name: &str,
) -> String {
    format!("{}/.{}", workarea, worker_name)
}

#[allow(dead_code)]
async fn check_for_master_conn(master_url: &str) -> Result<(), String> {
    // Check if master is reachable
    // Creates a file in the master workarea for master to check if worker is able to connect with master
    match crate::common_utils::submit_get_request(&master_url).await {
        Ok(_) => Ok(()),
        Err(e) => {
            Err(format!("Error in sending request to master from host {}: {}",local_ip_address::local_ip().unwrap(), e))
        }
    }
}
