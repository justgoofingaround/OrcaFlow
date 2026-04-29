use crate::common_utils::get_python_binary;
use crate::utils;
use crate::CONFIG;

#[allow(dead_code)]
pub async fn validate_spark_url(web_url: &str, _master_url: &str) -> Result<(), String> {
    // Validate spark web url
    let _ = match utils::submit_get_request(&format!("{}/json/", web_url)).await {
        Ok(response) => response,
        Err(_) => {
            return Err(format!("Invalid Spark Web UrL provided"));
        }
    };
    // Add a validation for spark master url
    Ok(())
}

pub async fn get_spark_log_dir() -> String {
    let mut log_path = std::path::PathBuf::from(&CONFIG.get().unwrap().workarea);
    log_path.push("logs");
    log_path.push("worker_logs");
    log_path.to_str().unwrap().to_string()
}

pub async fn get_spark_pid_dir() -> String {
    let mut pid_path = std::path::PathBuf::from(&CONFIG.get().unwrap().workarea);
    pid_path.push("worker_pids");
    pid_path.to_str().unwrap().to_string()
}

pub async fn set_spark_env(
    job_name: &str,
    spark_webui_port: &u16,
    cluster_home: &str,
) -> std::collections::HashMap<String, String> {
    let py_binary = get_python_binary(cluster_home).await;
    std::collections::HashMap::from([
        (String::from("SPARK_LOG_DIR"), get_spark_log_dir().await),
        (String::from("SPARK_IDENT_STRING"), job_name.to_owned()),
        (String::from("SPARK_PID_DIR"), get_spark_pid_dir().await),
        (
            String::from("SPARK_WORKER_WEBUI_PORT"),
            spark_webui_port.to_string(),
        ),
        (String::from("PYSPARK_PYTHON"), py_binary.clone()),
        (String::from("PYSPARK_DRIVER_PYTHON"), py_binary),
        (String::from("SPARK_NO_DAEMONIZE"), String::from("true")),
    ])
}
