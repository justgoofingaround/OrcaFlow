use axum::{
    routing::{get, post, delete},
    Router,
};
use clap::Parser;
use local_ip_address::local_ip;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::sync::{mpsc::channel, RwLock};

use crate::dto::{Config, ManagerMessage, Setup, SetupArgs, SparkJobHandler};
use crate::local_job_executor::job_actor;
use crate::utils::setup_logging;
use crate::worker_manager::worker_manager;
use crate::CONFIG;
use crate::{apis, utils};
pub async fn setup() {
    let setup_args = SetupArgs::parse();
    setup_logging();
    let config = Arc::new(RwLock::new(Config::new().await));
    setup_args.setup_config(&config).await;
    utils::get_or_create_db_dir(&setup_args.workarea).await;
    let protocol_map = Arc::new(RwLock::new(HashMap::new()));
    setup_args.setup_protocol_map(&protocol_map, true).await;
    let ip_address = local_ip().unwrap();
    let url = format!("{}:{}", ip_address, setup_args.port);
    let listener = tokio::net::TcpListener::bind(url).await.unwrap();
    let (worker_manager_tx, worker_manager_rx) = channel::<ManagerMessage>(1024);
    let (local_job_executor_tx, local_job_executor_rx) = channel::<ManagerMessage>(1024);
    let (job_scheduler_tx, job_scheduler_rx) = channel::<ManagerMessage>(1024);
    env::set_var(
        "MASTER_URL",
        format!("http://{}", listener.local_addr().unwrap()),
    );
    env::set_var("SPARK_MASTER_WEB_URL", setup_args.spark_web_url);
    let add_spark_worker = Arc::new(AtomicBool::new(true));
    CONFIG
        .set(Setup {
            spark_master_url: setup_args.spark_master_url,
            workarea: setup_args.workarea.clone(),
            worker_manager_tx: worker_manager_tx.clone(),
            local_job_executor_tx: local_job_executor_tx.clone(),
            cluster_home: utils::get_cluster_home().await,
            job_scheduler_tx: job_scheduler_tx.clone(),
            config: config.clone(),
            add_spark_worker: Arc::clone(&add_spark_worker),
            spark_job_handler: SparkJobHandler {
                jobs: Arc::new(RwLock::new(VecDeque::new())),
                notifier: Arc::new(tokio::sync::Notify::new()),
            },
            protocols_map: Arc::clone(&protocol_map),
        })
        .unwrap();
    tokio::spawn(utils::signal_handler());
    let _ = tokio::spawn(async move {
        worker_manager(worker_manager_rx).await;
    });

    let _ = tokio::spawn(async move {
        crate::job_scheduler::scheduler(job_scheduler_rx).await
    });
    let worker_tx = worker_manager_tx.clone();
    let _ = tokio::spawn(async move {
        job_actor(local_job_executor_rx, worker_tx).await;
    });
    let _ = tokio::spawn(async move {
        crate::spark::scheduler().await;
    });
    let worker_tx = worker_manager_tx.clone();
    let shared_state = apis::SharedState {
        worker_tx,
        logger: String::from("server"),
        scheduler_tx: job_scheduler_tx,
        config: config,
        protocol_map,
        add_spark_worker,
    };
    let app: Router<()> = Router::new()
        .route("/", get(apis::heartbeat))
        .route("/run_job", post(apis::run_job))
        .route("/kill_job", post(apis::kill_job))
        .route("/update_worker", post(apis::update_worker))
        .route("/update_job_status", post(apis::update_job_status))
        .route("/update_config", post(apis::update_config))
        .route("/add_spark_driver", post(apis::add_spark_driver))
        .route("/refresh_protocols", post(apis::refresh_protocols))
        .route("/kill_idle_worker", post(apis::kill_idle_worker))
        .route("/update_worker_status", post(apis::update_worker_status))
        .route("/get_config", get(apis::get_config))
        .route(
            "/can_request_spark_workers",
            get(apis::can_request_spark_workers),
        )
        .route(
            "/get_running_workers_count",
            post(apis::get_running_workers_count),
        )
        .route(
            "/run_spark_job",
            post(apis::run_spark_job),
        )
        .route(
            "/get_job_status",
            post(apis::get_job_status),
        )
        .route(
            "/kill_all_jobs",
            delete(apis::kill_all_jobs),
        )
        .with_state(shared_state);
    axum::serve(listener, app).await.unwrap()
}
