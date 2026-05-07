"""
OrcaFlow Data Pipeline DAG

Airflow DAG that orchestrates a nightly data processing pipeline:
1. Check cluster health
2. Ingest new data from source to HDFS
3. Run PySpark analytics on HDFS via Dataproc
4. Validate results
5. Send completion notification

Schedule: Daily at 2:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG default arguments
default_args = {
    "owner": "orcaflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 5, 1),
}

# Dataproc SSH helper
GCLOUD_SSH = (
    "gcloud compute ssh nyu-dataproc-m "
    "--project hpc-dataproc-19b8 "
    "--zone us-central1-f "
    "--command"
)

HDFS_BASE = "/user/ps5390_nyu_edu/orcaflow"


def check_cluster_health(**context):
    """Verify the Dataproc cluster and YARN are healthy."""
    import subprocess

    result = subprocess.run(
        [
            "gcloud", "compute", "ssh", "nyu-dataproc-m",
            "--project", "hpc-dataproc-19b8",
            "--zone", "us-central1-f",
            "--command", "yarn node -list 2>/dev/null | head -5"
        ],
        capture_output=True, text=True, timeout=60
    )

    if result.returncode != 0:
        raise RuntimeError(f"Cluster health check failed: {result.stderr}")

    print(f"Cluster healthy:\n{result.stdout}")
    return True


def submit_analytics_job(**context):
    """Submit the PySpark analytics job to Dataproc via YARN."""
    import subprocess
    import os

    # Upload the job script
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "orcaflow", "jobs", "hdfs_analytics.py"
    )

    # SCP upload
    subprocess.run([
        "gcloud", "compute", "scp",
        script_path,
        "nyu-dataproc-m:/tmp/orcaflow_pipeline_job.py",
        "--project", "hpc-dataproc-19b8",
        "--zone", "us-central1-f",
    ], check=True, timeout=120)

    # Submit spark job
    today = datetime.now().strftime("%Y%m%d")
    output_path = f"{HDFS_BASE}/output/pipeline_{today}"

    spark_cmd = (
        "spark-submit --master yarn --deploy-mode client "
        "--conf spark.executor.memory=2g "
        "--conf spark.executor.cores=2 "
        "--conf spark.executor.instances=2 "
        f"/tmp/orcaflow_pipeline_job.py "
        f"--output {output_path} "
        f"--records 1000000"
    )

    result = subprocess.run([
        "gcloud", "compute", "ssh", "nyu-dataproc-m",
        "--project", "hpc-dataproc-19b8",
        "--zone", "us-central1-f",
        "--command", spark_cmd,
    ], capture_output=True, text=True, timeout=3600)

    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        raise RuntimeError("Spark job failed")

    print(f"Job output:\n{result.stdout[-2000:]}")

    # Push output path to XCom for downstream tasks
    context["ti"].xcom_push(key="output_path", value=output_path)


def validate_results(**context):
    """Check that output files were written to HDFS."""
    import subprocess

    output_path = context["ti"].xcom_pull(
        task_ids="run_spark_analytics", key="output_path"
    )

    result = subprocess.run([
        "gcloud", "compute", "ssh", "nyu-dataproc-m",
        "--project", "hpc-dataproc-19b8",
        "--zone", "us-central1-f",
        "--command", f"hadoop fs -ls -R {output_path} 2>/dev/null | tail -20",
    ], capture_output=True, text=True, timeout=60)

    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"No output found at {output_path}")

    print(f"Validation passed — output files:\n{result.stdout}")


def notify_completion(**context):
    """Log pipeline completion with summary."""
    output_path = context["ti"].xcom_pull(
        task_ids="run_spark_analytics", key="output_path"
    )
    print(f"Pipeline completed successfully!")
    print(f"Results available at HDFS: {output_path}")
    print(f"View in Spark History: https://dataproc.hpc.nyu.edu/sparkhistory/")

    # Notify OrcaFlow API
    try:
        import requests
        requests.post("http://localhost:8000/api/jobs/submit", json={
            "job_type": "pipeline_completion",
            "dataset_size_mb": 0,
            "code_complexity_score": 0,
            "priority": 1,
        }, timeout=5)
    except Exception:
        pass  # API might not be running


# Define the DAG
with DAG(
    "orcaflow_data_pipeline",
    default_args=default_args,
    description="OrcaFlow nightly data analytics pipeline",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    catchup=False,
    tags=["orcaflow", "spark", "analytics"],
) as dag:

    # Task 1: Health check
    t1_health = PythonOperator(
        task_id="check_cluster_health",
        python_callable=check_cluster_health,
    )

    # Task 2: Prepare HDFS output directory
    t2_prepare = BashOperator(
        task_id="prepare_hdfs_dirs",
        bash_command=(
            f'{GCLOUD_SSH} "hadoop fs -mkdir -p {HDFS_BASE}/output '
            f'&& hadoop fs -mkdir -p {HDFS_BASE}/data"'
        ),
    )

    # Task 3: Run Spark analytics
    t3_spark = PythonOperator(
        task_id="run_spark_analytics",
        python_callable=submit_analytics_job,
        execution_timeout=timedelta(hours=1),
    )

    # Task 4: Validate output
    t4_validate = PythonOperator(
        task_id="validate_results",
        python_callable=validate_results,
    )

    # Task 5: Notify completion
    t5_notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    # Define task dependencies
    t1_health >> t2_prepare >> t3_spark >> t4_validate >> t5_notify
