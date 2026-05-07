"""
OrcaFlow FastAPI Server

Provides REST API for job submission, monitoring, and cluster status.
Integrates ML classifier, job router, Kafka events, and Dataproc connector.

Usage:
    python start_server.py

Environment variables:
    PORT: API server port (default: 8000)
    HOST: API server host (default: 0.0.0.0)
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import psutil
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import OrcaFlow components
from spark_executor import executor as local_executor
from job_router import router as job_router
from kafka_events import event_producer, event_consumer
from dataproc_connector import dataproc

# Initialize FastAPI
app = FastAPI(
    title="OrcaFlow API",
    description="Intelligent Workload Orchestration System with ML-based Job Routing and Distributed Spark Execution",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Job counter for ID generation
job_counter = 0


@app.on_event("startup")
async def startup_event():
    """Start background services on server startup."""
    event_consumer.start()
    logger.info("OrcaFlow API v2.0 started — ML classifier, Kafka, Dataproc integration active")


# ============================================================
# Health & Status Endpoints
# ============================================================

@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, Any]:
    """API health check with component status."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.0.0",
        "components": {
            "ml_classifier": job_router.classifier.model is not None or "rule_based_fallback",
            "kafka": event_producer.enabled,
            "dataproc": dataproc.gcloud_available,
        }
    }


@app.get("/api/cluster/status", tags=["Cluster"])
async def cluster_status() -> Dict[str, Any]:
    """Get cluster and job status with system metrics."""
    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        disk = psutil.disk_usage("/")
    except Exception as e:
        logger.warning(f"Could not get system metrics: {e}")
        cpu_percent = 0
        memory_percent = 0
        disk = None

    all_jobs = job_router.list_jobs()
    running = sum(1 for j in all_jobs if j.get("status") == "running")
    queued = sum(1 for j in all_jobs if j.get("status") in ("queued", "submitted", "uploading"))
    success = sum(1 for j in all_jobs if j.get("status") == "success")
    failed = sum(1 for j in all_jobs if j.get("status") == "failed")
    local_jobs = sum(1 for j in all_jobs if j.get("execution_target") == "local")
    cluster_jobs = sum(1 for j in all_jobs if j.get("execution_target") == "dataproc_yarn")

    return {
        "cluster_name": "orcaflow-cluster",
        "cpu_usage": cpu_percent,
        "memory_usage": memory_percent,
        "disk_usage": disk.percent if disk else 0,
        "total_jobs": len(all_jobs),
        "running_jobs": running,
        "queued_jobs": queued,
        "success_jobs": success,
        "failed_jobs": failed,
        "local_jobs": local_jobs,
        "cluster_jobs": cluster_jobs,
        "dataproc_connected": dataproc.gcloud_available,
        "kafka_connected": event_producer.enabled,
        "jobs": all_jobs[:10],
    }


# ============================================================
# Job Management Endpoints
# ============================================================

@app.post("/api/jobs/submit", tags=["Jobs"])
async def submit_job(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a job with ML-based classification and intelligent routing.

    The ML classifier analyzes job parameters and routes to:
    - Local execution (small_quick jobs)
    - NYU Dataproc YARN cluster (medium_cpu / large_intensive jobs)
    """
    global job_counter

    job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{job_counter:04d}"
    job_counter += 1

    # Prepare job data
    job_data = {
        "job_type": request_data.get("job_type", "batch_analytics"),
        "dataset_size_mb": request_data.get("dataset_size_mb", 100),
        "code_complexity_score": request_data.get("code_complexity_score", 5),
        "memory_requirement_mb": request_data.get("memory_requirement_mb", 1024),
        "cpu_requirement_cores": request_data.get("cpu_requirement_cores", 2),
        "priority": request_data.get("priority", 5),
        "estimated_duration_min": request_data.get("estimated_duration_min", 30),
        "input_path": request_data.get("input_path"),
        "output_path": request_data.get("output_path"),
        "script_path": request_data.get("script_path"),
    }

    try:
        # Emit Kafka event: submitted
        event_producer.emit_job_submitted(job_id, job_data)

        # Classify and route
        result = job_router.route_and_submit(job_id, job_data)

        # Emit Kafka events: classified and routed
        if "classification" in result:
            event_producer.emit_job_classified(job_id, result["classification"])
        event_producer.emit_job_routed(
            job_id, result.get("execution_target", "local"),
            result.get("classification", {}).get("job_class", "unknown")
        )

        logger.info(f"Job {job_id} submitted -> {result.get('execution_target')} ({result.get('classification', {}).get('job_class')})")

        return result

    except Exception as e:
        event_producer.emit_job_failed(job_id, str(e))
        logger.error(f"Job submission failed: {e}")
        raise HTTPException(status_code=500, detail=f"Job submission failed: {str(e)}")


@app.get("/api/jobs/{job_id}", tags=["Jobs"])
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """Get status of a specific job (checks both local and Dataproc)."""
    status = job_router.get_job_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return status


@app.get("/api/jobs/{job_id}/output", tags=["Jobs"])
async def get_job_output(job_id: str) -> Dict[str, Any]:
    """Get output/logs for a specific job."""
    output = job_router.get_job_output(job_id)
    if output is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {"job_id": job_id, "output": output}


@app.post("/api/jobs/{job_id}/cancel", tags=["Jobs"])
async def cancel_job(job_id: str) -> Dict[str, Any]:
    """Cancel a running job."""
    result = local_executor.cancel_job(job_id)
    if result:
        event_producer.emit("job_cancelled", job_id)
        return {"job_id": job_id, "status": "cancelled", "message": "Job cancelled"}
    raise HTTPException(status_code=400, detail="Could not cancel job")


@app.get("/api/jobs", tags=["Jobs"])
async def list_jobs(status: Optional[str] = None) -> Dict[str, Any]:
    """List all jobs from both local and Dataproc executors."""
    jobs = job_router.list_jobs(status=status)
    return {"jobs": jobs, "total": len(jobs)}


# ============================================================
# Classification Endpoint
# ============================================================

@app.post("/api/classify", tags=["ML Classifier"])
async def classify_job(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify a job without submitting it.
    Returns the ML prediction and recommended execution target.
    """
    classification = job_router.classify_job(request_data)
    resources = job_router.classifier.get_resource_estimate(classification["job_class"])

    target = "local" if classification["job_class"] == "small_quick" else "dataproc_yarn"

    return {
        "classification": classification,
        "resource_estimate": resources,
        "recommended_target": target,
    }


# ============================================================
# Dataproc & Kafka Endpoints
# ============================================================

@app.get("/api/dataproc/status", tags=["Dataproc"])
async def dataproc_status() -> Dict[str, Any]:
    """Check Dataproc cluster connection and YARN status."""
    conn = dataproc.test_connection()
    return {
        "gcloud_available": dataproc.gcloud_available,
        "connection": conn,
    }


@app.get("/api/dataproc/hdfs", tags=["Dataproc"])
async def hdfs_listing(path: Optional[str] = None) -> Dict[str, Any]:
    """List files in HDFS on the Dataproc cluster."""
    return dataproc.list_hdfs_files(path)


@app.get("/api/dataproc/yarn", tags=["Dataproc"])
async def yarn_status() -> Dict[str, Any]:
    """Get YARN application status from Dataproc."""
    return dataproc.get_yarn_status()


@app.get("/api/events", tags=["Kafka"])
async def get_events(limit: int = 50) -> Dict[str, Any]:
    """Get recent Kafka events for the dashboard."""
    events = event_consumer.get_recent_events(limit=limit)
    return {"events": events, "total": len(events), "kafka_enabled": event_consumer.enabled}


# ============================================================
# Dashboard
# ============================================================

@app.get("/", tags=["Dashboard"])
async def serve_dashboard() -> FileResponse | Dict[str, Any]:
    """Serve the analytics dashboard."""
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "ui", "dashboard.html"),
        os.path.join(os.path.dirname(__file__), "ui", "dashboard.html"),
        os.path.join(os.getcwd(), "orcaflow", "ui", "dashboard.html"),
        os.path.join(os.getcwd(), "ui", "dashboard.html"),
    ]

    for dashboard_path in possible_paths:
        if os.path.exists(dashboard_path):
            return FileResponse(dashboard_path)

    return {
        "message": "OrcaFlow API v2.0",
        "documentation": "/docs",
        "endpoints": {
            "submit_job": "POST /api/jobs/submit",
            "classify_job": "POST /api/classify",
            "list_jobs": "GET /api/jobs",
            "cluster_status": "GET /api/cluster/status",
            "dataproc_status": "GET /api/dataproc/status",
            "kafka_events": "GET /api/events",
        }
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")

    logger.info("=" * 60)
    logger.info("Starting OrcaFlow Server v2.0")
    logger.info("=" * 60)
    logger.info(f"Dashboard: http://localhost:{port}")
    logger.info(f"API Docs:  http://localhost:{port}/docs")
    logger.info(f"Dataproc:  {'connected' if dataproc.gcloud_available else 'not available'}")
    logger.info(f"Kafka:     {'connected' if event_producer.enabled else 'not available'}")
    logger.info("=" * 60)

    uvicorn.run(app, host=host, port=port, log_level="info")
