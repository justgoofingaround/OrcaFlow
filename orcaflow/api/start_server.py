"""
OrcaFlow FastAPI Server

Provides REST API for job submission, monitoring, and cluster status.
Integrates ML classifier, job router, and Dataproc connector.

Usage:
    python start_server.py

Environment variables:
    PORT: API server port (default: 8000)
    HOST: API server host (default: 0.0.0.0)
"""

import os
import logging
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any, List

import psutil
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
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
    logger.info("OrcaFlow API v2.0 started — ML classifier, Dataproc integration active")


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
        "jobs": all_jobs[:10],
    }


# ============================================================
# Path Validation Endpoint
# ============================================================

ALLOWED_EXTENSIONS = {".csv", ".tsv", ".parquet"}


@app.post("/api/validate-path", tags=["Jobs"])
async def validate_path(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that a file path exists and is a supported format."""
    path = request_data.get("path", "").strip()

    if not path:
        raise HTTPException(status_code=400, detail="No path provided")

    if not os.path.isfile(path):
        raise HTTPException(status_code=400, detail=f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}. Must be CSV, TSV, or Parquet.")

    size_bytes = os.path.getsize(path)
    size_mb = round(size_bytes / (1024 * 1024), 2)

    return {
        "valid": True,
        "path": path,
        "filename": os.path.basename(path),
        "size_bytes": size_bytes,
        "size_mb": size_mb,
    }


# ============================================================
# Job Management Endpoints
# ============================================================

# Analytics type → code complexity mapping
ANALYTICS_COMPLEXITY = {
    "profiling": 2,
    "aggregation": 5,
    "etl": 5,
    "ml": 8,
}


@app.post("/api/jobs/submit", tags=["Jobs"])
async def submit_job(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a job with ML-based classification and intelligent routing.
    Provide file_path (path to data on disk) + analytics_type.
    """
    global job_counter

    file_path = request_data.get("file_path", "").strip()
    analytics_type = request_data.get("analytics_type")

    if not file_path or not analytics_type:
        raise HTTPException(status_code=400, detail="file_path and analytics_type are required")

    if not os.path.isfile(file_path):
        raise HTTPException(status_code=400, detail=f"File not found: {file_path}")

    if analytics_type not in ANALYTICS_COMPLEXITY:
        raise HTTPException(status_code=400, detail=f"Invalid analytics_type: {analytics_type}. Must be one of: {list(ANALYTICS_COMPLEXITY.keys())}")

    job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{job_counter:04d}"
    job_counter += 1

    size_bytes = os.path.getsize(file_path)
    total_mb = round(size_bytes / (1024 * 1024), 2)
    complexity = ANALYTICS_COMPLEXITY[analytics_type]

    job_data = {
        "job_type": analytics_type,
        "analytics_type": analytics_type,
        "input_path": file_path,
        "dataset_size_mb": total_mb,
        "total_file_size_mb": total_mb,
        "code_complexity_score": complexity,
        "memory_requirement_mb": min(int(total_mb * 3), 8192),
        "cpu_requirement_cores": 4 if analytics_type == "ml" else 2,
        "priority": 5,
    }

    try:
        # Classify and route
        result = job_router.route_and_submit(job_id, job_data)

        logger.info(f"Job {job_id} submitted -> {result.get('execution_target')} ({result.get('classification', {}).get('job_class')})")

        return result

    except Exception as e:
        logger.error(f"Job submission failed: {e}")
        raise HTTPException(status_code=500, detail=f"Job submission failed: {str(e)}")


@app.post("/api/jobs/submit-script", tags=["Jobs"])
async def submit_script(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Submit a custom PySpark script to run on Dataproc against an HDFS data file."""
    global job_counter

    hdfs_path = request_data.get("hdfs_path", "").strip()
    script_content = request_data.get("script_content", "").strip()

    if not hdfs_path:
        raise HTTPException(status_code=400, detail="hdfs_path is required")
    if not script_content:
        raise HTTPException(status_code=400, detail="script_content is required")
    if not dataproc.gcloud_available:
        raise HTTPException(status_code=503, detail="Dataproc is not available")

    job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{job_counter:04d}"
    job_counter += 1

    # Save script to temp file
    script_file = os.path.join(tempfile.gettempdir(), f"orcaflow_{job_id}.py")
    with open(script_file, "w") as f:
        f.write(script_content)

    try:
        result = dataproc.submit_spark_job(
            job_id=job_id,
            script_path=script_file,
            args=[hdfs_path],
        )

        logger.info(f"Custom script job {job_id} submitted to Dataproc, data: {hdfs_path}")

        return {
            "job_id": job_id,
            "status": result["status"],
            "execution_target": "dataproc_yarn",
            "hdfs_path": hdfs_path,
            "message": "Custom script submitted to Dataproc YARN",
        }

    except Exception as e:
        logger.error(f"Script submission failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            os.remove(script_file)
        except OSError:
            pass


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
# Dataproc Endpoints
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


# ============================================================
# Dashboard
# ============================================================

@app.get("/", tags=["Dashboard"], response_model=None)
async def serve_dashboard():
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
    logger.info("=" * 60)

    uvicorn.run(app, host=host, port=port, log_level="info")
