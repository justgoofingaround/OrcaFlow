"""
OrcaFlow FastAPI Server

Provides REST API for job submission, monitoring, and cluster status.
Integrates with real PySpark job executor for distributed workload management.

Features:
- Real-time job submission and tracking
- System metrics monitoring
- Job cancellation and status queries
- Dashboard serving

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

# Import Spark job executor
from spark_executor import executor as spark_executor

# Initialize FastAPI
app = FastAPI(
    title="OrcaFlow API",
    description="Intelligent Workload Orchestration System with Real PySpark Execution",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware for cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job counter for ID generation
job_counter = 0


# ============================================================
# Health & Status Endpoints
# ============================================================

@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, Any]:
    """
    API health check endpoint.
    
    Returns:
        Dictionary with health status and timestamp
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


@app.get("/api/cluster/status", tags=["Cluster"])
async def cluster_status() -> Dict[str, Any]:
    """
    Get current cluster and job status.
    
    Gathers system metrics and aggregates job statistics.
    
    Returns:
        Dictionary with cluster metrics and job statistics
    """
    # Get real system metrics
    try:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
    except Exception as e:
        logger.warning(f"Could not get system metrics: {e}")
        cpu_percent = 0
        memory_percent = 0
    
    # Get job statistics from Spark executor
    all_jobs = spark_executor.list_jobs()
    running = sum(1 for j in all_jobs if j.get('status') == 'running')
    queued = sum(1 for j in all_jobs if j.get('status') == 'queued')
    success = sum(1 for j in all_jobs if j.get('status') == 'success')
    failed = sum(1 for j in all_jobs if j.get('status') == 'failed')
    
    return {
        "cluster_name": "orcaflow-local-cluster",
        "cpu_usage": cpu_percent,
        "memory_usage": memory_percent,
        "total_jobs": len(all_jobs),
        "running_jobs": running,
        "queued_jobs": queued,
        "success_jobs": success,
        "failed_jobs": failed,
        "jobs": all_jobs[:10]  # Return first 10 jobs
    }


# ============================================================
# Job Management Endpoints
# ============================================================

@app.post("/api/jobs/submit", tags=["Jobs"])
async def submit_job(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a new PySpark job for execution.
    
    Args:
        request_data: Job configuration dictionary with fields:
            - job_type: Type of job (default: batch_analytics)
            - dataset_size_mb: Dataset size in MB
            - code_complexity_score: Complexity level (1-10)
            - memory_requirement_mb: Memory requirement
            - cpu_requirement_cores: CPU cores needed
            - priority: Job priority (1-10)
            - estimated_duration_min: Estimated duration
    
    Returns:
        Dictionary with job_id, status, and PID
        
    Raises:
        HTTPException: If job submission fails
    """
    global job_counter
    
    job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{job_counter:04d}"
    job_counter += 1
    
    try:
        # Prepare job data
        job_data = {
            "job_type": request_data.get("job_type", "batch_analytics"),
            "dataset_size_mb": request_data.get("dataset_size_mb", 100),
            "code_complexity_score": request_data.get("code_complexity_score", 5),
            "memory_requirement_mb": request_data.get("memory_requirement_mb", 1024),
            "cpu_requirement_cores": request_data.get("cpu_requirement_cores", 2),
            "priority": request_data.get("priority", 5),
            "estimated_duration_min": request_data.get("estimated_duration_min", 30)
        }
        
        # Submit to real Spark executor
        result = spark_executor.submit_job(job_id, job_data)
        
        logger.info(f"Job {job_id} submitted successfully with PID {result.get('pid')}")
        
        return {
            "job_id": job_id,
            "status": "running",
            "message": "PySpark job queued for execution",
            "pid": result.get("pid")
        }
    
    except FileNotFoundError as e:
        logger.error(f"Job script not found: {e}")
        raise HTTPException(status_code=500, detail="Job script not found")
    
    except Exception as e:
        logger.error(f"Job submission failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Job submission failed: {str(e)}")


@app.get("/api/jobs/{job_id}", tags=["Jobs"])
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """
    Get status of a specific job.
    
    Args:
        job_id: Job identifier
    
    Returns:
        Dictionary with job status details
        
    Raises:
        HTTPException: If job not found
    """
    status = spark_executor.get_job_status(job_id)
    
    if status is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return status


@app.get("/api/jobs/{job_id}/output", tags=["Jobs"])
async def get_job_output(job_id: str) -> Dict[str, Any]:
    """
    Get output/logs for a specific job.
    
    Args:
        job_id: Job identifier
    
    Returns:
        Dictionary with job output
        
    Raises:
        HTTPException: If job not found
    """
    output = spark_executor.get_job_output(job_id)
    
    if output is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return {
        "job_id": job_id,
        "output": output
    }


@app.post("/api/jobs/{job_id}/cancel", tags=["Jobs"])
async def cancel_job(job_id: str) -> Dict[str, Any]:
    """
    Cancel a running job.
    
    Args:
        job_id: Job identifier
    
    Returns:
        Dictionary with cancellation status
        
    Raises:
        HTTPException: If cancellation fails
    """
    result = spark_executor.cancel_job(job_id)
    
    if result:
        return {
            "job_id": job_id,
            "status": "cancelled",
            "message": "Job cancelled successfully"
        }
    else:
        raise HTTPException(status_code=400, detail="Could not cancel job")


@app.get("/api/jobs", tags=["Jobs"])
async def list_jobs(status: Optional[str] = None) -> Dict[str, Any]:
    """
    List all jobs, optionally filtered by status.
    
    Args:
        status: Filter by job status (optional)
    
    Returns:
        Dictionary with jobs list and total count
    """
    jobs = spark_executor.list_jobs(status=status)
    return {"jobs": jobs, "total": len(jobs)}


# ============================================================
# Dashboard Serving
# ============================================================

@app.get("/", tags=["Dashboard"])
async def serve_dashboard() -> FileResponse | Dict[str, Any]:
    """
    Serve the analytics dashboard.
    
    Returns:
        HTML dashboard file or API documentation
    """
    # Try multiple paths
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "ui", "dashboard.html"),
        os.path.join(os.path.dirname(__file__), "ui", "dashboard.html"),
        os.path.join(os.getcwd(), "orcaflow", "ui", "dashboard.html"),
        os.path.join(os.getcwd(), "ui", "dashboard.html")
    ]
    
    for dashboard_path in possible_paths:
        if os.path.exists(dashboard_path):
            logger.info(f"Serving dashboard from: {dashboard_path}")
            return FileResponse(dashboard_path)
    
    logger.warning("Dashboard HTML not found, returning API documentation")
    return {
        "message": "OrcaFlow API - Real PySpark Job Orchestration",
        "version": "1.0.0",
        "documentation": "/docs",
        "endpoints": {
            "dashboard": "/",
            "health": "/health",
            "cluster_status": "/api/cluster/status",
            "submit_job": "POST /api/jobs/submit",
            "job_status": "GET /api/jobs/{job_id}",
            "job_output": "GET /api/jobs/{job_id}/output",
            "cancel_job": "POST /api/jobs/{job_id}/cancel",
            "list_jobs": "GET /api/jobs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv('PORT', 8000))
    host = os.getenv('HOST', '0.0.0.0')
    
    logger.info("=" * 60)
    logger.info("Starting OrcaFlow Server")
    logger.info("=" * 60)
    logger.info(f"🚀 Dashboard: http://localhost:{port}")
    logger.info(f"📚 API Docs: http://localhost:{port}/docs")
    logger.info("=" * 60)
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info"
    )
