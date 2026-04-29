"""Data models for OrcaFlow API"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from enum import Enum
from datetime import datetime


class JobType(str, Enum):
    """Types of jobs that can be submitted"""
    TRAINING = "training"
    INFERENCE = "inference"
    BATCH_ANALYTICS = "batch_analytics"
    DATA_PROCESSING = "data_processing"
    HYPERPARAMETER_SEARCH = "hyperparameter_search"


class JobPriority(int, Enum):
    """Job priority levels (1-10, higher = more urgent)"""
    LOW = 1
    MEDIUM = 5
    HIGH = 8
    CRITICAL = 10


class JobStatus(str, Enum):
    """Job execution status"""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    KILLED = "killed"
    PENDING_CLASSIFICATION = "pending_classification"


class ExecutionTarget(str, Enum):
    """Routing target for job execution"""
    LOCAL = "local"
    SPARK = "spark"


class JobSubmissionRequest(BaseModel):
    """Request to submit a new job"""
    job_type: JobType
    dataset_size_mb: int = Field(..., gt=0, description="Size of input dataset in MB")
    code_path: str = Field(..., description="Path to job code/script")
    parameters: Optional[Dict[str, Any]] = Field(default={}, description="Job parameters")
    priority: JobPriority = Field(default=JobPriority.MEDIUM)
    estimated_duration_min: Optional[int] = Field(default=None, description="User's estimate of job duration")
    code_complexity_score: Optional[float] = Field(default=5.0, ge=0, le=10, description="Estimated code complexity 0-10")
    memory_requirement_mb: Optional[int] = Field(default=512, description="Estimated memory needed")
    cpu_requirement_cores: Optional[int] = Field(default=1, description="Estimated CPU cores needed")


class ClassificationPrediction(BaseModel):
    """ML classification prediction for a job"""
    job_class: str = Field(..., description="Predicted job class")
    estimated_duration_sec: int = Field(..., description="Predicted execution duration")
    recommended_workers: int = Field(..., description="Recommended cluster worker count")
    estimated_resource_usage: Dict[str, Any] = Field(default={})
    confidence: float = Field(..., ge=0, le=1, description="Confidence score of prediction")
    reasoning: str = Field(..., description="Explanation of the prediction")


class SchedulingDecision(BaseModel):
    """Scheduling decision for a job"""
    execution_target: ExecutionTarget
    num_workers: int
    estimated_cost: float
    queue_position: int
    estimated_start_time: Optional[str] = None
    reasoning: str


class JobSubmissionResponse(BaseModel):
    """Response after job submission"""
    job_id: str
    status: JobStatus
    created_at: str
    classification: ClassificationPrediction
    scheduling_decision: SchedulingDecision


class JobStatusResponse(BaseModel):
    """Current status of a submitted job"""
    job_id: str
    status: JobStatus
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_sec: Optional[float] = None
    execution_target: ExecutionTarget
    worker_id: Optional[str] = None
    error_message: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None
    classification: ClassificationPrediction


class ClusterStatus(BaseModel):
    """Overall cluster health and resource status"""
    timestamp: str
    total_workers: int
    active_workers: int
    idle_workers: int
    total_cpu_cores: int
    available_cpu_cores: int
    total_memory_gb: float
    available_memory_gb: float
    running_jobs: int
    queued_jobs: int
    completed_jobs: int
    failed_jobs: int
    cluster_utilization_pct: float


class WorkerInfo(BaseModel):
    """Information about a cluster worker"""
    agent_id: str
    status: str  # "idle", "running", "offline"
    cpu_cores: int
    memory_mb: int
    location: Optional[str] = None
    running_job_id: Optional[str] = None
    cpu_usage_pct: float
    memory_usage_mb: int
    last_heartbeat: str


class MetricsData(BaseModel):
    """Time-series metrics data"""
    timestamp: str
    metric_name: str
    value: float
    labels: Optional[Dict[str, str]] = None
