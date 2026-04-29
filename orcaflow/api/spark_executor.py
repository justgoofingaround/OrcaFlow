"""
Real PySpark Job Executor

Manages the complete lifecycle of PySpark jobs:
- Job submission and subprocess management
- Real-time progress monitoring
- Status tracking and metrics collection
- Job cancellation and cleanup
"""

import os
import json
import subprocess
import time
import threading
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, List, Any

import psutil

# Configure logging
logger = logging.getLogger(__name__)


class SparkJobExecutor:
    """
    Manages PySpark job lifecycle.
    
    Submits jobs as background processes, monitors their progress,
    tracks metrics, and manages job status.
    """
    
    def __init__(self, output_base_dir: str = "/tmp/orcaflow_jobs"):
        """
        Initialize executor.
        
        Args:
            output_base_dir: Base directory for job outputs (default /tmp/orcaflow_jobs)
        """
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.output_dir = output_base_dir
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"SparkJobExecutor initialized with output dir: {self.output_dir}")
    
    def submit_job(self, job_id: str, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit a job for execution.
        
        Args:
            job_id: Unique job identifier
            job_data: Job configuration dictionary
            
        Returns:
            Dictionary with job_id, status, and PID
            
        Raises:
            FileNotFoundError: If job script not found
            Exception: If job submission fails
        """
        
        # Locate job script
        job_script = os.path.join(
            os.path.dirname(__file__),
            "..",
            "jobs",
            "sample_pyspark.py"
        )
        
        if not os.path.exists(job_script):
            raise FileNotFoundError(f"Job script not found: {job_script}")
        
        # Create job output directory
        job_output_dir = os.path.join(self.output_dir, job_id)
        os.makedirs(job_output_dir, exist_ok=True)
        
        # Create log file path
        log_file = os.path.join(job_output_dir, "job.log")
        
        # Initialize job metadata
        self.jobs[job_id] = {
            "job_id": job_id,
            "job_type": job_data.get('job_type', 'batch_analytics'),
            "status": "running",
            "created_at": datetime.now().isoformat(),
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "process": None,
            "pid": None,
            "log_file": log_file,
            "output_dir": job_output_dir,
            "data": job_data,
            "progress": 0,
            "output": "",
            "error": None,
            "records_processed": None,
            "execution_time": None,
            "cpu_usage": None,
            "memory_usage": None
        }
        
        # Get Python executable from virtual environment
        python_exe = self._get_python_executable()
        
        try:
            # Start job in a background process
            with open(log_file, 'w') as lf:
                process = subprocess.Popen(
                    [python_exe, job_script],
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    cwd=job_output_dir,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
                )
            
            self.jobs[job_id]["process"] = process
            self.jobs[job_id]["pid"] = process.pid
            
            # Start background monitoring thread
            monitor_thread = threading.Thread(
                target=self._monitor_job,
                args=(job_id,),
                daemon=True
            )
            monitor_thread.start()
            
            logger.info(f"Job {job_id} submitted with PID {process.pid}")
            
            return {
                "job_id": job_id,
                "status": "running",
                "pid": process.pid
            }
        
        except Exception as e:
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = str(e)
            logger.error(f"Job submission failed: {str(e)}")
            raise
    
    def _get_python_executable(self) -> str:
        """
        Get Python executable path from virtual environment.
        
        Returns:
            Path to Python executable
        """
        python_exe = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            ".venv",
            "Scripts",
            "python.exe"
        )
        return python_exe
    
    def _monitor_job(self, job_id: str) -> None:
        """
        Monitor job progress and completion in background thread.
        
        Args:
            job_id: Job identifier to monitor
        """
        job = self.jobs[job_id]
        process = job["process"]
        
        try:
            # Wait for process to complete
            return_code = process.wait()
            
            # Read output from log file
            try:
                with open(job["log_file"], 'r') as f:
                    output = f.read()
                job["output"] = output
            except IOError as e:
                logger.warning(f"Could not read job log: {e}")
                job["output"] = ""
            
            if return_code == 0:
                job["status"] = "success"
                job["progress"] = 100
                
                # Try to parse metrics from output
                self._parse_job_metrics(output, job)
                
            else:
                job["status"] = "failed"
                job["error"] = f"Process exited with code {return_code}"
            
            job["completed_at"] = datetime.now().isoformat()
            logger.info(f"Job {job_id} completed with status: {job['status']}")
        
        except Exception as e:
            job["status"] = "failed"
            job["error"] = str(e)
            job["completed_at"] = datetime.now().isoformat()
            logger.error(f"Job monitoring failed for {job_id}: {str(e)}")
    
    def _parse_job_metrics(self, output: str, job: Dict[str, Any]) -> None:
        """
        Parse metrics from job output.
        
        Args:
            output: Job output string
            job: Job metadata dictionary to update
        """
        try:
            if "Job Completion Report" in output:
                lines = output.split('\n')
                for line in lines:
                    if "Total records processed:" in line:
                        try:
                            count = int(line.split(':')[1].strip().replace(',', ''))
                            job["records_processed"] = count
                        except (ValueError, IndexError):
                            pass
                    elif "Execution time:" in line:
                        try:
                            time_str = line.split(':')[1].strip().split()[0]
                            job["execution_time"] = float(time_str)
                        except (ValueError, IndexError):
                            pass
        except Exception as e:
            logger.warning(f"Failed to parse job metrics: {e}")
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current job status.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Dictionary with job status details, or None if job not found
        """
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        
        # Update progress for running jobs
        if job["status"] == "running" and job["process"]:
            self._update_running_job_metrics(job)
        
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "progress": job.get("progress", 0),
            "created_at": job["created_at"],
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "job_type": job["job_type"],
            "records_processed": job.get("records_processed"),
            "execution_time": job.get("execution_time"),
            "error": job.get("error"),
            "pid": job.get("pid")
        }
    
    def _update_running_job_metrics(self, job: Dict[str, Any]) -> None:
        """
        Update metrics for running jobs.
        
        Args:
            job: Job metadata dictionary to update
        """
        try:
            if job["process"].poll() is None:
                # Still running - try to get CPU/memory metrics
                try:
                    p = psutil.Process(job["pid"])
                    cpu_percent = p.cpu_percent(interval=0.1)
                    memory_info = p.memory_info()
                    memory_mb = memory_info.rss / (1024 * 1024)
                    
                    # Simple progress heuristic
                    elapsed = (datetime.now() - 
                              datetime.fromisoformat(job["started_at"])).total_seconds()
                    job["progress"] = min(95, int(elapsed / 2))
                    job["cpu_usage"] = cpu_percent
                    job["memory_usage"] = memory_mb
                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        
        except Exception as e:
            logger.warning(f"Could not update job metrics: {e}")
    
    def list_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all jobs, optionally filtered by status.
        
        Args:
            status: Filter by status (optional)
            
        Returns:
            List of job status dictionaries
        """
        jobs = []
        for job_id in self.jobs:
            job_status = self.get_job_status(job_id)
            if job_status and (status is None or job_status["status"] == status):
                jobs.append(job_status)
        return jobs
    
    def get_job_output(self, job_id: str) -> Optional[str]:
        """
        Get job output/logs.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job output string, or None if job not found
        """
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        
        try:
            with open(job["log_file"], 'r') as f:
                return f.read()
        except IOError as e:
            logger.error(f"Could not read job output: {e}")
            return job.get("output", "")
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if cancelled successfully, False otherwise
        """
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        
        if job["process"] and job["status"] == "running":
            try:
                if os.name == 'nt':
                    # Windows: kill process group
                    os.killpg(os.getpgid(job["process"].pid), 9)
                else:
                    # Unix: terminate gracefully
                    job["process"].terminate()
                    job["process"].wait(timeout=5)
                
                job["status"] = "cancelled"
                job["completed_at"] = datetime.now().isoformat()
                logger.info(f"Job {job_id} cancelled successfully")
                return True
            
            except Exception as e:
                logger.error(f"Failed to cancel job {job_id}: {e}")
                return False
        
        return False


# Global executor instance
executor = SparkJobExecutor()
                cwd=job_output_dir,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
            )
            
            self.jobs[job_id]["process"] = process
            self.jobs[job_id]["pid"] = process.pid
            
            # Start monitoring thread
            monitor_thread = threading.Thread(
                target=self._monitor_job,
                args=(job_id,),
                daemon=True
            )
            monitor_thread.start()
            
            return {
                "job_id": job_id,
                "status": "running",
                "pid": process.pid
            }
        
        except Exception as e:
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = str(e)
            raise
    
    def _monitor_job(self, job_id):
        """Monitor job progress and completion"""
        job = self.jobs[job_id]
        process = job["process"]
        
        try:
            # Wait for process to complete
            return_code = process.wait()
            
            # Read output
            try:
                with open(job["log_file"], 'r') as f:
                    output = f.read()
                job["output"] = output
            except:
                job["output"] = ""
            
            if return_code == 0:
                job["status"] = "success"
                job["progress"] = 100
                
                # Try to parse results from output
                try:
                    if "Job Completion Report" in output:
                        # Extract metrics from output
                        lines = output.split('\n')
                        for i, line in enumerate(lines):
                            if "Total records processed:" in line:
                                try:
                                    count = int(line.split(':')[1].strip().replace(',', ''))
                                    job["records_processed"] = count
                                except:
                                    pass
                            elif "Execution time:" in line:
                                try:
                                    time_str = line.split(':')[1].strip().split()[0]
                                    job["execution_time"] = float(time_str)
                                except:
                                    pass
                except:
                    pass
            else:
                job["status"] = "failed"
                job["error"] = f"Process exited with code {return_code}"
            
            job["completed_at"] = datetime.now().isoformat()
        
        except Exception as e:
            job["status"] = "failed"
            job["error"] = str(e)
            job["completed_at"] = datetime.now().isoformat()
    
    def get_job_status(self, job_id):
        """Get job status"""
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        
        # If job is still running, check process status and update progress
        if job["status"] == "running" and job["process"]:
            try:
                if job["process"].poll() is None:
                    # Still running
                    try:
                        # Try to get CPU/memory usage as progress indicator
                        p = psutil.Process(job["pid"])
                        cpu_percent = p.cpu_percent(interval=0.1)
                        memory_info = p.memory_info()
                        memory_mb = memory_info.rss / (1024 * 1024)
                        
                        # Simple progress heuristic: progress = min(100, time_elapsed / 2)
                        elapsed = (datetime.fromisoformat(job["started_at"]) - datetime.now()).total_seconds()
                        job["progress"] = min(95, int(abs(elapsed) / 2))
                        job["cpu_usage"] = cpu_percent
                        job["memory_usage"] = memory_mb
                    except:
                        pass
            except:
                pass
        
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "progress": job.get("progress", 0),
            "created_at": job["created_at"],
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "job_type": job["job_type"],
            "records_processed": job.get("records_processed"),
            "execution_time": job.get("execution_time"),
            "error": job.get("error"),
            "pid": job.get("pid")
        }
    
    def list_jobs(self, status=None):
        """List all jobs"""
        jobs = []
        for job_id, job in self.jobs.items():
            job_status = self.get_job_status(job_id)
            if status is None or job_status["status"] == status:
                jobs.append(job_status)
        return jobs
    
    def get_job_output(self, job_id):
        """Get job output"""
        if job_id not in self.jobs:
            return None
        
        job = self.jobs[job_id]
        
        try:
            with open(job["log_file"], 'r') as f:
                return f.read()
        except:
            return job.get("output", "")
    
    def cancel_job(self, job_id):
        """Cancel a running job"""
        if job_id not in self.jobs:
            return False
        
        job = self.jobs[job_id]
        
        if job["process"] and job["status"] == "running":
            try:
                if os.name == 'nt':
                    # Windows
                    os.killpg(os.getpgid(job["process"].pid), 9)
                else:
                    # Unix
                    job["process"].terminate()
                    job["process"].wait(timeout=5)
                
                job["status"] = "cancelled"
                job["completed_at"] = datetime.now().isoformat()
                return True
            except:
                return False
        
        return False


# Global executor instance
executor = SparkJobExecutor()
