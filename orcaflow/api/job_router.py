"""
Job Router for OrcaFlow

Routes jobs to the appropriate execution target based on ML classification:
- small_quick    → Local execution (subprocess on this machine)
- medium_cpu     → Dataproc YARN cluster
- large_intensive → Dataproc YARN cluster with more resources
"""

import os
import sys
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Add ml-classifier to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml-classifier"))

from inference import JobClassifier
from spark_executor import executor as local_executor
from dataproc_connector import dataproc


class JobRouter:
    """Routes jobs to local or cluster execution based on ML classification."""

    def __init__(self):
        self.classifier = JobClassifier()
        logger.info("JobRouter initialized with ML classifier")

    def classify_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify a job using the ML model.

        Args:
            job_data: Job submission data with resource requirements

        Returns:
            Classification result with job_class, confidence, etc.
        """
        features = {
            "dataset_size_mb": job_data.get("dataset_size_mb", 100),
            "code_complexity_score": job_data.get("code_complexity_score", 5),
            "memory_requirement_mb": job_data.get("memory_requirement_mb", 512),
            "cpu_requirement_cores": job_data.get("cpu_requirement_cores", 1),
        }
        return self.classifier.classify(features)

    def route_and_submit(self, job_id: str, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify a job and route it to the appropriate execution target.

        Args:
            job_id: Unique job identifier
            job_data: Job configuration

        Returns:
            Dictionary with job_id, status, classification, and execution_target
        """
        # Step 1: Classify the job
        classification = self.classify_job(job_data)
        job_class = classification["job_class"]
        confidence = classification["confidence"]

        logger.info(
            f"Job {job_id} classified as '{job_class}' "
            f"(confidence: {confidence:.2%})"
        )

        # Step 2: Get resource estimates
        resources = self.classifier.get_resource_estimate(job_class)

        # Step 3: Route based on classification
        if job_class == "small_quick":
            return self._submit_local(job_id, job_data, classification, resources)
        else:
            return self._submit_dataproc(job_id, job_data, classification, resources)

    def _submit_local(self, job_id: str, job_data: Dict[str, Any],
                      classification: Dict, resources: Dict) -> Dict[str, Any]:
        """Submit job for local execution."""
        logger.info(f"Routing job {job_id} to LOCAL execution")

        result = local_executor.submit_job(job_id, job_data)

        return {
            "job_id": job_id,
            "status": result["status"],
            "pid": result.get("pid"),
            "execution_target": "local",
            "classification": classification,
            "resource_estimate": resources,
            "message": f"Job classified as '{classification['job_class']}' — running locally"
        }

    def _submit_dataproc(self, job_id: str, job_data: Dict[str, Any],
                         classification: Dict, resources: Dict) -> Dict[str, Any]:
        """Submit job to Dataproc YARN cluster."""
        logger.info(f"Routing job {job_id} to DATAPROC YARN cluster")

        if not dataproc.gcloud_available:
            logger.warning("Dataproc not available, falling back to local execution")
            return self._submit_local(job_id, job_data, classification, resources)

        # Determine Spark configuration based on classification
        job_class = classification["job_class"]
        if job_class == "large_intensive":
            spark_conf = {
                "spark.executor.memory": "4g",
                "spark.executor.cores": "4",
                "spark.driver.memory": "4g",
                "spark.executor.instances": "4",
                "spark.sql.shuffle.partitions": "200",
            }
        else:  # medium_cpu
            spark_conf = {
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "2g",
                "spark.executor.instances": "2",
                "spark.sql.shuffle.partitions": "100",
            }

        # Determine which script to run
        script_path = job_data.get("script_path")
        if not script_path:
            script_path = os.path.join(
                os.path.dirname(__file__), "..", "jobs", "hdfs_analytics.py"
            )

        # Build script arguments from job data
        args = []
        if job_data.get("input_path"):
            args.extend(["--input", job_data["input_path"]])
        if job_data.get("output_path"):
            args.extend(["--output", job_data["output_path"]])

        result = dataproc.submit_spark_job(
            job_id=job_id,
            script_path=script_path,
            args=args,
            spark_conf=spark_conf,
        )

        return {
            "job_id": job_id,
            "status": result["status"],
            "execution_target": "dataproc_yarn",
            "classification": classification,
            "resource_estimate": resources,
            "spark_config": spark_conf,
            "message": f"Job classified as '{classification['job_class']}' — submitted to NYU Dataproc YARN cluster"
        }

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status from whichever executor is handling it.

        Checks both local executor and Dataproc connector.
        """
        # Check local first
        local_status = local_executor.get_job_status(job_id)
        if local_status:
            local_status["execution_target"] = "local"
            return local_status

        # Check Dataproc
        dataproc_status = dataproc.get_job_status(job_id)
        if dataproc_status:
            return dataproc_status

        return None

    def get_job_output(self, job_id: str) -> str:
        """Get job output from whichever executor is handling it."""
        output = local_executor.get_job_output(job_id)
        if output is not None:
            return output

        output = dataproc.get_job_output(job_id)
        if output is not None:
            return output

        return None

    def list_jobs(self, status: str = None):
        """List all jobs from both executors."""
        jobs = []

        # Local jobs
        for job in local_executor.list_jobs(status=status):
            job["execution_target"] = "local"
            jobs.append(job)

        # Dataproc jobs
        for jid, job in dataproc.jobs.items():
            if status is None or job.get("status") == status:
                jobs.append(job)

        return jobs


# Global router instance
router = JobRouter()
