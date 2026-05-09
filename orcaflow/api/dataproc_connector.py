"""
Dataproc Connector for OrcaFlow

Handles SSH connection to NYU Dataproc cluster and remote Spark job submission.
Uses gcloud CLI for authentication and tunneling.
"""

import os
import subprocess
import threading
import logging
import time
import tempfile
from datetime import datetime
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# Dataproc cluster configuration
DATAPROC_PROJECT = "hpc-dataproc-19b8"
DATAPROC_ZONE = "us-central1-f"
DATAPROC_INSTANCE = "nyu-dataproc-m"
HDFS_USER_DIR = "/user/ps5390_nyu_edu"
HDFS_ORCAFLOW_DIR = f"{HDFS_USER_DIR}/orcaflow"


class DataprocConnector:
    """Manages connection and job submission to NYU Dataproc cluster."""

    HDFS_ORCAFLOW_DIR = HDFS_ORCAFLOW_DIR

    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._check_gcloud()

    def _check_gcloud(self):
        """Verify gcloud CLI is available."""
        try:
            result = subprocess.run(
                ["gcloud", "--version"],
                capture_output=True, text=True, timeout=10
            )
            self.gcloud_available = result.returncode == 0
            if self.gcloud_available:
                logger.info("gcloud CLI available")
            else:
                logger.warning("gcloud CLI not working properly")
        except FileNotFoundError:
            self.gcloud_available = False
            logger.warning("gcloud CLI not installed - Dataproc submission disabled")

    def _ssh_command(self, command: str, timeout: int = 300) -> subprocess.CompletedProcess:
        """
        Execute a command on the Dataproc master via SSH.

        Args:
            command: Shell command to run on Dataproc
            timeout: Command timeout in seconds

        Returns:
            CompletedProcess with stdout/stderr
        """
        ssh_cmd = [
            "gcloud", "compute", "ssh", DATAPROC_INSTANCE,
            "--project", DATAPROC_PROJECT,
            "--zone", DATAPROC_ZONE,
            "--command", command
        ]
        return subprocess.run(
            ssh_cmd, capture_output=True, text=True, timeout=timeout
        )

    def _scp_upload(self, local_path: str, remote_path: str) -> bool:
        """
        Upload a file to Dataproc master via SCP.

        Args:
            local_path: Local file path
            remote_path: Remote destination path

        Returns:
            True if upload succeeded
        """
        scp_cmd = [
            "gcloud", "compute", "scp",
            local_path,
            f"{DATAPROC_INSTANCE}:{remote_path}",
            "--project", DATAPROC_PROJECT,
            "--zone", DATAPROC_ZONE
        ]
        result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=1800)
        return result.returncode == 0

    def _scp_download(self, remote_path: str, local_path: str) -> bool:
        """
        Download a file or directory from Dataproc master via SCP.

        Args:
            remote_path: Remote source path
            local_path: Local destination path

        Returns:
            True if download succeeded
        """
        scp_cmd = [
            "gcloud", "compute", "scp", "--recurse",
            f"{DATAPROC_INSTANCE}:{remote_path}",
            local_path,
            "--project", DATAPROC_PROJECT,
            "--zone", DATAPROC_ZONE
        ]
        result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=300)
        return result.returncode == 0

    def upload_data_file(self, job_id: str, local_file_path: str) -> str:
        """
        Ensure a single data file is in HDFS, uploading only if needed.

        Uses a shared /orcaflow/data/ directory so the same file is never
        re-uploaded across multiple job runs.

        Args:
            job_id: Job identifier (used for temp dir naming)
            local_file_path: Absolute path to the local data file

        Returns:
            HDFS path to the directory containing the file
        """
        hdfs_data_dir = f"{HDFS_ORCAFLOW_DIR}/data"
        filename = os.path.basename(local_file_path)

        # Check if file already exists in HDFS
        self._ssh_command(f"hadoop fs -mkdir -p {hdfs_data_dir}", timeout=30)
        check = self._ssh_command(
            f"hadoop fs -test -f {hdfs_data_dir}/{filename} && echo EXISTS",
            timeout=30
        )

        if "EXISTS" in (check.stdout or ""):
            logger.info(f"File {filename} already in HDFS, skipping upload")
            return hdfs_data_dir

        # SCP file to Dataproc, then put into HDFS
        logger.info(f"Uploading {filename} to Dataproc HDFS...")
        remote_tmp = f"/tmp/orcaflow_data_{job_id}"
        self._ssh_command(f"mkdir -p {remote_tmp}", timeout=15)

        if not self._scp_upload(local_file_path, f"{remote_tmp}/{filename}"):
            raise RuntimeError(f"Failed to SCP {filename} to Dataproc")

        result = self._ssh_command(
            f"hadoop fs -put -f {remote_tmp}/{filename} {hdfs_data_dir}/",
            timeout=600
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to put {filename} into HDFS: {result.stderr}")

        self._ssh_command(f"rm -rf {remote_tmp}", timeout=15)
        logger.info(f"File uploaded to HDFS: {hdfs_data_dir}/{filename}")
        return hdfs_data_dir

    def test_connection(self) -> Dict[str, Any]:
        """Test SSH connectivity to Dataproc cluster."""
        if not self.gcloud_available:
            return {"connected": False, "error": "gcloud CLI not available"}

        try:
            result = self._ssh_command("echo connected && whoami && hadoop fs -ls /user/ps5390_nyu_edu/orcaflow 2>/dev/null || echo 'orcaflow dir not found'", timeout=30)
            if result.returncode == 0:
                return {"connected": True, "output": result.stdout.strip()}
            return {"connected": False, "error": result.stderr.strip()}
        except subprocess.TimeoutExpired:
            return {"connected": False, "error": "SSH connection timed out"}
        except Exception as e:
            return {"connected": False, "error": str(e)}

    def submit_spark_job(self, job_id: str, script_path: str, args: list = None,
                         spark_conf: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Submit a PySpark job to Dataproc via YARN.

        Args:
            job_id: Unique job identifier
            script_path: Local path to the PySpark script
            args: Command-line arguments for the script
            spark_conf: Additional Spark configuration

        Returns:
            Dictionary with job submission status
        """
        if not self.gcloud_available:
            raise RuntimeError("gcloud CLI not available - cannot submit to Dataproc")

        # Initialize job tracking
        self.jobs[job_id] = {
            "job_id": job_id,
            "status": "uploading",
            "execution_target": "dataproc_yarn",
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "completed_at": None,
            "output": "",
            "error": None,
            "yarn_app_id": None
        }

        # Upload script to Dataproc
        remote_script = f"/tmp/orcaflow_{job_id}.py"
        if not self._scp_upload(script_path, remote_script):
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = "Failed to upload script to Dataproc"
            return self.jobs[job_id]

        self.jobs[job_id]["status"] = "submitted"
        self.jobs[job_id]["started_at"] = datetime.now().isoformat()

        # Build spark-submit command
        spark_submit_parts = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "client",
        ]

        # Add spark configuration
        conf = spark_conf or {}
        conf.setdefault("spark.executor.memory", "2g")
        conf.setdefault("spark.executor.cores", "2")
        conf.setdefault("spark.driver.memory", "2g")
        for key, value in conf.items():
            spark_submit_parts.extend(["--conf", f"{key}={value}"])

        spark_submit_parts.append(remote_script)

        # Add script arguments
        if args:
            spark_submit_parts.extend(args)

        spark_submit_cmd = " ".join(spark_submit_parts)
        full_cmd = f"{spark_submit_cmd} 2>&1"

        logger.info(f"Submitting job {job_id} to Dataproc: {spark_submit_cmd}")

        # Run in background thread
        thread = threading.Thread(
            target=self._run_remote_job,
            args=(job_id, full_cmd, remote_script),
            daemon=True
        )
        thread.start()

        return {
            "job_id": job_id,
            "status": "submitted",
            "execution_target": "dataproc_yarn",
            "message": "Job submitted to NYU Dataproc cluster via YARN"
        }

    def _run_remote_job(self, job_id: str, command: str, remote_script: str):
        """Execute spark-submit on Dataproc and monitor completion."""
        try:
            self.jobs[job_id]["status"] = "running"

            # Execute spark-submit via SSH (long timeout for big jobs)
            result = self._ssh_command(command, timeout=3600)

            self.jobs[job_id]["output"] = result.stdout

            if result.returncode == 0:
                self.jobs[job_id]["status"] = "success"
                self.jobs[job_id]["progress"] = 100

                # Try to extract YARN application ID from output
                for line in result.stdout.split("\n"):
                    if "application_" in line:
                        import re
                        match = re.search(r'(application_\d+_\d+)', line)
                        if match:
                            self.jobs[job_id]["yarn_app_id"] = match.group(1)
                            break
            else:
                self.jobs[job_id]["status"] = "failed"
                self.jobs[job_id]["error"] = result.stderr or "spark-submit failed"

        except subprocess.TimeoutExpired:
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = "Job timed out (>1 hour)"
        except Exception as e:
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = str(e)
        finally:
            self.jobs[job_id]["completed_at"] = datetime.now().isoformat()

            # Cleanup remote script and temp data
            try:
                self._ssh_command(f"rm -f {remote_script}", timeout=15)
                self._ssh_command(f"rm -rf /tmp/orcaflow_data_{job_id}", timeout=15)
            except Exception:
                pass

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a Dataproc job."""
        return self.jobs.get(job_id)

    def get_job_output(self, job_id: str) -> Optional[str]:
        """Get output of a Dataproc job."""
        job = self.jobs.get(job_id)
        if job:
            return job.get("output", "")
        return None

    def list_hdfs_files(self, path: str = None) -> Dict[str, Any]:
        """List files in HDFS recursively, returning a parsed list."""
        hdfs_path = path or HDFS_ORCAFLOW_DIR
        try:
            result = self._ssh_command(f"hadoop fs -ls -R {hdfs_path}", timeout=30)
            if result.returncode != 0:
                return {"path": hdfs_path, "files": [], "error": result.stderr}

            # Skip Spark internal files
            skip_names = {"_SUCCESS", "_committed_", "_started_"}

            files = []
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("Found"):
                    continue
                parts = line.split()
                if len(parts) >= 8:
                    perms = parts[0]
                    size = int(parts[4]) if parts[4].isdigit() else 0
                    filepath = parts[-1]
                    filename = filepath.split("/")[-1]
                    is_dir = perms.startswith("d")

                    # Skip directories, Spark internal files, and part files
                    if is_dir:
                        continue
                    if (filename.startswith("part-")
                            or filename.startswith(".")
                            or any(s in filename for s in skip_names)):
                        continue

                    display = filepath.replace(HDFS_ORCAFLOW_DIR, "")
                    if not display:
                        display = "/"
                    size_mb = round(size / (1024 * 1024), 2) if size > 0 else 0
                    entry = display
                    if size_mb > 0:
                        entry += f"  ({size_mb} MB)"
                    files.append(entry)
            return {"path": hdfs_path, "files": files, "error": None}
        except Exception as e:
            return {"path": hdfs_path, "files": [], "error": str(e)}

    def get_yarn_status(self) -> Dict[str, Any]:
        """Get YARN cluster status."""
        try:
            result = self._ssh_command("yarn application -list 2>/dev/null | head -20", timeout=30)
            return {"status": "connected", "applications": result.stdout}
        except Exception as e:
            return {"status": "error", "error": str(e)}


# Global connector instance
dataproc = DataprocConnector()
