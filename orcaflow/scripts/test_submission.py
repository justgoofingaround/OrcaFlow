"""Test job submission to OrcaFlow API"""

import requests
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE = "http://localhost:8000"

def test_submit_job():
    """Test submitting a job to OrcaFlow API"""
    
    logger.info("🧪 Testing Job Submission...\n")
    
    # Test job request
    job_request = {
        "job_type": "batch_analytics",
        "dataset_size_mb": 2500,
        "code_path": "/home/jobs/analytics.py",
        "parameters": {
            "input_dir": "/data/input",
            "output_dir": "/data/output"
        },
        "priority": 7,
        "estimated_duration_min": 30,
        "code_complexity_score": 6,
        "memory_requirement_mb": 2048,
        "cpu_requirement_cores": 4
    }
    
    logger.info(f"Submitting job:\n{json.dumps(job_request, indent=2)}\n")
    
    try:
        response = requests.post(
            f"{API_BASE}/api/jobs/submit",
            json=job_request,
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Job submitted successfully!\n")
            logger.info(f"Job ID: {result['job_id']}")
            logger.info(f"Status: {result['status']}")
            logger.info(f"Classification: {result['classification']['job_class']}")
            logger.info(f"Routing: {result['scheduling_decision']['execution_target']}")
            logger.info(f"Confidence: {result['classification']['confidence']:.2%}\n")
            
            return result['job_id']
        else:
            logger.error(f"❌ Error: {response.status_code}")
            logger.error(response.text)
            return None
    
    except requests.exceptions.ConnectionError:
        logger.error("❌ Could not connect to API. Is it running?")
        logger.error(f"   Try: python orcaflow/api/main.py")
        return None


def test_get_job_status(job_id: str):
    """Test getting job status"""
    
    logger.info(f"\n🧪 Testing Get Job Status...\n")
    
    try:
        response = requests.get(
            f"{API_BASE}/api/jobs/{job_id}",
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Retrieved job status!\n")
            logger.info(f"Job ID: {result['job_id']}")
            logger.info(f"Status: {result['status']}")
            logger.info(f"Created: {result['created_at']}\n")
        else:
            logger.error(f"❌ Error: {response.status_code}")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")


def test_cluster_status():
    """Test getting cluster status"""
    
    logger.info(f"\n🧪 Testing Cluster Status...\n")
    
    try:
        response = requests.get(
            f"{API_BASE}/api/cluster/status",
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Retrieved cluster status!\n")
            logger.info(f"Active Workers: {result['active_workers']}/{result['total_workers']}")
            logger.info(f"Running Jobs: {result['running_jobs']}")
            logger.info(f"Cluster Utilization: {result['cluster_utilization_pct']:.1f}%\n")
        else:
            logger.error(f"❌ Error: {response.status_code}")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")


def test_health():
    """Test API health endpoint"""
    
    logger.info("🧪 Testing API Health...\n")
    
    try:
        response = requests.get(
            f"{API_BASE}/api/health",
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ API is healthy!\n")
            logger.info(f"Status: {result['status']}")
            logger.info(f"Version: {result['version']}\n")
            return True
        else:
            logger.error(f"❌ Health check failed: {response.status_code}\n")
            return False
    
    except Exception as e:
        logger.error(f"❌ Could not connect to API: {e}\n")
        return False


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("OrcaFlow API Test Suite")
    logger.info("=" * 60 + "\n")
    
    # Test health first
    if not test_health():
        logger.error("API is not running. Start it with:")
        logger.error("  python orcaflow/api/main.py")
        exit(1)
    
    # Test job submission
    job_id = test_submit_job()
    
    if job_id:
        # Test job status
        test_get_job_status(job_id)
    
    # Test cluster status
    test_cluster_status()
    
    logger.info("=" * 60)
    logger.info("✅ Test suite complete!")
    logger.info("=" * 60)
