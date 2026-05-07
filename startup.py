#!/usr/bin/env python3
"""
OrcaFlow Startup Script

Usage:
    python startup.py
    
Environment variables:
    JAVA_HOME: Path to Java 21+ installation
    PORT: API server port (default: 8000)
"""

import os
import sys
import subprocess
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_java():
    """Verify Java 21+ is available."""
    logger.info("Checking Java installation...")
    
    java_home = os.getenv('JAVA_HOME')
    if not java_home:
        logger.warning("JAVA_HOME not set. Spark may fail to start.")
        return False
    
    java_exe = os.path.join(java_home, 'bin', 'java.exe' if os.name == 'nt' else 'java')
    if not os.path.exists(java_exe):
        logger.error(f"Java not found at {java_exe}")
        return False
    
    try:
        result = subprocess.run([java_exe, '-version'], capture_output=True, text=True)
        logger.info(f"Java available: {result.stderr.split(chr(10))[0]}")
        return True
    except Exception as e:
        logger.error(f"Error checking Java: {e}")
        return False


def check_environment():
    """Check Python virtual environment."""
    logger.info("Checking virtual environment...")
    
    venv_path = Path('.venv')
    if not venv_path.exists():
        logger.warning("Virtual environment not found. Installing...")
        subprocess.run([sys.executable, '-m', 'venv', '.venv'])
    
    return True


def install_dependencies():
    """Install required packages."""
    logger.info("Installing dependencies...")
    
    requirements_file = Path('orcaflow/api/requirements.txt')
    if not requirements_file.exists():
        logger.error("requirements.txt not found")
        return False
    
    try:
        subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-r', str(requirements_file)],
            check=True
        )
        logger.info("Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install dependencies: {e}")
        return False


def start_api_server(port=8000):
    """Start the FastAPI server."""
    logger.info(f"Starting OrcaFlow API server on port {port}...")
    
    api_dir = Path('orcaflow/api')
    if not api_dir.exists():
        logger.error(f"API directory not found: {api_dir}")
        return False
    
    try:
        os.chdir(api_dir)
        subprocess.run(
            [sys.executable, '-m', 'uvicorn', 'start_server:app', f'--port={port}'],
            check=False
        )
    except Exception as e:
        logger.error(f"Failed to start API server: {e}")
        return False
    
    return True


def main():
    """Main startup sequence."""
    logger.info("=" * 60)
    logger.info("OrcaFlow Startup")
    logger.info("=" * 60)
    
    # Check prerequisites
    if not check_java():
        logger.warning("Java check failed - Spark jobs may not execute")
    
    if not check_environment():
        sys.exit(1)
    
    if not install_dependencies():
        sys.exit(1)
    
    # Start API server
    port = int(os.getenv('PORT', 8000))
    
    logger.info("=" * 60)
    logger.info(f"Starting API server on http://localhost:{port}")
    logger.info("Dashboard: http://localhost:{port}")
    logger.info("API Docs: http://localhost:{port}/docs")
    logger.info("=" * 60)
    
    start_api_server(port)


if __name__ == "__main__":
    main()
