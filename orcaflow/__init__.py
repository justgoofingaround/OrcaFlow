"""Initialize OrcaFlow package"""

__version__ = "1.0.0-alpha"
__author__ = "OrcaFlow Team"
__description__ = "Intelligent Workload Scheduling System for Distributed Computing"

import logging

# Configure package-level logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
logger.info(f"OrcaFlow {__version__} initialized")
