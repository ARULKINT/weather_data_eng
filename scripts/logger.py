"""
============================================================
File Name : logger.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Configure centralized logging for the ETL pipeline.

Description:
    Every module (Extract, Transform, Load, Main)
    will use this logger instead of print() statements.

Log File:
    logs/pipeline.log
============================================================
"""

# ==========================================================
# Standard Library Imports
# ==========================================================

import logging
from pathlib import Path

# ==========================================================
# Create Log Directory
# ==========================================================

LOG_DIRECTORY = Path("logs")
LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)

# ==========================================================
# Log File Path
# ==========================================================

LOG_FILE = LOG_DIRECTORY / "pipeline.log"

# ==========================================================
# Configure Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# ==========================================================
# Create Logger Object
# ==========================================================

logger = logging.getLogger("Weather_ETL")