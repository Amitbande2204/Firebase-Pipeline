"""
utils.py

Provides shared utilities and configuration for the Recipe Analytics Pipeline.
Includes:
    - Firebase project settings
    - Directory creation for ETL, validation, and analytics
    - Standardized logging setup
    - Helper functions (ID normalization, timestamps)

Used by all pipeline modules for consistent behavior and configuration.
"""

import os
import uuid
import datetime
import logging
from dotenv import load_dotenv

# load environment variables from a .env file if present
load_dotenv()

# configuration
# project Config
PROJECT_ID = os.getenv("PROJECT_ID", "assesment-amitkumarbande")
SERVICE_ACCOUNT_PATH = os.getenv("FIREBASE_KEY_PATH", "D:\\Assignment_DataEngineer\\serviceAccountKey.json")

# directory structure
BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "ETL_Output")
VALIDATION_DIR = os.path.join(BASE_DIR, "Validation_Output")
ANALYTICS_DIR = os.path.join(BASE_DIR, "Analytics_Output")
CHART_DIR = os.path.join(ANALYTICS_DIR, "Charts")

# ensure all directories exist
for d in [OUTPUT_DIR, VALIDATION_DIR, ANALYTICS_DIR, CHART_DIR]:
    os.makedirs(d, exist_ok=True)

# logging
def get_logger(name):
    """creates a standardized logger with timestamp and level."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    return logger

# helpers 
def normalize_id(text: str):
    """convert a string into a firestore-safe document ID."""
    if not text: return f"unknown_{uuid.uuid4().hex[:6]}"
    return (
        text.strip()
        .lower()
        .replace(" ", "_")
        .replace("'", "")
        .replace(".", "")
        .replace(",", "")
    )

def now_iso():
    return datetime.datetime.utcnow().isoformat() + "Z"