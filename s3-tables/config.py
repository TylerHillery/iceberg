"""
Configuration for AWS S3 Table Bucket Iceberg REST Catalog
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)

# Path configuration
DATA_DIR = Path(__file__).parent.parent / "data"

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
AWS_PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Iceberg S3 Table configuration
S3_TABLE_BUCKET_ARN = f"arn:aws:s3tables:{REGION}:{AWS_ACCOUNT_ID}:bucket/{BUCKET_NAME}"
ICEBERG_REST_ENDPOINT = f"https://s3tables.{REGION}.amazonaws.com/iceberg"

ICEBERG_CATALOG_CONFIG = {
    "type": "rest",
    "warehouse": S3_TABLE_BUCKET_ARN,
    "uri": ICEBERG_REST_ENDPOINT,
    "rest.sigload_catalogv4-enabled": "true",
    "rest.sigv4-enabled": "true",  # needed to set this and install boto for aws sso to work
    "rest.signing-name": "s3tables",
    "rest.signing-region": REGION,
}


def log_config_info():
    """Log configuration information"""
    logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")
    logger.info(f"AWS Profile: {AWS_PROFILE}")
    logger.info(f"Region: {REGION}")
    logger.info(f"Bucket Name: {BUCKET_NAME}")
    logger.info(f"S3 Table Bucket ARN: {S3_TABLE_BUCKET_ARN}")
    logger.info(f"Iceberg REST Endpoint: {ICEBERG_REST_ENDPOINT}")
