"""
Configuration for Supabase Analytics Buckets
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
DATA_DIR = Path(__file__).parent.parent.parent / "data"

# Load environment variables
load_dotenv()

# Supabase Configuration
SUPABASE_WAREHOUSE = os.getenv("SUPABASE_WAREHOUSE")
SUPABASE_TOKEN = os.getenv("SUPABASE_TOKEN")
SUPABASE_CATALOG_URI = os.getenv("SUPABASE_CATALOG_URI")
SUPABASE_AWS_ACCESS_KEY_ID = os.getenv("SUPABASE_AWS_ACCESS_KEY_ID")
SUPABASE_AWS_SECRET_ACCESS_KEY = os.getenv("SUPABASE_AWS_SECRET_ACCESS_KEY")
SUPABASE_AWS_REGION = os.getenv("SUPABASE_AWS_REGION")
SUPABASE_S3_ENDPOINT = os.getenv("SUPABASE_S3_ENDPOINT")

ICEBERG_CATALOG_CONFIG = {
    "type": "rest",
    "warehouse": SUPABASE_WAREHOUSE,
    "uri": SUPABASE_CATALOG_URI,
    "token": SUPABASE_TOKEN,
    "s3.endpoint": SUPABASE_S3_ENDPOINT,
    "s3.access-key-id": SUPABASE_AWS_ACCESS_KEY_ID,
    "s3.secret-access-key": SUPABASE_AWS_SECRET_ACCESS_KEY,
    "s3.region": SUPABASE_AWS_REGION,
}
