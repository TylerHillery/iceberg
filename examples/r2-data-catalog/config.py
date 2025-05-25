"""
Configuration for Cloudflare R2 Data Catalog
"""

import logging
import os

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configuration
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_WAREHOUSE = os.getenv("CLOUDFLARE_WAREHOUSE")
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
CLOUDFLARE_CATALOG_URI = os.getenv("CLOUDFLARE_CATALOG_URI")
CLOUDFLARE_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_ACCESS_KEY_ID")
CLOUDFLARE_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_SECRET_ACCESS_KEY")
CLOUDFLARE_ENDPOINT = os.getenv("CLOUDFLARE_ENDPOINT")

ICEBERG_CATALOG_CONFIG = {
    "type": "rest",
    "warehouse": CLOUDFLARE_WAREHOUSE,
    "uri": CLOUDFLARE_CATALOG_URI,
    "token": CLOUDFLARE_TOKEN,
}

# Define catalog connection details (replace variables)
WAREHOUSE = os.getenv("WAREHOUSE")
TOKEN = os.getenv("TOKEN")
CATALOG_URI = os.getenv("CATALOG_URI")
