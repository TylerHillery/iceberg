import subprocess

from config import (
    BUCKET_NAME,
    ICEBERG_CATALOG_CONFIG,
    REGION,
    S3_TABLE_BUCKET_ARN,
    log_config_info,
    logger,
)
from pyiceberg import catalog

from examples.utils.ice import purge_catalog

log_config_info()

iceberg_catalog = catalog.load_catalog("s3_table_catalog", **ICEBERG_CATALOG_CONFIG)
logger.info("Successfully loaded Iceberg catalog.")

purge_catalog(iceberg_catalog)

check_cmd = [
    "aws",
    "s3tables",
    "get-table-bucket",
    "--table-bucket-arn",
    S3_TABLE_BUCKET_ARN,
    "--region",
    REGION,
]
check_result = subprocess.run(check_cmd, capture_output=True, text=True)

if check_result.returncode == 0:
    logger.info(f"Table Bucket '{BUCKET_NAME}' exists. Deleting now...")

    delete_cmd = [
        "aws",
        "s3tables",
        "delete-table-bucket",
        "--region",
        REGION,
        "--table-bucket-arn",
        S3_TABLE_BUCKET_ARN,
    ]
    delete_result = subprocess.run(delete_cmd, capture_output=True, text=True)
    if delete_result.returncode == 0:
        logger.info(f"Table Bucket '{BUCKET_NAME}' has been deleted successfully")
    else:
        logger.error(f"Error deleting table bucket: {delete_result.stderr}")
else:
    logger.info(f"Table Bucket '{BUCKET_NAME}' doesn't exist or can't be accessed.")
