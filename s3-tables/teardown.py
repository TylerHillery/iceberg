import subprocess

from pyiceberg import catalog

from config import (
    logger,
    REGION,
    BUCKET_NAME,
    S3_TABLE_BUCKET_ARN,
    ICEBERG_CATALOG_CONFIG,
    log_config_info,
)

log_config_info()

try:
    iceberg_catalog = catalog.load_catalog("s3_table_catalog", **ICEBERG_CATALOG_CONFIG)
    logger.info("Successfully loaded Iceberg catalog.")
except Exception as e:
    logger.error(f"Failed to load Iceberg catalog: {e}")
    raise

found_namespace = False
for (namespace,) in iceberg_catalog.list_namespaces():
    found_namespace = True
    for namespace, table in iceberg_catalog.list_tables(namespace):
        iceberg_catalog.purge_table(f"{namespace}.{table}")
        logger.info(f"Dropped table: {namespace}.{table}")
    iceberg_catalog.drop_namespace(namespace)
    logger.info(f"Dropped namespace: {namespace}")
if not found_namespace:
    logger.info("Nothing to delete: no namespaces found.")

# Check if the table bucket exists before attempting to delete it
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

# Only delete the table bucket if it exists
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
