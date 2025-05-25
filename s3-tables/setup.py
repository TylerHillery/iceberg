"""
Example walkthrough on how to interact with AWS S3 Table Bucket Iceberg REST Catalog. You first need to create a S3 Table Bucket and you can follow
[README.md](./README.md) using the AWS CLI

[AWS Docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
"""

import subprocess

import pyarrow.parquet as pq
from config import (
    BUCKET_NAME,
    DATA_DIR,
    ICEBERG_CATALOG_CONFIG,
    REGION,
    S3_TABLE_BUCKET_ARN,
    log_config_info,
    logger,
)
from pyiceberg import catalog

log_config_info()

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

# Only create the table bucket if it doesn't exist
if check_result.returncode != 0 and "NotFoundException" in check_result.stderr:
    logger.info(f"Table Bucket '{BUCKET_NAME}' does not exist. Creating now...")

    create_cmd = [
        "aws",
        "s3tables",
        "create-table-bucket",
        "--region",
        REGION,
        "--name",
        BUCKET_NAME,
    ]
    create_result = subprocess.run(create_cmd, capture_output=True, text=True)
    if create_result.returncode == 0:
        logger.info(f"Table Bucket '{BUCKET_NAME}' has been created successfully")
    else:
        logger.error(f"Error creating table bucket: {create_result.stderr}")
else:
    logger.info(f"Table Bucket '{BUCKET_NAME}' already exists.")

# Connect to catalog
try:
    iceberg_catalog = catalog.load_catalog("s3_table_catalog", **ICEBERG_CATALOG_CONFIG)
    logger.info("Successfully loaded Iceberg catalog.")
except Exception as e:
    logger.error(f"Failed to load Iceberg catalog: {e}")
    raise

# Create Namespace
# Think of a namespace as similar to a "schema" in Postgres.
namespace = input("Enter the namespace name: ")
iceberg_catalog.create_namespace_if_not_exists(namespace)
logger.info(iceberg_catalog.list_namespaces())

# Read some sample data
file_path = DATA_DIR / "yellow_tripdata_2023-01.parquet"
df = pq.read_table(str(file_path))
df.to_pandas().head()

# Create table
table = iceberg_catalog.create_table_if_not_exists(
    f"{namespace}.taxi_dataset",
    schema=df.schema,
)

# Insert data
table.delete()
table.append(df)
num_rows = len(table.scan().to_arrow())
logger.info(f"Number of rows inserted: {num_rows}")
