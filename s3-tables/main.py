"""
Example walkthrough on how to interact with AWS S3 Table Bucket Iceberg REST Catalog. You first need to create a S3 Table Bucket and you can follow 
[README.md](./README.md) using the AWS CLI

[AWS Docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
"""

# Setup Configuration
import os
import logging

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from dotenv import load_dotenv
from pyiceberg import catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

load_dotenv()

AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
AWS_PROFILE = os.getenv("AWS_PROFILE")
REGION = os.getenv("REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

S3_TABLE_BUCKET_ARN = f"arn:aws:s3tables:{REGION}:{AWS_ACCOUNT_ID}:bucket/{BUCKET_NAME}"
ICEBERG_REST_ENDPOINT = f"https://s3tables.{REGION}.amazonaws.com/iceberg"

ICEBERG_CATALOG_CONFIG = {
    "type": "rest",
    "warehouse": S3_TABLE_BUCKET_ARN,
    "uri": ICEBERG_REST_ENDPOINT, 
    "rest.sigload_catalogv4-enabled": "true",
    "rest.sigv4-enabled": "true", # needed to set this and install boto for aws sso to work 
    "rest.signing-name": "s3tables",
    "rest.signing-region": REGION,
}

logger.info(f"AWS Account ID: {AWS_ACCOUNT_ID}")
logger.info(f"AWS Profile: {AWS_PROFILE}")
logger.info(f"Region: {REGION}")
logger.info(f"Bucket Name: {BUCKET_NAME}")
logger.info(f"S3 Table Bucket ARN: {S3_TABLE_BUCKET_ARN}")
logger.info(f"Iceberg REST Endpoint: {ICEBERG_REST_ENDPOINT}")

# Connect to catalog
try:
    catalog = catalog.load_catalog(
        "s3_table_catalog",
        **ICEBERG_CATALOG_CONFIG
    )
    logger.info("Successfully loaded Iceberg catalog.")
except Exception as e:
    logger.error(f"Failed to load Iceberg catalog: {e}")
    raise

# Create Namespace
# Think of a namespace as similar to a "schema" in Postgres.
namespace = input("Enter the namespace name: ")
catalog.create_namespace_if_not_exists(namespace)
logger.info(catalog.list_namespaces())

# Read some sample data
file_path = DATA_DIR / "yellow_tripdata_2023-01.parquet"
df = pq.read_table(str(file_path))
df.to_pandas().head()

# Create table
table = catalog.create_table_if_not_exists(
    f"{namespace}.taxi_dataset",
    schema=df.schema,
)

# Insert data
table.delete()
table.append(df)
num_rows = len(table.scan().to_arrow())
logger.info(f"Number of rows inserted: {num_rows}")

# Query with DuckDB

# Install extensions 
install_extensions_sql = """
INSTALL iceberg;
LOAD iceberg;
"""

duckdb.sql(install_extensions_sql)

# Create Secrets
create_secrets_sql = f"""
CREATE OR REPLACE SECRET (
      TYPE S3,
      PROVIDER credential_chain,
      CHAIN 'sso',
      PROFILE '{AWS_PROFILE}'
  )
"""
duckdb.query(create_secrets_sql)

attach_sql = f"""
DETACH DATABASE IF EXISTS s3_tables_catalog;
ATTACH '{S3_TABLE_BUCKET_ARN}' AS s3_tables_catalog (
   TYPE iceberg,
   AUTHORIZATION_TYPE 'SIGV4',
   ENDPOINT '{ICEBERG_REST_ENDPOINT.removeprefix("https://")}'
);
"""

duckdb.query(attach_sql)

duckdb.query("SHOW ALL TABLES")

duckdb.query("""
    select 
        * 
    from 
        s3_tables_catalog.default.taxi_dataset 
    limit 
        2
""")
