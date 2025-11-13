"""
This script demonstrates how to query an AWS S3 Table using DuckDB with the Iceberg extension.

Prerequisites:
You must first run setup.py to create the Analytcs Bucket, namespace, and table
with sample data. The setup script handles the initial configuration and data loading.
"""

import duckdb
from config import (
    SUPABASE_AWS_ACCESS_KEY_ID,
    SUPABASE_AWS_REGION,
    SUPABASE_AWS_SECRET_ACCESS_KEY,
    SUPABASE_CATALOG_URI,
    SUPABASE_S3_ENDPOINT,
    SUPABASE_TOKEN,
    SUPABASE_WAREHOUSE,
)

# Install extensions
install_extensions_sql = """
INSTALL iceberg;
LOAD iceberg;
"""

duckdb.query(install_extensions_sql)

# Create Secrets
create_secrets_sql = f"""
CREATE OR REPLACE SECRET supabase_s3 (
    TYPE s3,
    KEY_ID '{SUPABASE_AWS_ACCESS_KEY_ID}',
    SECRET '{SUPABASE_AWS_SECRET_ACCESS_KEY}',
    ENDPOINT '{SUPABASE_S3_ENDPOINT}',
    REGION '{SUPABASE_AWS_REGION}',
    URL_STYLE 'path'
);

CREATE OR REPLACE SECRET supabase_iceberg (
    TYPE iceberg,
    TOKEN '{SUPABASE_TOKEN}'
);
"""
duckdb.query(create_secrets_sql)

attach_sql = f"""
DETACH DATABASE IF EXISTS supabase_analytics_bucket_catalog;
ATTACH '{SUPABASE_WAREHOUSE}' AS supabase_analytics_bucket_catalog (
    TYPE ICEBERG,
    SECRET supabase_iceberg,
    ENDPOINT '{SUPABASE_CATALOG_URI}'
);
"""

duckdb.query(attach_sql)

duckdb.query("SHOW ALL TABLES")

print(
    duckdb.query("""
    select
        *
    from
        supabase_analytics_bucket_catalog.demo_namespace.taxi_dataset
    limit
        2
""").to_df()
)
