import duckdb

from config import AWS_PROFILE, S3_TABLE_BUCKET_ARN, ICEBERG_REST_ENDPOINT

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
