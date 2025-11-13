import pyarrow.parquet as pq
from config import (
    DATA_DIR,
    ICEBERG_CATALOG_CONFIG,
    logger,
)
from pyiceberg import catalog

# Connect to catalog
iceberg_catalog = catalog.load_catalog(
    "supabase_analytics_bucket_catalog", **ICEBERG_CATALOG_CONFIG
)
logger.info("Successfully loaded Iceberg catalog.")

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
