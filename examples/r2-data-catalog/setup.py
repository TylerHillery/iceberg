import duckdb
from config import ICEBERG_CATALOG_CONFIG, logger
from pyiceberg import catalog


class DataLoader:
    """Handles loading data from a source to Iceberg tables."""

    def __init__(
        self,
        catalog_conn: catalog.Catalog,
        db_conn: duckdb.DuckDBPyConnection,
        namespace: str,
    ):
        """
        Initialize the data loader.

        Args:
            catalog_conn: Iceberg catalog connection
            db_conn: DuckDB connection
            namespace: Target namespace for tables
        """
        self.catalog = catalog_conn
        self.conn = db_conn
        self.namespace = namespace

    def setup_bigquery_connection(self):
        """Set up BigQuery connection in DuckDB."""
        load_sql = """
        install bigquery from community;
        load bigquery;
        attach 'project=pypacktrends-prod' AS pypacktrends_prod (type bigquery, read_only);
        """
        self.conn.sql(load_sql)

    def load_table_from_query(self, query: str, target_table_name: str) -> int:
        """
        Load data from a query into an Iceberg table.

        Args:
            query: SQL query to fetch data
            target_table_name: Name for the target table

        Returns:
            Number of rows inserted
        """

        table_id = f"{self.namespace}.{target_table_name}"
        df = self.conn.query(query).arrow()

        self.catalog.purge_table(table_id)
        table = self.catalog.create_table(table_id, schema=df.schema)
        table.append(df)

        num_rows = len(table.scan().to_arrow())
        logger.info(f"Number of rows inserted into {target_table_name}: {num_rows}")

        return num_rows


con = duckdb.connect()

iceberg_catalog = catalog.load_catalog("r2_data_catalog", **ICEBERG_CATALOG_CONFIG)
logger.info("Successfully loaded Iceberg catalog.")

namespace = input("Enter the namespace name: ")
iceberg_catalog.create_namespace_if_not_exists(namespace)
logger.info(iceberg_catalog.list_namespaces())

loader = DataLoader(iceberg_catalog, con, namespace)
loader.setup_bigquery_connection()

# Load PyPI packages table
pypi_packages_table = "pypi_packages"
packages_query = f"""
select * from pypacktrends_prod.dbt.{pypi_packages_table}
"""
loader.load_table_from_query(packages_query, pypi_packages_table)

# Load PyPI downloads table
downloads_table = "pypi_package_downloads_per_week"
downloads_query = f"""
select * from pypacktrends_prod.dbt.{downloads_table}
"""
loader.load_table_from_query(downloads_query, downloads_table)
