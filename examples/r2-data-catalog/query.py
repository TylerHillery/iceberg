"""
This script demonstrates how to query an Cloudflare R2 Data Catalog using DuckDB with the Iceberg extension.
"""

import duckdb
from config import (
    CLOUDFLARE_CATALOG_URI,
    CLOUDFLARE_TOKEN,
    CLOUDFLARE_WAREHOUSE,
    logger,
)

# Install extensions
install_extensions_sql = """
INSTALL iceberg;
LOAD iceberg;
"""

duckdb.query(install_extensions_sql)

# Create Secrets
create_secrets_sql = f"""
CREATE OR REPLACE SECRET r2_secret (
    TYPE ICEBERG,
    TOKEN '{CLOUDFLARE_TOKEN}'
);
"""
duckdb.query(create_secrets_sql)

attach_sql = f"""
DETACH DATABASE IF EXISTS r2_catalog;
ATTACH '{CLOUDFLARE_WAREHOUSE}' AS r2_catalog (
    TYPE ICEBERG,
    ENDPOINT '{CLOUDFLARE_CATALOG_URI}'
);
"""

duckdb.query(attach_sql)


logger.info(duckdb.query("SHOW ALL TABLES"))

duckdb.query("""
    select
        *
    from
        r2_catalog.default.pypi_packages
    limit
        2
""")
