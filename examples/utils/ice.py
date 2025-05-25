"""
Iceberg catalog utility functions.
"""

import logging

from pyiceberg import catalog

logger = logging.getLogger(__name__)


def purge_catalog(catalog: catalog.Catalog) -> int:
    """
    Purge all namespaces and tables from an Iceberg catalog.

    Args:
        catalog: The Iceberg catalog to purge
        on_purge_table: Optional callback function to execute after purging each table,
                       receives namespace and table name as arguments

    Returns:
        int: Number of tables purged
    """
    tables_purged = 0
    found_namespace = False

    for (namespace,) in catalog.list_namespaces():
        found_namespace = True
        for ns, table in catalog.list_tables(namespace):
            table_identifier = f"{ns}.{table}"
            catalog.purge_table(table_identifier)
            tables_purged += 1
            logger.info(f"Purged table: {table_identifier}")
        catalog.drop_namespace(namespace)
        logger.info(f"Dropped namespace: {namespace}")

    if not found_namespace:
        logger.info("Nothing to delete: no namespaces found.")

    return tables_purged
