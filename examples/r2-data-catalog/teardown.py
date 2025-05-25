from config import ICEBERG_CATALOG_CONFIG, logger
from pyiceberg import catalog

from examples.utils.ice import purge_catalog

iceberg_catalog = catalog.load_catalog("r2_data_catalog", **ICEBERG_CATALOG_CONFIG)
logger.info("Successfully loaded Iceberg catalog.")

purge_catalog(iceberg_catalog)
