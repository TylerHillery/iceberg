"""
This script demonstrates how to query an Cloudflare R2 Data Catalog using DuckDB with the Iceberg extension.
"""

import altair as alt
import duckdb
import pandas as pd
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

pd.set_option("display.max_rows", 500)  # Show up to 500 rows
pd.set_option("display.max_columns", None)  # Show all columns
pd.set_option("display.width", None)  # Don't wrap columns
pd.set_option("display.max_colwidth", None)  # Show full column content

# Define library groups
graph_libs = [
    "matplotlib",
    "seaborn",
    "plotly",
    "bokeh",
    "altair",
    "pygal",
    "graph-tool",
    "holoviews",
    "geopandas",
    "mayavi",
    "vispy",
]
ml_libs = [
    "scikit-learn",
    "tensorflow",
    "keras",
    "xgboost",
    "lightgbm",
    "catboost",
    "pytorch",
    "fastai",
    "statsmodels",
    "mlflow",
]
data_libs = [
    "pandas",
    "numpy",
    "polars",
    "dask",
    "modin",
    "duckdb",
    "pyarrow",
]

# Build a mapping from package to group
lib_group_map = {}
for lib in graph_libs:
    lib_group_map[lib] = "graph"
for lib in ml_libs:
    lib_group_map[lib] = "ml"
for lib in data_libs:
    lib_group_map[lib] = "data"

all_libs = list(lib_group_map.keys())

# Query all relevant packages at once
df = duckdb.query(f"""
    SELECT
        package_name,
        downloads
    FROM
        r2_catalog.default.pypi_package_downloads_per_week
    WHERE
        package_downloaded_week = '2025-05-12'
        -- AND package_name IN ({",".join([f"'{lib}'" for lib in all_libs])})
    ORDER BY
        downloads DESC
    LIMIT
        30
""").to_df()

# Add group column
df["group"] = df["package_name"].map(lib_group_map)


# Function to plot bar chart for a group
def plot_group_chart(group_name=None):
    if group_name is not None:
        group_df = df[df["group"] == group_name]
        title = f"Top {group_name.capitalize()} PyPI Package Downloads for 2025-05-12"
    else:
        group_df = df
        title = "Top PyPI Package Downloads for 2025-05-12"
    chart = (
        alt.Chart(group_df)
        .mark_bar()
        .encode(
            x=alt.X("downloads:Q", title="Downloads"),
            y=alt.Y("package_name:N", sort="-x", title="Package Name"),
            tooltip=["package_name:N", "downloads:Q"],
        )
        .properties(width=600, height=400, title=title)
    )
    return chart


# Example: display charts for each group
plot_group_chart("graph")
plot_group_chart("ml")
plot_group_chart("data")
plot_group_chart()
