"""
Iceberg (S3 Tables) writer for pipeline metrics.

Writes MetricRecord DataFrames to a per-environment Iceberg table in the
``pipeline_metrics`` S3 Tables namespace. S3 Tables auto-registers them into
the Glue Data Catalog, so Superset/Athena pick them up with no Crawler.
"""

import logging
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from overture_core.iceberg import S3TABLES_CATALOG_ALIAS
from overture_core.telemetry.record import METRIC_RECORD_SCHEMA
from overture_core.telemetry.writers.base import MetricsWriter

logger = logging.getLogger(__name__)

# Default catalog alias shared with overture_core.iceberg.
S3TABLES_CATALOG = S3TABLES_CATALOG_ALIAS

# S3 Tables namespace that holds the per-environment metrics tables.
NAMESPACE = "pipeline_metrics"

_SPARK_TO_SQL_TYPE = {
    "StringType": "STRING",
    "DoubleType": "DOUBLE",
    "TimestampType": "TIMESTAMP",
}

# environment is interpolated straight into DDL/DML, so restrict it to a
# safe identifier shape.
_VALID_ENVIRONMENT = re.compile(r"^[a-z0-9_]+$")


def table_name(environment: str, catalog: str = S3TABLES_CATALOG) -> str:
    """Table identifier for an environment, matching the pre-existing Glue
    table names (e.g. ``prod_pipeline_metrics``) so Superset needs no changes."""
    if not _VALID_ENVIRONMENT.fullmatch(environment):
        raise ValueError(
            f"Invalid environment {environment!r}: must match "
            f"{_VALID_ENVIRONMENT.pattern!r} to be used in a SQL table identifier."
        )
    return f"{catalog}.{NAMESPACE}.{environment}_pipeline_metrics"


def ensure_table_exists(spark: SparkSession, table: str) -> None:
    """Create the Iceberg table if it doesn't exist yet.

    Partitioned by all three columns the write-time overwrite predicate
    filters on. Leaving snapshot out lets one file hold several snapshots,
    and overwrite()'s file-level delete rejects a partial match (#4621).
    """
    columns = ", ".join(
        f"{field.name} {_SPARK_TO_SQL_TYPE[type(field.dataType).__name__]}"
        for field in METRIC_RECORD_SCHEMA.fields
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns}) "
        "USING iceberg PARTITIONED BY (stage, metric_id, snapshot)"
    )


def _partition_field_names(spark: SparkSession, table: str) -> set[str]:
    """Partition field names across every spec the table has used: the
    ``partitions`` metadata table's struct is the union of all of them."""
    partition_column = spark.table(f"{table}.partitions").schema["partition"]
    return {field.name for field in partition_column.dataType.fields}


def migrate_partition_spec(spark: SparkSession, table: str) -> bool:
    """Bring a table created before #4621 onto the current partition spec.

    Adding the field is metadata-only, so existing files keep the old spec
    and still hit the overwrite failure until rewrite_data_files moves them
    over. rewrite-all is set so the planner doesn't skip well-sized files.

    Returns whether anything changed; safe to call repeatedly.
    """
    if "snapshot" in _partition_field_names(spark, table):
        return False

    spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD snapshot")
    catalog, _, identifier = table.partition(".")
    spark.sql(
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => '{identifier}', options => map('rewrite-all', 'true'))"
    )
    return True


class IcebergMetricsWriter(MetricsWriter):
    """
    Writes MetricRecord DataFrames to a per-environment Iceberg table.

    Args:
        environment: Deployment environment ("dev", "staging", "prod"). Selects
            the table ``pipeline_metrics.{environment}_pipeline_metrics``.
        catalog: Spark catalog alias. Override for cross-environment writes
            via a secondary catalog registered through ``extra_spark_conf``.
    """

    def __init__(self, environment: str, catalog: str = S3TABLES_CATALOG):
        self.environment = environment
        self.table = table_name(environment, catalog=catalog)

    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        stage: str,
        metric_id: str,
        snapshot: str,
    ) -> None:
        """
        Write a DataFrame of MetricRecords to the Iceberg table.

        overwrite() replaces this (stage, metric_id, snapshot)'s rows in one
        atomic commit, so retries are idempotent without the duplicate-row
        window a DELETE plus append() would leave on partial failure.
        """
        ensure_table_exists(spark, self.table)

        condition = (
            (F.col("stage") == stage)
            & (F.col("metric_id") == metric_id)
            & (F.col("snapshot") == snapshot)
        )

        logger.info(f"Writing metric '{metric_id}' to: {self.table}")
        df.writeTo(self.table).overwrite(condition)
        logger.info(f"Successfully wrote metric '{metric_id}' to {self.table}")
