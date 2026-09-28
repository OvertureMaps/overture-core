"""
TelemetryEmitter — the single entry point for recording pipeline metrics.

The emitter provides two ways to record metrics:
- record(): Fire-and-forget for a single metric value
- record_dataframe(): Batch write for a DataFrame of MetricRecords

The caller provides run-identifying dimensions (stage, snapshot) at construction.
The emitter handles storage backend resolution, schema validation, and table
name construction.

Example:
    >>> from overture_core.telemetry import TelemetryEmitter
    >>>
    >>> emitter = TelemetryEmitter(
    ...     spark=spark,
    ...     stage="feed_ingest",
    ...     snapshot="meta-places-2026-03-04",
    ... )
    >>>
    >>> # Record a single metric
    >>> emitter.record(metric_id="IngestInputCount", value=1_250_000)
    >>>
    >>> # Record with dimensions
    >>> emitter.record(
    ...     metric_id="IngestFilterDiscardCount",
    ...     value=342,
    ...     sub_partition="invalid_geometry",
    ... )
    >>>
    >>> # Batch write a DataFrame of MetricRecords
    >>> emitter.record_dataframe(metrics_df)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when

from overture_core.telemetry.record import (
    METRIC_RECORD_SCHEMA,
    validate_dataframe,
)
from overture_core.telemetry.writers.base import MetricsWriter

logger = logging.getLogger(__name__)


class TelemetryEmitter:
    """
    Single entry point for recording pipeline metrics.

    The emitter writes metrics to a centralized storage backend. Run-identifying
    dimensions (stage, snapshot) are set at construction and applied to all
    metrics recorded through this instance.

    Environment resolution (selects the `pipeline_metrics.{environment}_pipeline_metrics`
    Iceberg table):
        1. Explicit `environment` constructor arg
        2. Spark conf `spark.overture.metrics.environment`
        3. Env var OVERTURE_METRICS_ENVIRONMENT
        4. Default: "dev"

    Args:
        spark: Active SparkSession
        stage: Pipeline stage producing the metrics. Conventionally one of
            "feed_ingest", "matching", "corpus_load", "merge", "theme_promote",
            but any string is accepted; the value is used as a partition key.
        snapshot: Dataset + version identifier per RFD #82
            (e.g., "meta-places-2026-03-04").
        environment: Deployment environment override ("dev", "staging", "prod").
            If None, resolved from Spark conf or env var.
        theme: Default theme for metrics recorded via record().
            Can be overridden per-call.
        type: Default type for metrics recorded via record().
            Can be overridden per-call.
        writer: MetricsWriter instance. If None, creates an IcebergMetricsWriter
            for the resolved environment.
    """

    def __init__(
        self,
        spark: SparkSession,
        stage: str,
        snapshot: str,
        environment: Optional[str] = None,
        theme: Optional[str] = None,
        type: Optional[str] = None,
        writer: Optional[MetricsWriter] = None,
    ):
        self.spark = spark
        self.stage = stage
        self.snapshot = snapshot
        self.environment = environment or self._resolve_environment(spark)
        self.theme = theme or ""
        self.type = type or ""

        if writer is not None:
            self.writer = writer
        else:
            from overture_core.telemetry.writers.iceberg import IcebergMetricsWriter

            self.writer = IcebergMetricsWriter(environment=self.environment)

        logger.info(
            f"TelemetryEmitter initialized: stage={stage}, snapshot={snapshot}, environment={self.environment}"
        )

    def record(
        self,
        metric_id: str,
        value: float,
        primary_partition: Optional[str] = None,
        sub_partition: Optional[str] = None,
        location_id: Optional[str] = None,
        metadata: Optional[str] = None,
        theme: Optional[str] = None,
        type: Optional[str] = None,
        baseline_snapshot: Optional[str] = None,
    ) -> None:
        """
        Record a single metric value. Writes immediately (fire-and-forget).

        The emitter auto-fills stage, snapshot, and timestamp from its context.
        The caller provides metric_id, value, and optional dimension fields.

        Args:
            metric_id: What's being measured (e.g., "IngestInputCount")
            value: The numeric metric value
            primary_partition: Primary slicing dimension (optional)
            sub_partition: Secondary slicing dimension (optional)
            location_id: Geographic identifier (optional)
            metadata: Additional context (optional)
            theme: Override the emitter's default theme (optional)
            type: Override the emitter's default type (optional)
            baseline_snapshot: Snapshot id of the baseline this metric was
                compared against, if applicable (optional)
        """
        row = Row(
            metric_id=metric_id,
            stage=self.stage,
            value=float(value),
            timestamp=datetime.now(timezone.utc),
            snapshot=self.snapshot,
            theme=theme if theme is not None else self.theme,
            type=type if type is not None else self.type,
            location_id=location_id or "",
            primary_partition=primary_partition or "",
            sub_partition=sub_partition or "",
            metadata=metadata or "",
            baseline_snapshot=baseline_snapshot or "",
        )

        df = self.spark.createDataFrame([row], schema=METRIC_RECORD_SCHEMA)
        self.writer.write(self.spark, df, self.stage, metric_id, self.snapshot)

    def record_dataframe(self, df: DataFrame) -> None:
        """
        Write a DataFrame of MetricRecords. Validates schema, enriches missing
        columns, and writes each metric_id to its own partition path.

        If the DataFrame is missing 'stage' or 'snapshot' columns, they are
        added from the emitter's context. This supports migration from existing
        metric templates that don't yet produce these fields.

        Args:
            df: DataFrame to write. Must contain at least the required
                MetricRecord columns (after enrichment).

        Raises:
            SchemaValidationError: If the DataFrame doesn't conform to the schema
                after enrichment.
        """
        # Persist before the per-metric write loop: each metric_id triggers a
        # separate writer.write() action, and without caching the upstream
        # lineage (e.g. metric calculations over a large baseline) would be
        # recomputed once per metric. With ~30 metrics and a multi-million-row
        # baseline this turns minutes of compute into hours.
        if hasattr(self.writer, "delete_success_marker"):
            self.writer.delete_success_marker()

        enriched_df = self._enrich_dataframe(df).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            validate_dataframe(enriched_df)

            # Get distinct metric_ids and write each to its own partition path
            metric_ids = [
                row.metric_id
                for row in enriched_df.select("metric_id").distinct().collect()
            ]

            logger.info(
                f"Writing {len(metric_ids)} metric(s) for snapshot={self.snapshot}"
            )

            for metric_id in metric_ids:
                metric_df = enriched_df.filter(col("metric_id") == metric_id)
                self.writer.write(
                    self.spark, metric_df, self.stage, metric_id, self.snapshot
                )

            if hasattr(self.writer, "write_success_marker"):
                self.writer.write_success_marker()
        finally:
            enriched_df.unpersist()

    def _enrich_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Enrich a DataFrame with emitter context columns if they are missing.

        Adds stage, snapshot, theme, type columns from the emitter's context
        when they are not present in the input DataFrame. This bridges the gap
        for existing metric templates that don't produce these fields.

        Also fills null optional string columns with empty strings for
        consistency with the schema convention.
        """
        result = df

        # Add missing columns from emitter context
        if "stage" not in result.columns:
            result = result.withColumn("stage", lit(self.stage))
        if "snapshot" not in result.columns:
            result = result.withColumn("snapshot", lit(self.snapshot))
        if "timestamp" not in result.columns:
            result = result.withColumn("timestamp", current_timestamp())
        if "theme" not in result.columns:
            result = result.withColumn("theme", lit(self.theme))
        if "type" not in result.columns:
            result = result.withColumn("type", lit(self.type))

        # Fill missing optional string columns with empty strings
        optional_string_cols = [
            "theme",
            "type",
            "location_id",
            "primary_partition",
            "sub_partition",
            "metadata",
            "baseline_snapshot",
        ]
        for col_name in optional_string_cols:
            if col_name not in result.columns:
                result = result.withColumn(col_name, lit(""))
            else:
                result = result.withColumn(
                    col_name,
                    when(col(col_name).isNull(), lit("")).otherwise(col(col_name)),
                )

        # Select columns in schema order, dropping any extras not in schema
        schema_columns = [field.name for field in METRIC_RECORD_SCHEMA.fields]
        available = [c for c in schema_columns if c in result.columns]
        result = result.select(*available)

        return result

    @staticmethod
    def _resolve_environment(spark: SparkSession) -> str:
        """
        Resolve the deployment environment from Spark conf or env var.

        Priority: spark.overture.metrics.environment > OVERTURE_METRICS_ENVIRONMENT > "dev"
        """
        try:
            env = spark.conf.get("spark.overture.metrics.environment", None)
            if env:
                return env
        except Exception:
            pass
        env = os.environ.get("OVERTURE_METRICS_ENVIRONMENT")
        if env:
            return env
        logger.warning(
            "Telemetry environment could not be resolved from "
            "spark.overture.metrics.environment or OVERTURE_METRICS_ENVIRONMENT; "
            "defaulting to 'dev'. Metrics will land in the dev_pipeline_metrics table."
        )
        return "dev"
