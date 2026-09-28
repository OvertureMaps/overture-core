"""Tests for the TelemetryEmitter."""

import os
from datetime import datetime
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from overture_core.telemetry.emitter import TelemetryEmitter
from overture_core.telemetry.record import (
    METRIC_RECORD_SCHEMA,
    SchemaValidationError,
)
from overture_core.telemetry.writers.base import MetricsWriter


class MockWriter(MetricsWriter):
    """Test writer that captures write calls."""

    def __init__(self):
        self.writes = []

    def write(self, spark, df, stage, metric_id, snapshot):
        # Collect the data for assertions
        self.writes.append(
            {
                "stage": stage,
                "metric_id": metric_id,
                "snapshot": snapshot,
                "rows": df.collect(),
                "count": df.count(),
            }
        )


class TestTelemetryEmitterInit:
    """Tests for TelemetryEmitter initialization."""

    def test_init_with_explicit_writer(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="meta-places-2026-03-04",
            writer=writer,
        )
        assert emitter.stage == "feed_ingest"
        assert emitter.snapshot == "meta-places-2026-03-04"
        assert emitter.writer is writer

    def test_init_with_default_writer_resolves_iceberg_table(self, spark: SparkSession):
        emitter = TelemetryEmitter(
            spark=spark,
            stage="matching",
            snapshot="meta-places-2026-03-04",
            environment="staging",
        )
        assert (
            emitter.writer.table
            == "s3tables_catalog.pipeline_metrics.staging_pipeline_metrics"
        )

    def test_init_defaults_theme_and_type_to_empty(self, spark: SparkSession):
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=MockWriter(),
        )
        assert emitter.theme == ""
        assert emitter.type == ""

    def test_init_with_theme_and_type(self, spark: SparkSession):
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            theme="places",
            type="place",
            writer=MockWriter(),
        )
        assert emitter.theme == "places"
        assert emitter.type == "place"


class TestRecord:
    """Tests for TelemetryEmitter.record()."""

    def test_record_single_metric(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="meta-places-2026-03-04",
            theme="places",
            writer=writer,
        )

        emitter.record(metric_id="IngestInputCount", value=1_250_000)

        assert len(writer.writes) == 1
        write = writer.writes[0]
        assert write["stage"] == "feed_ingest"
        assert write["metric_id"] == "IngestInputCount"
        assert write["snapshot"] == "meta-places-2026-03-04"
        assert write["count"] == 1

        row = write["rows"][0]
        assert row.metric_id == "IngestInputCount"
        assert row.stage == "feed_ingest"
        assert row.value == 1_250_000.0
        assert row.snapshot == "meta-places-2026-03-04"
        assert row.theme == "places"

    def test_record_with_dimensions(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        emitter.record(
            metric_id="IngestFilterDiscardCount",
            value=342,
            primary_partition="meta-places",
            sub_partition="invalid_geometry",
            location_id="US",
        )

        row = writer.writes[0]["rows"][0]
        assert row.primary_partition == "meta-places"
        assert row.sub_partition == "invalid_geometry"
        assert row.location_id == "US"

    def test_record_auto_fills_timestamp(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        emitter.record(metric_id="Test", value=1.0)

        row = writer.writes[0]["rows"][0]
        assert row.timestamp is not None
        assert isinstance(row.timestamp, datetime)

    def test_record_theme_override(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            theme="places",
            writer=writer,
        )

        # Override theme for this specific call
        emitter.record(metric_id="Test", value=1.0, theme="addresses")

        row = writer.writes[0]["rows"][0]
        assert row.theme == "addresses"

    def test_record_uses_default_theme_when_not_overridden(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            theme="places",
            writer=writer,
        )

        emitter.record(metric_id="Test", value=1.0)

        row = writer.writes[0]["rows"][0]
        assert row.theme == "places"

    def test_record_converts_value_to_float(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        emitter.record(metric_id="Test", value=100)  # int input

        row = writer.writes[0]["rows"][0]
        assert isinstance(row.value, float)
        assert row.value == 100.0


class TestRecordDataframe:
    """Tests for TelemetryEmitter.record_dataframe()."""

    def test_record_dataframe_writes_per_metric_id(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        data = [
            (
                "MetricA",
                "feed_ingest",
                100.0,
                datetime(2026, 3, 4),
                "snap",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ),
            (
                "MetricA",
                "feed_ingest",
                200.0,
                datetime(2026, 3, 4),
                "snap",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ),
            (
                "MetricB",
                "feed_ingest",
                50.0,
                datetime(2026, 3, 4),
                "snap",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ),
        ]
        df = spark.createDataFrame(data, schema=METRIC_RECORD_SCHEMA)

        emitter.record_dataframe(df)

        # Should write twice — once for MetricA, once for MetricB
        assert len(writer.writes) == 2
        metric_ids = {w["metric_id"] for w in writer.writes}
        assert metric_ids == {"MetricA", "MetricB"}

        # MetricA should have 2 rows
        metric_a_write = next(w for w in writer.writes if w["metric_id"] == "MetricA")
        assert metric_a_write["count"] == 2

    def test_record_dataframe_enriches_missing_stage(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        # DataFrame without stage or snapshot columns
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )
        data = [("TestMetric", 100.0, datetime(2026, 3, 4))]
        df = spark.createDataFrame(data, schema=schema)

        emitter.record_dataframe(df)

        assert len(writer.writes) == 1
        row = writer.writes[0]["rows"][0]
        assert row.stage == "feed_ingest"
        assert row.snapshot == "snap"

    def test_record_dataframe_enriches_missing_snapshot(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="meta-places-2026-03-04",
            writer=writer,
        )

        # DataFrame with stage but no snapshot
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )
        data = [("TestMetric", "feed_ingest", 100.0, datetime(2026, 3, 4))]
        df = spark.createDataFrame(data, schema=schema)

        emitter.record_dataframe(df)

        row = writer.writes[0]["rows"][0]
        assert row.snapshot == "meta-places-2026-03-04"

    def test_record_dataframe_rejects_wrong_type(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            writer=writer,
        )

        # 'value' as StringType instead of DoubleType
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", StringType(), False),  # Wrong type
                StructField("timestamp", TimestampType(), False),
                StructField("snapshot", StringType(), False),
            ]
        )
        data = [
            ("TestMetric", "feed_ingest", "not_a_number", datetime(2026, 3, 4), "snap")
        ]
        df = spark.createDataFrame(data, schema=schema)

        with pytest.raises(SchemaValidationError, match="Column type mismatches"):
            emitter.record_dataframe(df)

    def test_record_dataframe_fills_null_optional_columns(self, spark: SparkSession):
        writer = MockWriter()
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            theme="places",
            writer=writer,
        )

        data = [
            (
                "TestMetric",
                "feed_ingest",
                100.0,
                datetime(2026, 3, 4),
                "snap",
                "places",
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ]
        df = spark.createDataFrame(data, schema=METRIC_RECORD_SCHEMA)

        emitter.record_dataframe(df)

        row = writer.writes[0]["rows"][0]
        # Null optional columns should be filled with empty strings
        assert row.location_id == ""
        assert row.primary_partition == ""
        assert row.sub_partition == ""
        assert row.metadata == ""
        assert row.baseline_snapshot == ""


class TestEnvironmentResolution:
    """Tests for environment parameter and table name resolution."""

    def test_explicit_environment(self, spark: SparkSession):
        emitter = TelemetryEmitter(
            spark=spark,
            stage="feed_ingest",
            snapshot="snap",
            environment="prod",
        )
        assert emitter.environment == "prod"
        assert (
            emitter.writer.table
            == "s3tables_catalog.pipeline_metrics.prod_pipeline_metrics"
        )

    def test_environment_from_spark_conf(self, spark: SparkSession):
        spark.conf.set("spark.overture.metrics.environment", "staging")
        try:
            emitter = TelemetryEmitter(
                spark=spark,
                stage="feed_ingest",
                snapshot="snap",
            )
            assert emitter.environment == "staging"
            assert (
                emitter.writer.table
                == "s3tables_catalog.pipeline_metrics.staging_pipeline_metrics"
            )
        finally:
            spark.conf.unset("spark.overture.metrics.environment")

    def test_environment_from_env_var(self, spark: SparkSession):
        with patch.dict(os.environ, {"OVERTURE_METRICS_ENVIRONMENT": "staging"}):
            emitter = TelemetryEmitter(
                spark=spark,
                stage="feed_ingest",
                snapshot="snap",
            )
            assert emitter.environment == "staging"
            assert (
                emitter.writer.table
                == "s3tables_catalog.pipeline_metrics.staging_pipeline_metrics"
            )

    def test_environment_defaults_to_dev(self, spark: SparkSession):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("OVERTURE_METRICS_ENVIRONMENT", None)
            emitter = TelemetryEmitter(
                spark=spark,
                stage="feed_ingest",
                snapshot="snap",
            )
            assert emitter.environment == "dev"
            assert (
                emitter.writer.table
                == "s3tables_catalog.pipeline_metrics.dev_pipeline_metrics"
            )

    def test_spark_conf_overrides_env_var(self, spark: SparkSession):
        spark.conf.set("spark.overture.metrics.environment", "prod")
        try:
            with patch.dict(os.environ, {"OVERTURE_METRICS_ENVIRONMENT": "staging"}):
                emitter = TelemetryEmitter(
                    spark=spark,
                    stage="feed_ingest",
                    snapshot="snap",
                )
                assert emitter.environment == "prod"
        finally:
            spark.conf.unset("spark.overture.metrics.environment")

    def test_explicit_environment_overrides_spark_conf(self, spark: SparkSession):
        spark.conf.set("spark.overture.metrics.environment", "staging")
        try:
            emitter = TelemetryEmitter(
                spark=spark,
                stage="feed_ingest",
                snapshot="snap",
                environment="prod",
            )
            assert emitter.environment == "prod"
        finally:
            spark.conf.unset("spark.overture.metrics.environment")
