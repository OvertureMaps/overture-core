"""Tests for the MetricRecord schema and validation."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from overture_core.telemetry.record import (
    ALL_COLUMNS,
    METRIC_RECORD_SCHEMA,
    REQUIRED_COLUMNS,
    MetricRecord,
    SchemaValidationError,
    validate_dataframe,
)


class TestMetricRecordDataclass:
    """Tests for the MetricRecord dataclass."""

    def test_create_with_all_fields(self):
        record = MetricRecord(
            metric_id="IngestInputCount",
            stage="feed_ingest",
            value=1_250_000.0,
            timestamp=datetime(2026, 3, 4, 14, 30),
            snapshot="meta-places-2026-03-04",
            theme="places",
            type="place",
            location_id="US",
            primary_partition="meta-places",
            sub_partition="invalid_geometry",
            metadata="test",
            baseline_snapshot="release_2026-02-01",
        )
        assert record.metric_id == "IngestInputCount"
        assert record.stage == "feed_ingest"
        assert record.value == 1_250_000.0
        assert record.snapshot == "meta-places-2026-03-04"
        assert record.theme == "places"
        assert record.type == "place"
        assert record.location_id == "US"
        assert record.primary_partition == "meta-places"
        assert record.sub_partition == "invalid_geometry"
        assert record.metadata == "test"
        assert record.baseline_snapshot == "release_2026-02-01"

    def test_create_with_required_fields_only(self):
        record = MetricRecord(
            metric_id="IngestInputCount",
            stage="feed_ingest",
            value=100.0,
            timestamp=datetime(2026, 3, 4),
            snapshot="meta-places-2026-03-04",
        )
        assert record.metric_id == "IngestInputCount"
        assert record.theme is None
        assert record.type is None
        assert record.location_id is None
        assert record.primary_partition is None
        assert record.sub_partition is None
        assert record.metadata is None
        assert record.baseline_snapshot is None


class TestMetricRecordSchema:
    """Tests for the METRIC_RECORD_SCHEMA StructType."""

    def test_schema_has_correct_fields(self):
        field_names = {f.name for f in METRIC_RECORD_SCHEMA.fields}
        expected = {
            "metric_id",
            "stage",
            "value",
            "timestamp",
            "snapshot",
            "theme",
            "type",
            "location_id",
            "primary_partition",
            "sub_partition",
            "metadata",
            "baseline_snapshot",
        }
        assert field_names == expected

    def test_required_columns_are_non_nullable(self, subtests):
        # subtests (built into pytest 9) reports every offending field instead
        # of stopping at the first failure, unlike a plain loop of asserts.
        for field in METRIC_RECORD_SCHEMA.fields:
            if field.name in REQUIRED_COLUMNS:
                with subtests.test(field=field.name):
                    assert not field.nullable, (
                        f"Required column '{field.name}' should be non-nullable"
                    )

    def test_optional_columns_are_nullable(self, subtests):
        optional = ALL_COLUMNS - REQUIRED_COLUMNS
        for field in METRIC_RECORD_SCHEMA.fields:
            if field.name in optional:
                with subtests.test(field=field.name):
                    assert field.nullable, (
                        f"Optional column '{field.name}' should be nullable"
                    )

    def test_schema_field_types(self):
        type_map = {f.name: type(f.dataType) for f in METRIC_RECORD_SCHEMA.fields}
        assert type_map["metric_id"] == StringType
        assert type_map["stage"] == StringType
        assert type_map["value"] == DoubleType
        assert type_map["timestamp"] == TimestampType
        assert type_map["snapshot"] == StringType
        assert type_map["theme"] == StringType
        assert type_map["primary_partition"] == StringType
        assert type_map["sub_partition"] == StringType

    def test_schema_matches_dataclass_fields(self):
        """Verify the Spark schema covers all dataclass fields and vice versa."""
        import dataclasses

        dataclass_fields = {f.name for f in dataclasses.fields(MetricRecord)}
        schema_fields = {f.name for f in METRIC_RECORD_SCHEMA.fields}
        assert dataclass_fields == schema_fields


class TestValidateDataframe:
    """Tests for validate_dataframe()."""

    def test_valid_dataframe_passes(self, spark: SparkSession):
        data = [
            (
                "IngestInputCount",
                "feed_ingest",
                1000.0,
                datetime(2026, 3, 4),
                "meta-places-2026-03-04",
                "places",
                "place",
                "",
                "meta-places",
                "",
                "",
                "",
            )
        ]
        df = spark.createDataFrame(data, schema=METRIC_RECORD_SCHEMA)
        # Should not raise
        validate_dataframe(df)

    def test_missing_required_column_raises(self, spark: SparkSession):
        # DataFrame missing 'snapshot'
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
            ]
        )
        data = [("IngestInputCount", "feed_ingest", 1000.0, datetime(2026, 3, 4))]
        df = spark.createDataFrame(data, schema=schema)

        with pytest.raises(SchemaValidationError, match="Missing required columns"):
            validate_dataframe(df)

    def test_unexpected_column_raises(self, spark: SparkSession):
        data = [
            (
                "IngestInputCount",
                "feed_ingest",
                1000.0,
                datetime(2026, 3, 4),
                "meta-places-2026-03-04",
                "extra_value",
            )
        ]
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("snapshot", StringType(), False),
                StructField("bad_column", StringType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema=schema)

        with pytest.raises(SchemaValidationError, match="Unexpected columns"):
            validate_dataframe(df)

    def test_wrong_column_type_raises(self, spark: SparkSession):
        # 'value' as LongType instead of DoubleType
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", LongType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("snapshot", StringType(), False),
            ]
        )
        data = [("IngestInputCount", "feed_ingest", 1000, datetime(2026, 3, 4), "snap")]
        df = spark.createDataFrame(data, schema=schema)

        with pytest.raises(SchemaValidationError, match="Column type mismatches"):
            validate_dataframe(df)

    def test_subset_of_optional_columns_passes(self, spark: SparkSession):
        """DataFrame with only required columns should pass validation."""
        schema = StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("stage", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("snapshot", StringType(), False),
            ]
        )
        data = [
            ("IngestInputCount", "feed_ingest", 1000.0, datetime(2026, 3, 4), "snap")
        ]
        df = spark.createDataFrame(data, schema=schema)
        # Should not raise — optional columns are allowed to be absent
        validate_dataframe(df)
