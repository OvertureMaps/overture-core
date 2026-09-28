"""
MetricRecord data structure and Spark schema for standardized metrics output.

Every metric — regardless of stage, theme, or producer — is stored as a MetricRecord.
This module provides:
- MetricRecord: Python dataclass for type-safe metric construction
- METRIC_RECORD_SCHEMA: Spark StructType for DataFrame validation
- validate_dataframe(): Validates a DataFrame conforms to the MetricRecord schema
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass
class MetricRecord:
    """
    Standardized output format for all pipeline metrics.

    Attributes:
        metric_id: What's being measured (e.g., "IngestInputCount", "MatchReductionRate").
            Defined as enums/constants in consumer repos.
        stage: Pipeline stage that produced the metric.
            One of: "feed_ingest", "matching", "corpus_load", "merge".
        value: The numeric metric value (count, percentage, rate, etc.)
        timestamp: When the metric was calculated.
        snapshot: Dataset + version identifier per RFD #82
            (e.g., "meta-places-2026-03-04").
        theme: Theme the data belongs to (e.g., "places", "addresses").
        type: Specific type within a theme (e.g., "place", "division").
        location_id: Geographic identifier (country, H3 cell) if applicable.
        primary_partition: Primary slicing dimension. Meaning depends on the metric.
            For per-provider stages, typically the dataset (e.g., "meta-places").
        sub_partition: Secondary slicing dimension. Meaning depends on the metric.
            (e.g., filter name, confidence bucket, match sub-type).
        metadata: Additional context about the metric (if any).
        baseline_snapshot: Snapshot identifier of the baseline run this metric
            was compared against, if applicable. Empty/null for metrics that
            don't involve a baseline comparison.
    """

    metric_id: str
    stage: str
    value: float
    timestamp: datetime
    snapshot: str
    theme: Optional[str] = None
    type: Optional[str] = None
    location_id: Optional[str] = None
    primary_partition: Optional[str] = None
    sub_partition: Optional[str] = None
    metadata: Optional[str] = None
    baseline_snapshot: Optional[str] = None


# Column ordering matches the dataclass field order for consistency.
METRIC_RECORD_SCHEMA = StructType(
    [
        StructField("metric_id", StringType(), nullable=False),
        StructField("stage", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("snapshot", StringType(), nullable=False),
        StructField("theme", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("location_id", StringType(), nullable=True),
        StructField("primary_partition", StringType(), nullable=True),
        StructField("sub_partition", StringType(), nullable=True),
        StructField("metadata", StringType(), nullable=True),
        StructField("baseline_snapshot", StringType(), nullable=True),
    ]
)
"""Spark StructType for the MetricRecord schema. Use for DataFrame creation and validation."""

# Required columns that must be present and non-nullable
REQUIRED_COLUMNS = {"metric_id", "stage", "value", "timestamp", "snapshot"}

# All columns in the schema
ALL_COLUMNS = {field.name for field in METRIC_RECORD_SCHEMA.fields}

# Expected Spark types for each column (for validation)
_EXPECTED_TYPES = {
    field.name: type(field.dataType) for field in METRIC_RECORD_SCHEMA.fields
}


class SchemaValidationError(Exception):
    """Raised when a DataFrame does not conform to the MetricRecord schema."""

    pass


def validate_dataframe(df: DataFrame) -> None:
    """
    Validate that a DataFrame conforms to the MetricRecord schema.

    Checks:
    - All required columns are present
    - No unexpected columns exist
    - Column types match the expected schema

    Args:
        df: The DataFrame to validate

    Raises:
        SchemaValidationError: If the DataFrame does not conform to the schema
    """
    df_columns = set(df.columns)
    df_schema = {field.name: type(field.dataType) for field in df.schema.fields}

    # Check for missing required columns
    missing = REQUIRED_COLUMNS - df_columns
    if missing:
        raise SchemaValidationError(f"Missing required columns: {sorted(missing)}")

    # Check for unexpected columns
    unexpected = df_columns - ALL_COLUMNS
    if unexpected:
        raise SchemaValidationError(
            f"Unexpected columns: {sorted(unexpected)}. "
            f"Expected columns: {sorted(ALL_COLUMNS)}"
        )

    # Check column types for columns that are present
    type_mismatches = []
    for col_name in df_columns:
        expected_type = _EXPECTED_TYPES[col_name]
        actual_type = df_schema[col_name]
        if actual_type != expected_type:
            type_mismatches.append(
                f"Column '{col_name}': expected {expected_type.__name__}, "
                f"got {actual_type.__name__}"
            )

    if type_mismatches:
        raise SchemaValidationError(
            f"Column type mismatches: {'; '.join(type_mismatches)}"
        )
