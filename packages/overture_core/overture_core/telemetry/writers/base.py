"""
Abstract base class for metrics writers.

Writers are responsible for persisting MetricRecord DataFrames to a storage backend.
The TelemetryEmitter delegates all write operations to a writer instance, making
the storage backend pluggable.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class MetricsWriter(ABC):
    """
    Abstract interface for writing metric DataFrames to storage.

    Implementations handle the specifics of where and how metrics are persisted
    (e.g., Parquet on S3, Iceberg tables, data warehouse).
    """

    @abstractmethod
    def write(
        self,
        spark: SparkSession,
        df: DataFrame,
        stage: str,
        metric_id: str,
        snapshot: str,
    ) -> None:
        """
        Write a DataFrame of MetricRecords to storage.

        Args:
            spark: Active SparkSession
            df: DataFrame conforming to MetricRecord schema
            stage: Pipeline stage (e.g., "feed_ingest", "matching")
            metric_id: The metric being recorded (e.g., "IngestInputCount")
            snapshot: Dataset + version identifier (e.g., "meta-places-2026-03-04")
        """
        pass
