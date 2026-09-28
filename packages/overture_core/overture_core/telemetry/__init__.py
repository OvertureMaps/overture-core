"""
overture_core.telemetry: Unified telemetry and metrics recording for Overture pipelines.

Provides a single interface for recording pipeline metrics across all stages
(feed ingest, matching, corpus load, merge) with pluggable storage backends.

Requires the ``telemetry`` extra (``pip install "overture-core[telemetry]"``) for
PySpark; the rest of overture_core does not import this subpackage.
"""

from overture_core.telemetry.record import MetricRecord, METRIC_RECORD_SCHEMA
from overture_core.telemetry.emitter import TelemetryEmitter
from overture_core.telemetry.writers import IcebergMetricsWriter, MetricsWriter

__all__ = [
    "MetricRecord",
    "METRIC_RECORD_SCHEMA",
    "TelemetryEmitter",
    "MetricsWriter",
    "IcebergMetricsWriter",
]
