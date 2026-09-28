"""Tests for IcebergMetricsWriter.

Asserts on the SQL and DataFrameWriterV2 calls the writer issues, against a
mocked SparkSession: a real write needs the Iceberg runtime jars and a
registered ``s3tables_catalog``, neither of which the test fixture has. The
tests taking the `spark` fixture only need an active SparkContext for col().
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from overture_core.telemetry.writers.iceberg import (
    NAMESPACE,
    S3TABLES_CATALOG,
    IcebergMetricsWriter,
    ensure_table_exists,
    migrate_partition_spec,
    table_name,
)


_TABLE = "s3tables_catalog.pipeline_metrics.dev_pipeline_metrics"


def _spark_with_partition_fields(*names):
    """SparkSession whose `.partitions` metadata table reports `names`."""
    spark = MagicMock()
    spark.table.return_value.schema.__getitem__.return_value.dataType.fields = [
        SimpleNamespace(name=name) for name in names
    ]
    return spark


def test_table_name_matches_existing_glue_table_naming():
    assert (
        table_name("prod") == "s3tables_catalog.pipeline_metrics.prod_pipeline_metrics"
    )
    assert table_name("dev") == "s3tables_catalog.pipeline_metrics.dev_pipeline_metrics"


def test_table_name_accepts_catalog_override():
    """A cross-environment secondary catalog registered via extra_spark_conf
    should be usable in place of the default s3tables_catalog."""
    assert (
        table_name("prod", catalog="s3tables_prod_catalog")
        == "s3tables_prod_catalog.pipeline_metrics.prod_pipeline_metrics"
    )


@pytest.mark.parametrize("environment", ["dev; DROP TABLE x", "dev.staging", "", "DEV"])
def test_table_name_rejects_unsafe_environment_values(environment):
    with pytest.raises(ValueError):
        table_name(environment)


def test_writer_resolves_table_from_environment():
    writer = IcebergMetricsWriter(environment="staging")
    assert writer.table == f"{S3TABLES_CATALOG}.{NAMESPACE}.staging_pipeline_metrics"


def test_writer_accepts_catalog_override():
    writer = IcebergMetricsWriter(
        environment="staging", catalog="s3tables_prod_catalog"
    )
    assert (
        writer.table
        == "s3tables_prod_catalog.pipeline_metrics.staging_pipeline_metrics"
    )


def test_ensure_table_exists_issues_create_table_ddl():
    spark = MagicMock()
    ensure_table_exists(spark, _TABLE)

    (ddl,), _ = spark.sql.call_args
    assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS {_TABLE} (")
    assert "USING iceberg PARTITIONED BY (stage, metric_id, snapshot)" in ddl
    # Every MetricRecord column must be declared.
    for column in (
        "metric_id STRING",
        "stage STRING",
        "value DOUBLE",
        "timestamp TIMESTAMP",
        "snapshot STRING",
        "baseline_snapshot STRING",
    ):
        assert column in ddl


# An already-migrated table must issue nothing: re-adding the field errors,
# and the rewrite is expensive.
@pytest.mark.parametrize(
    "existing_fields, expected_sql",
    [
        (
            ("stage", "metric_id"),
            [
                f"ALTER TABLE {_TABLE} ADD PARTITION FIELD snapshot",
                "CALL s3tables_catalog.system.rewrite_data_files("
                "table => 'pipeline_metrics.dev_pipeline_metrics', "
                "options => map('rewrite-all', 'true'))",
            ],
        ),
        (("stage", "metric_id", "snapshot"), []),
    ],
)
def test_migrate_partition_spec(existing_fields, expected_sql):
    spark = _spark_with_partition_fields(*existing_fields)

    assert migrate_partition_spec(spark, _TABLE) is bool(expected_sql)
    assert [call.args[0] for call in spark.sql.call_args_list] == expected_sql


def test_write_overwrites_matching_rows_in_a_single_atomic_commit(spark):
    """`spark` (the real local SparkSession fixture) is only needed so
    F.col() has an active SparkContext to build against; the mocked
    `spark_conn` below is what the writer actually calls .sql()/.writeTo() on.
    """
    spark_conn = MagicMock()
    df = MagicMock()
    writer = IcebergMetricsWriter(environment="dev")

    writer.write(
        spark_conn,
        df,
        stage="feed_ingest",
        metric_id="IngestInputCount",
        snapshot="snap-1",
    )

    # Only the CREATE TABLE IF NOT EXISTS DDL goes through spark.sql; the
    # write itself is a single writeTo(...).overwrite(condition) call rather
    # than a separate DELETE followed by append (two commits would leave a
    # window where a mid-write failure loses or duplicates rows).
    spark_conn.sql.assert_called_once()
    assert "CREATE TABLE IF NOT EXISTS" in spark_conn.sql.call_args.args[0]

    df.writeTo.assert_called_once_with(writer.table)
    df.writeTo.return_value.overwrite.assert_called_once()
    condition = df.writeTo.return_value.overwrite.call_args.args[0]
    condition_sql = str(condition)
    for expected in (
        "stage",
        "feed_ingest",
        "metric_id",
        "IngestInputCount",
        "snapshot",
        "snap-1",
    ):
        assert expected in condition_sql


def test_write_does_not_drop_partition_key_columns(spark):
    """stage/metric_id/snapshot must stay as real columns: Iceberg has no
    directory-based partition inference."""
    spark_conn = MagicMock()
    df = MagicMock()
    writer = IcebergMetricsWriter(environment="dev")

    writer.write(
        spark_conn, df, stage="feed_ingest", metric_id="Metric", snapshot="snap"
    )

    df.drop.assert_not_called()
