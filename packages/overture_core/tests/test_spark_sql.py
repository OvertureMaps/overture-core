import subprocess
import sys

import pytest

from overture_core.spark_sql import SqlColumn, create_table_sql


def test_nullable_and_required_columns():
    sql = create_table_sql(
        "catalog.db.events",
        [
            SqlColumn("id", "STRING", nullable=False, comment="Source identifier"),
            SqlColumn("score", "DOUBLE"),
        ],
    )
    assert sql == (
        "CREATE TABLE catalog.db.events (\n"
        "  id STRING NOT NULL COMMENT 'Source identifier',\n"
        "  score DOUBLE\n)\nUSING ICEBERG"
    )


def test_caller_supplied_identifiers_types_and_format():
    sql = create_table_sql(
        "`my-catalog`.`my-db`.`events`",
        (
            SqlColumn(name, sql_type)
            for name, sql_type in [
                ("source id", "DECIMAL(10, 2)"),
                ("attributes", "MAP<STRING, ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>>"),
            ]
        ),
        table_format="parquet",
        if_not_exists=True,
    )
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `my-catalog`.`my-db`.`events`")
    assert "`source id` DECIMAL(10, 2)" in sql
    assert "MAP<STRING, ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>>" in sql
    assert sql.endswith("USING PARQUET")


@pytest.mark.parametrize(
    "sql_type", ["STRING NOT NULL", "STRING COMMENT 'x'", "INT, x INT", "INT; INT"]
)
def test_invalid_sql_type_is_rejected(sql_type):
    with pytest.raises(ValueError, match="Invalid sql_type"):
        create_table_sql("events", [SqlColumn("id", sql_type)])


@pytest.mark.parametrize(
    "table_format",
    [
        "iceberg; DROP TABLE x",
        "iceberg LOCATION 's3://x'",
        "org.apache.spark.sql.parquet",
        " iceberg",
        "iceberg ",
        "1parquet",
    ],
)
def test_invalid_table_format_is_rejected(table_format):
    with pytest.raises(ValueError, match="short provider name"):
        create_table_sql(
            "events", [SqlColumn("id", "STRING")], table_format=table_format
        )


@pytest.mark.parametrize("table_format", ["iceberg", "parquet", "delta", "custom_v2"])
def test_short_provider_names_are_supported(table_format):
    sql = create_table_sql(
        "events", [SqlColumn("id", "STRING")], table_format=table_format
    )
    assert sql.endswith(f"USING {table_format.upper()}")


@pytest.mark.parametrize("name", ["a b", "my-col", "a;b", "`ok` garbage", "`id`"])
def test_column_names_are_literal_text(name):
    sql = create_table_sql("events", [SqlColumn(name, "INT")])
    quoted_name = "`" + name.replace("`", "``") + "`"
    assert f"  {quoted_name} INT\n" in sql


def test_comments_escape_quotes_and_backslashes():
    sql = create_table_sql(
        "events",
        [
            SqlColumn("id", "STRING", comment="Provider's C:\\new\\data"),
            SqlColumn("value", "INT", comment=""),
        ],
    )
    assert "COMMENT 'Provider\\'s C:\\\\new\\\\data'" in sql
    assert "value INT COMMENT ''" in sql


@pytest.mark.parametrize("columns", [[], iter(())])
def test_empty_schema_is_rejected(columns):
    with pytest.raises(ValueError, match="At least one column"):
        create_table_sql("events", columns)


@pytest.mark.parametrize(
    "table,format", [("", "iceberg"), ("  ", "iceberg"), ("t", "")]
)
def test_empty_table_or_format_is_rejected(table, format):
    with pytest.raises(ValueError, match="must not be empty"):
        create_table_sql(table, [SqlColumn("id", "STRING")], table_format=format)


@pytest.mark.parametrize("name,sql_type", [("", "STRING"), ("id", " ")])
def test_empty_column_name_or_type_is_rejected(name, sql_type):
    with pytest.raises(ValueError, match="must not be empty"):
        create_table_sql("events", [SqlColumn(name, sql_type)])


def test_sql_generation_does_not_import_spark_or_arrow():
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys\n"
            "sys.modules['pyspark'] = None\n"
            "sys.modules['pyarrow'] = None\n"
            "from overture_core.spark_sql import SqlColumn, create_table_sql\n"
            "assert 'id STRING' in create_table_sql('events', [SqlColumn('id', 'STRING')])",
        ],
        check=True,
    )
