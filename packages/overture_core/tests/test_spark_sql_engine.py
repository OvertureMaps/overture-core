"""Validate CREATE TABLE output with Spark's parser and a local Parquet table."""

import json

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from overture_core.spark_sql import SqlColumn, create_table_sql

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("overture-core-sql-test")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_parser_preserves_nullability_types_and_comments(spark):
    comment = "Provider's C:\\new\\data, café\nsecond line"
    sql = create_table_sql(
        "catalog.db.events",
        [
            SqlColumn("id", "STRING", nullable=False, comment=comment),
            SqlColumn("values", "ARRAY<DECIMAL(10, 2)>", comment=""),
        ],
    )
    parser = spark._jsparkSession.sessionState().sqlParser()
    parser.parsePlan(sql)
    # CREATE TABLE logical-plan fields differ between Spark 3 and 4.
    schema_sql = sql.split("(", 1)[1].rsplit(")\nUSING", 1)[0]
    fields = json.loads(parser.parseTableSchema(schema_sql).json())["fields"]
    assert fields[0] == {
        "name": "id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": comment},
    }
    assert fields[1]["nullable"] is True
    assert fields[1]["type"]["elementType"] == "decimal(10,2)"
    assert fields[1]["metadata"]["comment"] == ""


def test_create_table_with_caller_owned_clauses(spark, tmp_path):
    sql = create_table_sql(
        "shared_sql_test",
        [SqlColumn("id", "STRING"), SqlColumn("country", "STRING")],
        table_format="parquet",
        if_not_exists=True,
    )
    sql += f"\nPARTITIONED BY (country)\nLOCATION '{tmp_path.as_uri()}'"
    sql += "\nTBLPROPERTIES ('pipeline' = 'overture')"
    try:
        spark.sql(sql)
        spark.sql(sql)
        assert spark.table("shared_sql_test").columns == ["id", "country"]
        assert (
            spark.sql("SHOW TBLPROPERTIES shared_sql_test ('pipeline')").first()[1]
            == "overture"
        )
    finally:
        spark.sql("DROP TABLE IF EXISTS shared_sql_test")


@pytest.mark.parametrize("name", ["a b", "my-col", "a;b", "`ok` garbage", "`id`"])
def test_parser_preserves_literal_column_names(spark, name):
    sql = create_table_sql("events", [SqlColumn(name, "INT")])
    parser = spark._jsparkSession.sessionState().sqlParser()
    parser.parsePlan(sql)
    schema_sql = sql.split("(", 1)[1].rsplit(")\nUSING", 1)[0]
    fields = json.loads(parser.parseTableSchema(schema_sql).json())["fields"]
    assert [field["name"] for field in fields] == [name]
