"""Integration tests for the SQL UUID generators against real engines.

Each class is skipped automatically (via a module-level import check feeding
`pytest.mark.skipif`) unless its engine client is installed, so a normal
`pytest` run using only the `dev` extra never attempts these; they need a
real JVM (Spark) or a running Trino coordinator, which is what the
"sql-engines" CI workflow provides. That workflow installs
`overture-core[sql-spark]` / `overture-core[sql-trino]` and runs each half
with `pytest -m spark` / `pytest -m trino` in its own job, and only triggers
on changes touching `uuids_sql.py` or this file, so the JVM/Docker cost is
paid only when it is actually relevant. The two classes skip independently:
one engine's client being installed doesn't require the other's, since a
module-level `pytest.importorskip` would abort collection of both classes
the moment either import is missing.
"""

import os
import uuid

import pytest

from overture_core.uuids import generate_uuid3, generate_uuid5
from overture_core.uuids_sql import (
    generate_uuid3_sql,
    generate_uuid4_sql,
    generate_uuid5_sql,
)

NAMESPACE_DNS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
NAMES = ["example.com", "overture maps", "", "unicode-\u00e9\u00e8-name"]

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None


@pytest.mark.spark
@pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; skipping Spark engine tests"
)
class TestSparkEngine:
    """Runs the generated Spark-dialect SQL against a real local Spark session."""

    @classmethod
    @pytest.fixture(scope="class")
    def spark(cls):
        from pyspark.sql import SparkSession

        session = (
            SparkSession.builder.appName("overture-core-uuids-sql-test")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        yield session
        session.stop()

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid3_sql_matches_generate_uuid3(self, spark, name):
        expected = generate_uuid3(NAMESPACE_DNS, name)
        df = spark.createDataFrame([(name,)], ["name_col"])
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name_col", engine="spark")
        assert df.selectExpr(sql).first()[0] == expected

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid5_sql_matches_generate_uuid5(self, spark, name):
        expected = generate_uuid5(NAMESPACE_DNS, name)
        df = spark.createDataFrame([(name,)], ["name_col"])
        sql = generate_uuid5_sql(NAMESPACE_DNS, "name_col", engine="spark")
        assert df.selectExpr(sql).first()[0] == expected

    def test_uuid4_sql_produces_valid_v4_uuid(self, spark):
        sql = generate_uuid4_sql(engine="spark")
        got = spark.range(1).selectExpr(sql).first()[0]
        assert uuid.UUID(got).version == 4


try:
    import trino  # noqa: F401
except ImportError:
    trino = None


@pytest.mark.trino
@pytest.mark.skipif(
    trino is None, reason="trino client not installed; skipping Trino engine tests"
)
class TestTrinoEngine:
    """Runs the generated Trino-dialect SQL against a real Trino coordinator.

    Connects to TRINO_HOST/TRINO_PORT (defaults to localhost:8080, matching
    the trinodb/trino service container the sql-engines CI workflow runs).
    """

    @classmethod
    @pytest.fixture(scope="class")
    def cursor(cls):
        from trino.dbapi import connect

        conn = connect(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="overture-core-ci",
            catalog="system",
            schema="runtime",
        )
        cur = conn.cursor()
        yield cur
        conn.close()

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid3_sql_matches_generate_uuid3(self, cursor, name):
        expected = generate_uuid3(NAMESPACE_DNS, name)
        sql = generate_uuid3_sql(NAMESPACE_DNS, "name_col", engine="trino")
        cursor.execute(f"select {sql} from (values (?)) as t(name_col)", [name])
        assert cursor.fetchone()[0] == expected

    @pytest.mark.parametrize("name", NAMES)
    def test_uuid5_sql_matches_generate_uuid5(self, cursor, name):
        expected = generate_uuid5(NAMESPACE_DNS, name)
        sql = generate_uuid5_sql(NAMESPACE_DNS, "name_col", engine="trino")
        cursor.execute(f"select {sql} from (values (?)) as t(name_col)", [name])
        assert cursor.fetchone()[0] == expected

    def test_uuid4_sql_produces_valid_v4_uuid(self, cursor):
        cursor.execute(f"select {generate_uuid4_sql(engine='trino')}")
        assert uuid.UUID(cursor.fetchone()[0]).version == 4
