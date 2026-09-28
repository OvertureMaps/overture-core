import os
import sys
from pathlib import Path

import pytest
from collections.abc import Generator

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None
    # Lets a plain `pytest` run with only the `dev` extra skip this directory
    # instead of failing collection on the pyspark imports.
    collect_ignore_glob = ["test_*.py"]

TESTS_DIR = str(Path(__file__).parent)

# Ensure PySpark workers use the same Python as the test runner.
# This prevents mismatches when multiple Python versions are installed.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def pytest_collection_modifyitems(items):
    for item in items:
        if TESTS_DIR in str(item.path.parent):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Session-scoped Spark fixture.

    Uses a plain SparkSession (no Sedona) since telemetry tests only need
    basic DataFrame operations — no geospatial functions.
    """
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("overture-core-telemetry-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.executorEnv.PYTHONPATH", TESTS_DIR)
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
