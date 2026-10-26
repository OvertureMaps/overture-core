"""hadoop_fs goes through Spark's Hadoop FileSystem, exercised here on local paths."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from overture_spark.hadoop_fs import delete_prefix, list_files, read_text

try:
    import pyspark  # noqa: F401
except ImportError:
    pyspark = None

needs_spark = pytest.mark.skipif(
    pyspark is None, reason="pyspark not installed; requires the sql-spark extra"
)


@pytest.fixture(scope="module")
def spark():
    from overture_spark import SparkPlatform, getSparkSedonaSession

    session = getSparkSedonaSession(
        spark_platform=SparkPlatform.LOCAL, app_name="HadoopFs"
    )
    yield session
    session.stop()


def _rel(uris: list[str], root: Path) -> list[str]:
    return [u.split(f"/{root.name}/", 1)[1] for u in uris]


@pytest.mark.spark
@needs_spark
def test_read_text(spark, tmp_path: Path):
    f = tmp_path / "inputs.json"
    f.write_text('{"address": ["s3://b/ü"]}', encoding="utf-8")

    assert read_text(spark, str(f)) == '{"address": ["s3://b/ü"]}'


@pytest.mark.spark
@needs_spark
def test_list_files_is_recursive_and_sorted(spark, tmp_path: Path):
    for rel in ["b/part-1.parquet", "a/x/part-0.parquet", "_SUCCESS"]:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(b"")

    assert _rel(list_files(spark, str(tmp_path)), tmp_path) == [
        "_SUCCESS",
        "a/x/part-0.parquet",
        "b/part-1.parquet",
    ]


@pytest.mark.spark
@needs_spark
def test_list_files_missing_path_is_empty(spark, tmp_path: Path):
    assert list_files(spark, str(tmp_path / "nope")) == []


@pytest.mark.spark
@needs_spark
def test_delete_prefix_is_recursive_and_tolerates_missing(spark, tmp_path: Path):
    (tmp_path / "x" / "y").mkdir(parents=True)
    (tmp_path / "x" / "y" / "f").write_bytes(b"")
    (tmp_path / "keep").write_bytes(b"")

    delete_prefix(spark, str(tmp_path / "x"))
    delete_prefix(spark, str(tmp_path / "x"))

    assert [p.name for p in tmp_path.iterdir()] == ["keep"]


@pytest.mark.parametrize("op", [read_text, list_files, delete_prefix])
@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("s3://bkt/tmp/x", "s3a://bkt/tmp/x"),
        ("s3a://bkt/tmp/x", "s3a://bkt/tmp/x"),
        ("/tmp/x", "/tmp/x"),
    ],
)
def test_s3_uris_use_s3a(op, uri, expected):
    spark = MagicMock(name="spark")
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path
    # A missing path, so list_files returns without iterating the mock.
    hadoop_path.return_value.getFileSystem.return_value.exists.return_value = False

    op(spark, uri)

    hadoop_path.assert_called_once_with(expected)
