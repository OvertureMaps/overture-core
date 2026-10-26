"""Small driver-side S3 operations through Spark's Hadoop FileSystem.

These use whatever credentials S3A is configured with on the Spark platform.
On Wherobots, S3A is the only client that gets the bucket's role.

``s3://`` URIs are read as ``s3a://``; other URIs, including local paths, go to
whichever Hadoop FileSystem handles their scheme. For small files only: py4j
copies every byte through the driver. For Python clients on workers, see
:mod:`overture_spark.s3`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _path_and_fs(spark: SparkSession, uri: str):
    if uri.startswith("s3://"):
        uri = "s3a://" + uri[len("s3://") :]
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path, path.getFileSystem(spark._jsc.hadoopConfiguration())


def read_text(spark: SparkSession, uri: str) -> str:
    """The file at ``uri``, decoded as UTF-8."""
    path, fs = _path_and_fs(spark, uri)
    jvm = spark._jvm
    buf = jvm.java.io.ByteArrayOutputStream()
    # close=True closes both streams, including on failure.
    jvm.org.apache.hadoop.io.IOUtils.copyBytes(fs.open(path), buf, 65536, True)
    return buf.toString("UTF-8")


def list_files(spark: SparkSession, uri: str) -> list[str]:
    """Sorted URIs of every file under ``uri``; empty if it doesn't exist."""
    path, fs = _path_and_fs(spark, uri)
    if not fs.exists(path):
        return []
    files = []
    it = fs.listFiles(path, True)
    while it.hasNext():
        files.append(it.next().getPath().toString())
    return sorted(files)


def delete_prefix(spark: SparkSession, uri: str) -> None:
    """Delete ``uri`` and everything under it; a no-op if it doesn't exist.

    On S3A, removing the last object under a parent recreates the parent's
    empty directory marker (a zero-byte ``parent/`` object), so the parent
    still exists afterwards.
    """
    path, fs = _path_and_fs(spark, uri)
    fs.delete(path, True)
