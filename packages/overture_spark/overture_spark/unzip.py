"""Extract a ZIP archive on S3 to another S3 prefix, one entry per object.

Uses boto3 rather than Hadoop FS, so a multi-GB archive isn't copied through
py4j. Deflate64 entries (Windows-built archives such as NAD r24) need the
``zip`` extra (``overture-spark[zip]``); ``boto3`` is imported lazily.
"""

from __future__ import annotations

import os
import tempfile
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from overture_core.cloud.aws.object import parse_s3_uri
from overture_core.deflate64 import enable_zipfile_deflate64
from overture_core.file_io import NonSeekableReader

from overture_spark.hadoop_fs import delete_prefix
from overture_spark.s3 import resolve_s3_access, s3_client

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_MIB = 1024 * 1024
# Entries are streamed, so the part size caps the entry size at 10,000 parts.
_PART_SIZE = 64 * _MIB


def _parse(uri: str) -> tuple[str, str]:
    return parse_s3_uri(uri.replace("s3a://", "s3://", 1))


def unzip_to_s3(
    spark: SparkSession,
    s3_zip_uri: str,
    dest_s3_prefix: str,
    entry_filter: Callable[[str], bool] | None = None,
    max_workers: int = 16,
) -> str:
    """Extract a ZIP from S3 to ``dest_s3_prefix``.

    Downloads the archive once, then uploads each entry from its own thread.
    Entries are wrapped in :class:`~overture_core.file_io.NonSeekableReader` so
    ``upload_fileobj`` streams them in one pass instead of decompressing each
    one twice to learn its size. Deflate64 entries are supported.

    Source and destination may use different S3A roles, so each gets its own
    client. ``dest_s3_prefix`` is deleted first: the caller owns its cleanup
    afterwards and must not share it between extractions.

    Args:
        spark: Active session, used to resolve S3A credentials and clear the prefix.
        s3_zip_uri: ``s3://`` or ``s3a://`` URI of the ZIP.
        dest_s3_prefix: ``s3://`` or ``s3a://`` prefix to extract into.
        entry_filter: Predicate on entry names; ``None`` extracts every file.
            Directory entries are always skipped.
        max_workers: Number of concurrent uploads.

    Returns:
        ``dest_s3_prefix``.

    Raises:
        ImportError: If the ``compression`` extra (``inflate64``) isn't installed.
        ValueError: If a URI isn't an S3 URI.
    """
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config

    enable_zipfile_deflate64()

    src_bucket, src_key = _parse(s3_zip_uri)
    dst_bucket, dst_prefix = _parse(dest_s3_prefix)
    src_s3 = s3_client(resolve_s3_access(spark, s3_zip_uri))
    s3 = s3_client(
        resolve_s3_access(spark, dest_s3_prefix),
        config=Config(max_pool_connections=max_workers),
    )
    transfer_config = TransferConfig(
        multipart_threshold=_PART_SIZE,
        multipart_chunksize=_PART_SIZE,
        max_concurrency=1,
    )
    # Non-seekable streams buffer parts in memory; hold one at a time per thread.
    transfer_config.max_in_memory_upload_chunks = 1

    delete_prefix(spark, dest_s3_prefix)

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        src_s3.download_file(src_bucket, src_key, tmp_path)

        with zipfile.ZipFile(tmp_path) as zf:
            entries = [
                n
                for n in zf.namelist()
                if (entry_filter is None or entry_filter(n)) and not n.endswith("/")
            ]

        def _upload(name: str) -> None:
            # A ZipFile handle isn't thread-safe, so each thread opens its own.
            with zipfile.ZipFile(tmp_path) as thread_zf, thread_zf.open(name) as entry:
                s3.upload_fileobj(
                    NonSeekableReader(entry),
                    dst_bucket,
                    f"{dst_prefix.rstrip('/')}/{name}",
                    Config=transfer_config,
                )

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # Consuming the iterator re-raises the first failure.
            list(pool.map(_upload, entries))
    finally:
        os.unlink(tmp_path)

    return dest_s3_prefix
