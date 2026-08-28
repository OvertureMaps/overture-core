"""S3 object and prefix utilities built on boto3."""

from dataclasses import dataclass
from urllib.parse import urlparse

import boto3
import botocore.exceptions
from boto3.s3.transfer import TransferConfig

# 256 MB parts and 10 concurrent threads gives good throughput for large-file
# copies without saturating a task's network allocation.
_MULTIPART_CHUNK_BYTES = 256 * 1024 * 1024
_DEFAULT_MAX_CONCURRENCY = 10


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``.

    Raises:
        ValueError: If *s3_uri* isn't an ``s3://`` URI.
    """
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Not an S3 URI: {s3_uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def build_s3_uri(bucket: str, key: str) -> str:
    """Build an ``s3://bucket/key`` URI from a bucket and key."""
    return f"s3://{bucket}/{key.lstrip('/')}"


def object_exists(bucket: str, key: str) -> bool:
    """Return whether an object exists at ``bucket/key``."""
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise


def delete_object(bucket: str, key: str) -> None:
    """Delete an object at ``bucket/key``.

    S3's ``DeleteObject`` is idempotent — it returns success whether or not
    the key existed, so this is a plain no-op-safe delete rather than a
    check-then-act (checking first with :func:`object_exists` would cost an
    extra round trip and a TOCTOU race for no benefit).
    """
    boto3.client("s3").delete_object(Bucket=bucket, Key=key)


def write_marker(bucket: str, key: str) -> None:
    """Write an empty marker object (e.g. a ``_SUCCESS`` file) to ``bucket/key``."""
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=b"")


@dataclass(frozen=True)
class CopyResult:
    """Outcome of a :func:`copy_prefix` call."""

    files_copied: int
    total_bytes: int


def copy_prefix(
    source_bucket: str,
    source_prefix: str,
    destination_bucket: str,
    destination_prefix: str,
    *,
    max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
) -> CopyResult:
    """Copy every object under *source_prefix* to *destination_prefix*.

    Uses boto3's managed multipart copy so large objects transfer in
    parallel chunks. Prefixes are compared with their leading/trailing
    slashes stripped; directory-marker keys (ending in ``/``) are skipped.

    Args:
        source_bucket: Bucket to copy objects from.
        source_prefix: Prefix under *source_bucket* to copy.
        destination_bucket: Bucket to copy objects into.
        destination_prefix: Prefix under *destination_bucket* to copy into.
        max_concurrency: Concurrent copy threads per object.

    Returns:
        A :class:`CopyResult` with the number of files and bytes copied.
    """
    s3 = boto3.client("s3")
    transfer_config = TransferConfig(
        multipart_threshold=_MULTIPART_CHUNK_BYTES,
        multipart_chunksize=_MULTIPART_CHUNK_BYTES,
        max_concurrency=max_concurrency,
    )

    source_prefix = source_prefix.strip("/")
    destination_prefix = destination_prefix.strip("/")

    files_copied = 0
    total_bytes = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=source_bucket, Prefix=f"{source_prefix}/"):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            if source_key.endswith("/"):
                continue

            relative_key = source_key[len(source_prefix) :].lstrip("/")
            dest_key = f"{destination_prefix}/{relative_key}"

            s3.copy(
                CopySource={"Bucket": source_bucket, "Key": source_key},
                Bucket=destination_bucket,
                Key=dest_key,
                Config=transfer_config,
            )
            files_copied += 1
            total_bytes += obj["Size"]

    return CopyResult(files_copied=files_copied, total_bytes=total_bytes)


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every object under *prefix* in *bucket*.

    Returns:
        The number of objects deleted.
    """
    s3 = boto3.client("s3")
    prefix = prefix.strip("/")

    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if not keys:
            continue
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        deleted += len(keys)

    return deleted
