"""S3 object and prefix utilities built on boto3."""

import logging
import os
from dataclasses import dataclass
from urllib.parse import urlparse
from uuid import uuid4

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
        ValueError: If *s3_uri* isn't an ``s3://`` URI, or has no bucket.
    """
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Not an S3 URI: {s3_uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def build_s3_uri(bucket: str, key: str) -> str:
    """Build an ``s3://bucket/key`` URI from a bucket and key."""
    key = key.lstrip("/")
    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"


def object_exists(bucket: str, key: str) -> bool:
    """Return whether an object exists at ``bucket/key``."""
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return False
        raise


def prefix_exists(bucket: str, prefix: str) -> bool:
    """Return whether at least one object exists under ``bucket/prefix``.

    Unlike :func:`object_exists` (an exact-key check via ``head_object``),
    this checks whether anything lives under a prefix — the S3 equivalent of
    "does this directory have contents".
    """
    response = boto3.client("s3").list_objects_v2(
        Bucket=bucket, Prefix=prefix, MaxKeys=1
    )
    return response.get("KeyCount", 0) > 0


def bucket_writable(bucket: str, test_key: str | None = None) -> bool:
    """Return whether the caller's credentials can write to *bucket*.

    Probes by putting and then deleting an empty object at *test_key*.
    Returns ``False`` on any ``ClientError`` (e.g. ``AccessDenied``) rather
    than raising — this is a plain boolean check to branch on, not a
    validation function. Cleans up the test object if the put succeeded but
    something else failed before the delete.

    *test_key* defaults to a random per-call key under
    ``.overture_core_write_test/`` — a fixed key would let concurrent probes
    against the same bucket race on the same object (one call's delete
    removing another's still-in-flight test object). Pass an explicit
    *test_key* only if you need a deterministic path, e.g. in a test.
    """
    if test_key is None:
        test_key = f".overture_core_write_test/{uuid4().hex}"
    s3 = boto3.client("s3")
    put_succeeded = False
    deleted = False
    try:
        s3.put_object(Bucket=bucket, Key=test_key, Body=b"")
        put_succeeded = True
        s3.delete_object(Bucket=bucket, Key=test_key)
        deleted = True
        return True
    except botocore.exceptions.ClientError:
        return False
    finally:
        if put_succeeded and not deleted:
            try:
                s3.delete_object(Bucket=bucket, Key=test_key)
            except botocore.exceptions.ClientError:
                pass


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


def get_object_bytes(bucket: str, key: str) -> bytes | None:
    """Read an object's body as bytes, or ``None`` if ``bucket/key`` doesn't exist.

    Collapses the "does this exist" question into the return value instead
    of a try/except at every call site. Raises for any error other than a
    missing key.
    """
    try:
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return None
        raise
    return response["Body"].read()


def put_object(
    bucket: str, key: str, body: bytes, content_type: str | None = None
) -> None:
    """Write *body* to ``bucket/key``, optionally setting *content_type*.

    Generalizes :func:`write_marker` (which always writes an empty body) to
    an arbitrary payload, the common case for writing a JSON or text
    artifact rather than a zero-byte marker file.
    """
    kwargs = {"Bucket": bucket, "Key": key, "Body": body}
    if content_type is not None:
        kwargs["ContentType"] = content_type
    boto3.client("s3").put_object(**kwargs)


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
    list_prefix = f"{source_prefix}/" if source_prefix else ""
    for page in paginator.paginate(Bucket=source_bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            if source_key.endswith("/"):
                continue

            relative_key = source_key[len(source_prefix) :].lstrip("/")
            dest_key = (
                f"{destination_prefix}/{relative_key}"
                if destination_prefix
                else relative_key
            )

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
    list_prefix = f"{prefix}/" if prefix else ""
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if not keys:
            continue
        s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
        deleted += len(keys)

    return deleted


def list_common_prefixes(bucket: str, prefix: str) -> list[str]:
    """List immediate "subdirectory" names one level under *prefix*.

    Paginates ``ListObjectsV2`` with ``Delimiter="/"`` and returns each
    ``CommonPrefixes`` entry trimmed to just its segment name (no bucket, no
    *prefix*, no trailing slash). Skips the ``$folder$`` placeholder some
    tools write.

    Example: for ``prefix="root/"`` with ``root/version=1/`` and
    ``root/version=2/`` present, returns ``["version=1", "version=2"]``.
    """
    s3 = boto3.client("s3")
    names: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix) :].rstrip("/")
            if name and name != "$folder$":
                names.append(name)
    return names


def upload_directory(
    directory_path: str,
    bucket: str,
    prefix: str = "",
    *,
    max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
) -> list[str]:
    """Upload a local directory tree to S3, preserving its relative layout.

    Uses boto3's managed multipart upload (the same TransferConfig pattern as
    :func:`copy_prefix`) so large local files transfer in parallel chunks.

    Args:
        directory_path: Local directory to upload.
        bucket: Destination bucket.
        prefix: S3 prefix (folder) to upload files under.
        max_concurrency: Concurrent upload threads per file.

    Returns:
        List of resulting ``s3://`` URLs, one per uploaded file.
    """
    s3 = boto3.client("s3")
    transfer_config = TransferConfig(
        multipart_threshold=_MULTIPART_CHUNK_BYTES,
        multipart_chunksize=_MULTIPART_CHUNK_BYTES,
        max_concurrency=max_concurrency,
    )

    result: list[str] = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, directory_path)
            key = os.path.join(prefix, relative_path).replace("\\", "/").lstrip("/")
            s3.upload_file(local_path, bucket, key, Config=transfer_config)
            uri = build_s3_uri(bucket, key)
            logging.info("Uploaded %s to %s", local_path, uri)
            result.append(uri)
    return result
