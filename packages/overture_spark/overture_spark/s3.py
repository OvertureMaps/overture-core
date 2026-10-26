"""S3 credentials for Python clients (boto3, PyArrow) in Spark jobs.

Spark's JVM S3 clients (Hadoop S3A, Iceberg) can get per-bucket credentials
that Python clients don't see. On Wherobots, for example, only S3A gets the
storage-integration role, so boto3 and PyArrow are denied with the pod's
default credentials. Resolve what S3A uses for a bucket once on the driver,
ship the resulting :class:`S3Access` to Python workers, and build clients from
it there::

    access = resolve_s3_access(spark, "s3a://bucket/prefix")

    def upload(rows):
        s3 = s3_client(access)
        ...

    df.foreachPartition(upload)

Where S3A has no such config (e.g. Glue), clients use the default credential
chain as before. ``boto3`` and ``pyarrow`` are imported lazily; install them
directly or with ``overture-spark[s3]``.
"""

from __future__ import annotations

import functools
import logging
import os
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    import boto3
    import botocore.client
    import botocore.session
    import pyarrow.fs
    from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

# Wherobots sets this on every pod; its storage-integration role requires it.
_EXTERNAL_ID_ENV = "WBC_STINT_ASSUME_ROLE_EXTERNAL_ID"
_SESSION_NAME = "overture-spark"
# Serializes client creation from the cached sessions, which aren't thread-safe.
_CLIENT_LOCK = threading.Lock()


@dataclass(frozen=True)
class S3Access:
    """How Python clients should authenticate to a bucket.

    An assumed role if ``role_arn`` is set, else static keys if set, else the
    default credential chain. ``region`` is ``None`` to let each SDK pick its
    default. Picklable and hashable, so it can be shipped to Python workers
    and used as a cache key.
    """

    region: str | None = None
    role_arn: str | None = None
    access_key: str | None = field(default=None, repr=False)
    secret_key: str | None = field(default=None, repr=False)

    def __post_init__(self):
        if (self.access_key is None) != (self.secret_key is None):
            raise ValueError("access_key and secret_key must be set together")


def resolve_s3_access(spark: SparkSession, uri: str) -> S3Access:
    """The credentials S3A uses for ``uri``'s bucket. Driver only.

    Reads the Hadoop conf (which includes ``spark.hadoop.*`` settings), where
    each ``fs.s3a.bucket.<bucket>.<key>`` overrides ``fs.s3a.<key>``:
    ``assumed.role.arn``, else ``access.key`` and ``secret.key``, plus
    ``endpoint.region``. A bucket's own keys also win over a global role.
    Returns ``S3Access()`` for non-S3 URIs.
    """
    url = urlparse(uri)
    if url.scheme not in ("s3", "s3a") or not url.netloc:
        return S3Access()
    conf = spark._jsc.hadoopConfiguration()
    bucket, default = f"fs.s3a.bucket.{url.netloc}.", "fs.s3a."

    def get(key: str, *prefixes: str) -> str | None:
        for prefix in prefixes or (bucket, default):
            value = (conf.get(prefix + key) or "").strip()
            if value:
                return value
        return None

    region = get("endpoint.region")
    access_key, secret_key = get("access.key"), get("secret.key")
    has_keys = bool(access_key and secret_key)
    bucket_keys = has_keys and bool(
        get("access.key", bucket) or get("secret.key", bucket)
    )
    role_arn = get("assumed.role.arn", bucket)
    if not role_arn and not bucket_keys:
        role_arn = get("assumed.role.arn", default)
    if role_arn:
        return S3Access(region=region, role_arn=role_arn)
    if has_keys:
        return S3Access(region=region, access_key=access_key, secret_key=secret_key)
    return S3Access(region=region)


@functools.cache
def boto3_session(access: S3Access) -> boto3.Session:
    """A boto3 session for ``access``, cached per process.

    With ``role_arn``, it assumes the role on first use, with the default
    chain's credentials and the Wherobots external ID if set, and refreshes
    before expiry. Sessions aren't thread-safe: use :func:`s3_client` to
    create S3 clients from several threads.
    """
    import boto3

    if access.role_arn:
        return boto3.Session(
            botocore_session=_assumed_role_session(access.role_arn),
            region_name=access.region,
        )
    return boto3.Session(
        aws_access_key_id=access.access_key,
        aws_secret_access_key=access.secret_key,
        region_name=access.region,
    )


def s3_client(access: S3Access, **client_kwargs) -> botocore.client.BaseClient:
    """boto3 S3 client for ``access``, built from the cached session.

    Sessions aren't thread-safe, so client creation is serialized."""
    with _CLIENT_LOCK:
        return boto3_session(access).client("s3", **client_kwargs)


@functools.cache
def pyarrow_s3_filesystem(access: S3Access) -> pyarrow.fs.S3FileSystem:
    """A PyArrow S3 filesystem for ``access``, cached per process.

    With ``role_arn``, PyArrow assumes the role, with the default chain's
    credentials and the Wherobots external ID if set, and refreshes it. Paths
    passed to the filesystem are ``bucket/key``, without a scheme.
    """
    from pyarrow import fs

    if access.role_arn:
        return fs.S3FileSystem(
            role_arn=access.role_arn,
            session_name=_SESSION_NAME,
            external_id=_external_id(access.role_arn),
            region=access.region,
        )
    return fs.S3FileSystem(
        access_key=access.access_key,
        secret_key=access.secret_key,
        region=access.region,
    )


def _external_id(role_arn: str) -> str | None:
    external_id = os.environ.get(_EXTERNAL_ID_ENV)
    log.debug(
        "Assuming %s %s an external ID from $%s",
        role_arn,
        "with" if external_id else "without",
        _EXTERNAL_ID_ENV,
    )
    return external_id


def _assumed_role_session(role_arn: str) -> botocore.session.Session:
    """A botocore session whose credentials are ``role_arn``, assumed lazily."""
    import botocore.session
    from botocore.credentials import (
        AssumeRoleCredentialFetcher,
        CredentialProvider,
        CredentialResolver,
        DeferredRefreshableCredentials,
    )

    session = botocore.session.Session()
    extra_args = {"RoleSessionName": _SESSION_NAME}
    if external_id := _external_id(role_arn):
        extra_args["ExternalId"] = external_id

    def create_client(*args, **kwargs):
        # Refreshes run on whichever thread needs credentials.
        with _CLIENT_LOCK:
            return session.create_client(*args, **kwargs)

    class AssumedRole(CredentialProvider):
        METHOD = "assume-role"

        def load(self):
            source = botocore.session.Session().get_credentials()
            if source is None:
                return None
            fetcher = AssumeRoleCredentialFetcher(
                # Calls STS with explicit source credentials, in the session's region.
                client_creator=create_client,
                source_credentials=source,
                role_arn=role_arn,
                extra_args=extra_args,
            )
            return DeferredRefreshableCredentials(
                refresh_using=fetcher.fetch_credentials, method=self.METHOD
            )

    session.register_component(
        "credential_provider", CredentialResolver([AssumedRole()])
    )
    return session
