"""Shared fixtures for the bundle tests.

Bundle tests use an in-memory `BundleStore`; moto's in-process S3 backs the
`S3BundleStore` tests (client reuse, store contract).
"""

import datetime as dt

import boto3
import pytest

from overture_core.bundle.store import InMemoryBundleStore

DEFAULT_LAST_MODIFIED = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


@pytest.fixture(autouse=True)
def _no_aws(monkeypatch):
    """Fail any test that would open a real S3 client.

    Bundle tests pass an explicit store; reaching ``boto3.client`` means a
    call site missed it.  `counted_s3` overrides this for the tests of the
    default store.
    """

    def _refuse(*args, **_kw):
        raise AssertionError(f"unexpected boto3.client{args}: pass store=")

    monkeypatch.setattr(boto3, "client", _refuse)


@pytest.fixture
def store() -> InMemoryBundleStore:
    """Empty in-memory store whose writes are stamped 2026-01-01 UTC."""
    return InMemoryBundleStore(clock=lambda: DEFAULT_LAST_MODIFIED)


@pytest.fixture
def s3_client(moto_s3):
    """A moto S3 client with the buckets the bundle tests use."""
    for bucket in ("bkt", "bkt1", "bkt2"):
        moto_s3.create_bucket(Bucket=bucket)
    return moto_s3


@pytest.fixture
def counted_s3(monkeypatch, s3_client):
    """`s3_client` installed as ``boto3.client``, plus a record of every call."""
    calls: list[tuple] = []

    def _client(*args, **_kw):
        calls.append(args)
        return s3_client

    monkeypatch.setattr(boto3, "client", _client)
    return s3_client, calls
