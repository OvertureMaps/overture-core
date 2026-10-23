"""Shared fixtures for the bundle tests.

Bundle tests run against an in-memory `BundleStore`.  `FakeS3` (a boto3
client stand-in) is only for the tests of the default `S3BundleStore` path:
client reuse and the store contract.
"""

import datetime as dt

import boto3
import pytest
from botocore.exceptions import ClientError

from overture_core.bundle.store import InMemoryBundleStore

DEFAULT_LAST_MODIFIED = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


class FakeS3:
    """In-memory stand-in for the subset of the boto3 S3 client the store uses."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.last_modified: dict[tuple[str, str], dt.datetime] = {}

    def put(self, bucket: str, key: str, body: bytes = b"", lm=None) -> None:
        self.objects[(bucket, key)] = body
        if lm is not None:
            self.last_modified[(bucket, key)] = lm

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {
            "ContentLength": len(self.objects[(Bucket, Key)]),
            "LastModified": self.last_modified.get(
                (Bucket, Key), DEFAULT_LAST_MODIFIED
            ),
        }

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = self.objects[(Bucket, Key)]
        return {"Body": type("_Body", (), {"read": lambda _self: body})()}

    def put_object(self, Bucket, Key, Body=b"", **_kwargs):
        self.objects[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()
        return {}

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}

    def delete_objects(self, Bucket, Delete):
        for obj in Delete.get("Objects", []):
            self.objects.pop((Bucket, obj["Key"]), None)
        return {"Deleted": Delete.get("Objects", [])}

    def generate_presigned_url(self, ClientMethod, Params, ExpiresIn):
        assert ClientMethod == "get_object"
        return (
            f"https://{Params['Bucket']}.s3.example/{Params['Key']}"
            f"?X-Amz-Expires={ExpiresIn}"
        )

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, MaxKeys=1000, **_kw):
        contents, common = [], set()
        for b, k in sorted(self.objects):
            if b != Bucket or not k.startswith(Prefix):
                continue
            rest = k[len(Prefix) :]
            if Delimiter and Delimiter in rest:
                common.add(Prefix + rest[: rest.index(Delimiter) + 1])
            else:
                contents.append({"Key": k})
        contents = contents[:MaxKeys]
        return {
            "KeyCount": len(contents) + len(common),
            "Contents": contents,
            "CommonPrefixes": [{"Prefix": p} for p in sorted(common)],
        }

    def get_paginator(self, op_name):
        assert op_name == "list_objects_v2"
        fake = self

        class _Paginator:
            def paginate(self, **kwargs):
                yield fake.list_objects_v2(**kwargs)

        return _Paginator()


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
def fake_s3() -> FakeS3:
    """A `FakeS3` client to pass to ``S3BundleStore(client=...)``."""
    return FakeS3()


@pytest.fixture
def counted_s3(monkeypatch) -> tuple[FakeS3, list[tuple]]:
    """``FakeS3`` installed as ``boto3.client``, plus a record of every call."""
    fake = FakeS3()
    calls: list[tuple] = []

    def _client(*args, **_kw):
        calls.append(args)
        return fake

    monkeypatch.setattr(boto3, "client", _client)
    return fake, calls
