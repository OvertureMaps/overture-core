"""Conditional-write helpers against an in-process S3 (moto), not mocked clients."""

import boto3
import pytest

pytest.importorskip("moto")
from moto import mock_aws  # noqa: E402

from overture_core.cloud.aws.object import (  # noqa: E402
    ConditionalWriteConflictError,
    get_object_with_etag,
    put_object,
    put_object_if_absent,
    put_object_if_unchanged,
    update_object,
)

BUCKET = "conditional-write-test"


@pytest.fixture(autouse=True)
def s3(monkeypatch):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client


class TestConditionalWrites:
    def test_missing_object_reads_as_none_pair(self):
        assert get_object_with_etag(BUCKET, "missing") == (None, None)

    def test_create_then_create_again_is_rejected(self):
        assert put_object_if_absent(BUCKET, "k", b"first") is True
        assert put_object_if_absent(BUCKET, "k", b"second") is False
        assert get_object_with_etag(BUCKET, "k")[0] == b"first"

    def test_write_with_current_etag_succeeds(self):
        put_object(BUCKET, "k", b"v1")
        _, etag = get_object_with_etag(BUCKET, "k")
        assert put_object_if_unchanged(BUCKET, "k", b"v2", etag) is True
        assert get_object_with_etag(BUCKET, "k")[0] == b"v2"

    def test_write_with_stale_etag_is_rejected(self):
        put_object(BUCKET, "k", b"v1")
        _, stale = get_object_with_etag(BUCKET, "k")
        put_object(BUCKET, "k", b"v2")
        assert put_object_if_unchanged(BUCKET, "k", b"v3", stale) is False
        assert get_object_with_etag(BUCKET, "k")[0] == b"v2"


class TestDeletedBetweenReadAndWrite:
    def test_update_object_recreates_after_delete(self, s3):
        put_object(BUCKET, "k", b"a")
        raced = []

        def fn(body):
            if not raced:
                raced.append(True)
                s3.delete_object(Bucket=BUCKET, Key="k")
            return (body or b"") + b"!"

        assert update_object(BUCKET, "k", fn) == b"!"


class TestUpdateObject:
    def test_creates_missing_object(self):
        assert update_object(BUCKET, "k", lambda body: b"new") == b"new"

    def test_retries_after_concurrent_write_without_losing_it(self):
        put_object(BUCKET, "k", b"a")
        raced = []

        def fn(body):
            if not raced:
                raced.append(True)
                put_object(BUCKET, "k", body + b"b")
            return body + b"c"

        assert update_object(BUCKET, "k", fn) == b"abc"
        assert get_object_with_etag(BUCKET, "k")[0] == b"abc"

    def test_gives_up_when_every_attempt_loses(self, monkeypatch):
        monkeypatch.setattr("overture_core.cloud.aws.object.time.sleep", lambda _: None)
        put_object(BUCKET, "k", b"a")

        def fn(body):
            put_object(BUCKET, "k", body + b"x")
            return body + b"y"

        with pytest.raises(ConditionalWriteConflictError):
            update_object(BUCKET, "k", fn, max_attempts=3)
