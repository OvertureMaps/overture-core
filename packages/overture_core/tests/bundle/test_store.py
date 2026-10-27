"""Contract tests for the `BundleStore` implementations."""

import datetime as dt
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import pytest

from overture_core.bundle.store import BundleStore, InMemoryBundleStore, S3BundleStore

# Matches the `store` fixture's clock and `FakeS3`'s default LastModified.
DEFAULT_LAST_MODIFIED = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


@pytest.fixture(params=["memory", "s3"])
def any_store(request) -> BundleStore:
    """Each `BundleStore` implementation, empty."""
    if request.param == "memory":
        return request.getfixturevalue("store")
    return S3BundleStore(client=request.getfixturevalue("fake_s3"))


class TestContract:
    def test_is_a_bundle_store(self, any_store):
        assert isinstance(any_store, BundleStore)

    def test_write_read_exists_round_trip(self, any_store):
        assert any_store.exists("b", "k") is False
        assert any_store.read("b", "k") is None
        assert any_store.last_modified("b", "k") is None

        any_store.write("b", "k", b"hello")
        assert any_store.exists("b", "k") is True
        assert any_store.read("b", "k") == b"hello"
        assert any_store.last_modified("b", "k") == DEFAULT_LAST_MODIFIED

    def test_write_defaults_to_empty_marker(self, any_store):
        any_store.write("b", "run/success")
        assert any_store.read("b", "run/success") == b""

    def test_write_replaces(self, any_store):
        any_store.write("b", "k", b"one")
        any_store.write("b", "k", b"two")
        assert any_store.read("b", "k") == b"two"

    def test_buckets_are_isolated(self, any_store):
        any_store.write("b1", "k/x", b"1")
        assert any_store.exists("b2", "k/x") is False
        assert any_store.prefix_exists("b2", "k/") is False
        assert list(any_store.iter_keys("b2", "")) == []

    def test_prefix_exists_matches_as_is(self, any_store):
        any_store.write("b", "root/v=10/success")
        assert any_store.prefix_exists("b", "root/v=1") is True
        assert any_store.prefix_exists("b", "root/v=1/") is False
        assert any_store.prefix_exists("b", "root/v=10/") is True

    def test_list_children(self, any_store):
        for key in (
            "root/v=2/run=a/success",
            "root/v=10/data/part-0",
            "root/v=1/success",
            "root/file.txt",
            "root/$folder$/x",
            "root//empty-segment",
            "rootless/v=9/x",
        ):
            any_store.write("b", key)
        assert any_store.list_children("b", "root/") == ["v=1", "v=10", "v=2"]
        assert any_store.list_children("b", "root/v=2/") == ["run=a"]
        assert any_store.list_children("b", "missing/") == []

    def test_iter_keys_is_recursive_and_sorted(self, any_store):
        for key in ("r/v=2/x", "r/v=1/y/z", "r/v=10/x", "other/x"):
            any_store.write("b", key)
        assert list(any_store.iter_keys("b", "r/")) == [
            "r/v=1/y/z",
            "r/v=10/x",
            "r/v=2/x",
        ]
        assert list(any_store.iter_keys("b", "r/v=1")) == ["r/v=1/y/z", "r/v=10/x"]

    def test_delete(self, any_store):
        any_store.write("b", "k")
        any_store.delete("b", "k")
        assert any_store.exists("b", "k") is False
        any_store.delete("b", "k")  # missing is fine

    @pytest.mark.parametrize("prefix", ["root/v=1", "root/v=1/", "/root/v=1/"])
    def test_delete_prefix_is_a_directory(self, any_store, prefix):
        for key in ("root/v=1/a", "root/v=1/b/c", "root/v=10/a", "root/v=1x"):
            any_store.write("b", key)
        assert any_store.delete_prefix("b", prefix) == 2
        assert list(any_store.iter_keys("b", "")) == ["root/v=10/a", "root/v=1x"]
        assert any_store.delete_prefix("b", prefix) == 0

    @pytest.mark.parametrize("prefix", ["", "/", "//"])
    def test_delete_prefix_refuses_whole_bucket(self, any_store, prefix):
        any_store.write("b", "k/x")
        with pytest.raises(ValueError, match="whole bucket"):
            any_store.delete_prefix("b", prefix)
        assert any_store.exists("b", "k/x") is True

    def test_presigned_url(self, any_store):
        any_store.write("b", "run/data/part 0.parquet")
        url = any_store.presigned_url("b", "run/data/part 0.parquet", expires_in=60)
        assert isinstance(url, str)
        assert "60" in url


class TestInMemoryBundleStore:
    def test_clock_defaults_to_now_utc(self):
        store = InMemoryBundleStore()
        before = dt.datetime.now(dt.timezone.utc)
        store.write("b", "k")
        stamp = store.last_modified("b", "k")
        assert stamp.tzinfo is not None
        assert before <= stamp <= dt.datetime.now(dt.timezone.utc)

    def test_write_seeds_last_modified(self, store):
        old = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        store.write("b", "k", last_modified=old)
        assert store.last_modified("b", "k") == old

    def test_stamps_are_stored_in_utc(self, store):
        eastern = dt.timezone(dt.timedelta(hours=-5))
        store.write("b", "k", last_modified=dt.datetime(2020, 1, 1, tzinfo=eastern))
        stamp = store.last_modified("b", "k")
        assert stamp.tzinfo is dt.timezone.utc
        assert stamp == dt.datetime(2020, 1, 1, 5, tzinfo=dt.timezone.utc)

    def test_rejects_naive_last_modified(self, store):
        with pytest.raises(ValueError, match="timezone-aware"):
            store.write("b", "k", last_modified=dt.datetime(2020, 1, 1))
        assert store.exists("b", "k") is False

    def test_rejects_naive_clock(self):
        store = InMemoryBundleStore(clock=lambda: dt.datetime(2020, 1, 1))
        with pytest.raises(ValueError, match="timezone-aware"):
            store.write("b", "k")

    def test_presigned_url_is_opaque(self, store):
        assert (
            store.presigned_url("b", "a/part 0")
            == "memory://b/a/part%200?expires_in=3600"
        )

    def test_write_copies_body(self, store):
        body = bytearray(b"abc")
        store.write("b", "k", body)
        body[0] = ord("z")
        assert store.read("b", "k") == b"abc"


class TestS3BundleStore:
    def test_injected_client(self, fake_s3):
        assert S3BundleStore(client=fake_s3).client is fake_s3

    def test_client_is_lazy_and_reused(self, counted_s3):
        fake, calls = counted_s3
        store = S3BundleStore()
        assert calls == []
        store.write("b", "k", b"x")
        assert store.read("b", "k") == b"x"
        assert store.exists("b", "k") is True
        assert store.presigned_url("b", "k", expires_in=5).endswith("X-Amz-Expires=5")
        assert store.client is fake
        assert calls == [("s3",)]

    def test_concurrent_first_use_opens_one_client(self, monkeypatch, fake_s3):
        calls: list[tuple] = []

        def _slow_client(*args, **_kw):
            calls.append(args)
            time.sleep(0.05)  # widen the window for a second creator
            return fake_s3

        monkeypatch.setattr(boto3, "client", _slow_client)
        store = S3BundleStore()
        workers = 8
        start = threading.Barrier(workers)

        def _first_use(_):
            start.wait()
            return store.client

        with ThreadPoolExecutor(max_workers=workers) as pool:
            clients = list(pool.map(_first_use, range(workers)))
        assert calls == [("s3",)]
        assert all(c is fake_s3 for c in clients)

    def test_each_store_opens_its_own_client(self, counted_s3):
        _, calls = counted_s3
        S3BundleStore().exists("b", "k")
        S3BundleStore().exists("b", "k")
        assert calls == [("s3",), ("s3",)]
