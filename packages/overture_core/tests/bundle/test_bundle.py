"""Tests for the bundle model."""

import datetime as dt
import json

import boto3
import pytest
from botocore.exceptions import ClientError

from overture_core.bundle.bundle import (
    Bundle,
    BundleDecision,
    CleanupReport,
    DeleteResult,
    PartitionResolutionError,
    ReleaseCandidateBundle,
    ScratchBundle,
    SourceIngestBundle,
    SourceRawBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
    _natural_key,
    _resolve_partitions,
)

_DEFAULT_LM = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


# ── Fakes ────────────────────────────────────────────────────────────────────


class FakeS3:
    """In-memory stand-in for the subset of the boto3 S3 client the model uses."""

    def __init__(self) -> None:
        self.store: dict[tuple[str, str], bytes] = {}
        self.last_modified: dict[tuple[str, str], dt.datetime] = {}

    def put(self, bucket: str, key: str, body: bytes = b"", lm=None) -> None:
        self.store[(bucket, key)] = body
        if lm is not None:
            self.last_modified[(bucket, key)] = lm

    def keys(self, bucket: str = "bkt") -> set[str]:
        return {k for b, k in self.store if b == bucket}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {
            "ContentLength": len(self.store[(Bucket, Key)]),
            "LastModified": self.last_modified.get((Bucket, Key), _DEFAULT_LM),
        }

    def put_object(self, Bucket, Key, Body=b"", **_kwargs):
        self.store[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()
        return {}

    def delete_object(self, Bucket, Key):
        self.store.pop((Bucket, Key), None)
        return {}

    def delete_objects(self, Bucket, Delete):
        for obj in Delete.get("Objects", []):
            self.store.pop((Bucket, obj["Key"]), None)
        return {"Deleted": Delete.get("Objects", [])}

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, MaxKeys=1000, **_kw):
        contents, common = [], set()
        for b, k in sorted(self.store):
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


class RecordingMetadata:
    """Minimal `BundleMetadata` implementation that records calls."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.section: dict = {"inputs": []}

    def init(self, input_paths=None, capture_variables=True):
        self.calls.append(("init", input_paths, capture_variables))
        self.section["inputs"] = [{"path": p} for p in input_paths or []]

    def add(self, key, value):
        self.calls.append(("add", key, value))
        self.section[key] = value

    def read_dag_section(self):
        return self.section

    def update_inputs(self, inputs):
        self.calls.append(("update_inputs", inputs))
        self.section["inputs"] = inputs

    def finalize(self, status="success"):
        self.calls.append(("finalize", status))


class RecordingDropper:
    """`TableDropper` that records calls and returns a fixed table list."""

    def __init__(self, tables=("db.tbl",), fail: bool = False) -> None:
        self.calls: list[dict] = []
        self.tables = list(tables)
        self.fail = fail

    def __call__(self, bucket, base_prefix, dry_run):
        self.calls.append(
            {"bucket": bucket, "base_prefix": base_prefix, "dry_run": dry_run}
        )
        if self.fail:
            raise RuntimeError("athena down")
        return list(self.tables)


@pytest.fixture
def s3(monkeypatch) -> FakeS3:
    fake = FakeS3()
    monkeypatch.setattr(boto3, "client", lambda *_a, **_kw: fake)
    return fake


# ── Paths ────────────────────────────────────────────────────────────────────


class TestPaths:
    def test_base_uri_and_children(self):
        b = Bundle(bucket="bkt", root_path="/root/", partitions={"version": "1"})
        assert b.base_uri == "s3://bkt/root/version=1"
        assert b.base_prefix == "root/version=1"
        assert b.data_uri == "s3://bkt/root/version=1/data"
        assert b.metadata_uri == "s3://bkt/root/version=1/metadata.json"
        assert b.success_uri == "s3://bkt/root/version=1/success"
        assert b.sub_directory("/extra/") == "s3://bkt/root/version=1/extra"
        assert b.root_uri == "s3://bkt/root"

    def test_namespace_prefixes_root(self):
        b = Bundle(bucket="bkt", root_path="feeds", namespace="/alice/")
        assert b.base_uri == "s3://bkt/alice/feeds"
        assert b.root_prefix == "alice/feeds"

    def test_empty_namespace_is_noop(self):
        b = Bundle(bucket="bkt", root_path="feeds", namespace="")
        assert b.base_uri == "s3://bkt/feeds"

    def test_unset_partition_raises(self):
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": ""})
        with pytest.raises(ValueError, match="Partition 'version' has no value"):
            _ = b.base_uri

    def test_from_path_and_from_base_uri(self):
        b = Bundle.from_path("s3://bkt/a/b", partitions={"run": "r1"})
        assert b.base_uri == "s3://bkt/a/b/run=r1"
        b2 = Bundle.from_base_uri("s3://bkt/theme_promote/theme=places/run=X")
        assert b2.root_uri == "s3://bkt/theme_promote"
        assert b2.partitions == {"theme": "places", "run": "X"}

    def test_version_and_run_setters(self):
        b = Bundle(bucket="bkt", root_path="r", partitions={"version": "", "run": ""})
        b.version = "1"
        b.run = "r1"
        assert b.base_uri == "s3://bkt/r/version=1/run=r1"

    def test_repr(self):
        b = Bundle(bucket="bkt", root_path="r", partitions={"version": "1"})
        assert repr(b) == "Bundle(bucket='bkt', root_prefix='r', version='1')"


class TestSubclasses:
    def test_bucket_is_required(self):
        with pytest.raises(TypeError):
            SourceRawBundle(provider="p", resource="r")  # type: ignore[call-arg]

    def test_source_raw(self):
        b = SourceRawBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="r1"
        )
        assert b.base_uri == "s3://bkt/datasets/provider=p/resource=r/version=1/run=r1"
        assert b.BUNDLE_TYPE == "source_raw"
        assert (b.provider, b.resource) == ("p", "r")

    def test_source_ingest_with_namespace_and_root_override(self):
        b = SourceIngestBundle(
            provider="p",
            resource="r",
            bucket="bkt",
            version="1",
            run="r1",
            namespace="ns",
        )
        assert b.base_uri == "s3://bkt/ns/feeds/provider=p/resource=r/version=1/run=r1"
        assert b.changelog_uri.endswith("/run=r1/changelog")
        assert b.metrics_summary_prefix.endswith("/run=r1/metrics_summary")
        b2 = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", root_path="other"
        )
        assert b2.root_uri == "s3://bkt/other"

    def test_source_ingest_from_path(self):
        b = SourceIngestBundle.from_path(
            "s3://bkt/feeds", provider="p", resource="r", version="1", run="r1"
        )
        assert isinstance(b, SourceIngestBundle)
        assert b.base_uri == "s3://bkt/feeds/provider=p/resource=r/version=1/run=r1"

    def test_source_ingest_from_path_keeps_namespace_and_root(self):
        b = SourceIngestBundle.from_path(
            "s3://bkt/alice/feeds", provider="p", resource="r", version="1", run="r1"
        )
        assert b.base_uri == (
            "s3://bkt/alice/feeds/provider=p/resource=r/version=1/run=r1"
        )
        b2 = SourceIngestBundle.from_path(
            "s3://bkt/custom/", {"provider": "p", "resource": "r"}, version="2"
        )
        assert b2.root_uri == "s3://bkt/custom"
        assert b2.partitions == {
            "provider": "p",
            "resource": "r",
            "version": "2",
            "run": "",
        }

    def test_source_ingest_from_path_without_root_uses_default(self):
        b = SourceIngestBundle.from_path("s3://bkt", provider="p", resource="r")
        assert b.root_uri == "s3://bkt/feeds"

    @pytest.mark.parametrize(
        "bundle",
        [
            SourceRawBundle(
                provider="p", resource="r", bucket="bkt", version="1", run="a"
            ),
            SourceIngestBundle(
                provider="p",
                resource="r",
                bucket="bkt",
                version="1",
                run="a",
                namespace="ns",
            ),
            ThemeAssembleBundle(theme="t", bucket="bkt", run="a"),
            ThemePromoteBundle(theme="t", bucket="bkt", schema="v1", run="a"),
            ReleaseCandidateBundle(bucket="bkt", release="R", run="a"),
            ScratchBundle(purpose="p", bucket="bkt", run="a", root_path="tmp/x"),
            Bundle(bucket="bkt", root_path="root", partitions={"k": "v"}),
        ],
        ids=lambda b: type(b).__name__,
    )
    def test_from_base_uri_and_from_path_round_trip(self, bundle):
        cls = type(bundle)
        parsed = cls.from_base_uri(bundle.base_uri)
        assert type(parsed) is cls
        assert parsed.base_uri == bundle.base_uri
        rebuilt = cls.from_path(bundle.root_uri, bundle.partitions)
        assert type(rebuilt) is cls
        assert rebuilt.base_uri == bundle.base_uri

    def test_from_base_uri_rejects_unknown_partition(self):
        with pytest.raises(ValueError, match=r"has no partition\(s\) \['schema'\]"):
            ThemeAssembleBundle.from_base_uri(
                "s3://bkt/theme_stage/theme=t/schema=s/run=r"
            )

    def test_theme_assemble(self):
        b = ThemeAssembleBundle(theme="places", bucket="bkt", run="r1")
        assert b.base_uri == "s3://bkt/theme_stage/theme=places/run=r1"
        assert b.metrics_uri == "s3://bkt/theme_stage/theme=places/run=r1/metrics"

    def test_theme_promote(self):
        b = ThemePromoteBundle(theme="places", bucket="bkt", schema="v1", run="r1")
        assert b.base_uri == "s3://bkt/theme_promote/theme=places/schema=v1/run=r1"
        assert b.pmtiles_uri.endswith("/pmtiles")
        assert b.bridgefiles_prefix.endswith("/bridgefiles")
        assert b.validation_uri.endswith("/validation")

    def test_release_candidate(self):
        b = ReleaseCandidateBundle(
            bucket="bkt", release="2026-05-20.0", run="r1", namespace="ns"
        )
        assert b.base_uri == "s3://bkt/ns/release_candidate/release=2026-05-20.0/run=r1"
        assert b.release == "2026-05-20.0"
        assert b.registry_uri.endswith("/registry")

    def test_scratch(self):
        b = ScratchBundle(purpose="debug", bucket="bkt", run="r1")
        assert b.base_uri == "s3://bkt/scratch/purpose=debug/run=r1"
        assert b.BUNDLE_TYPE == "scratch"

    @pytest.mark.parametrize(
        "cls",
        [
            SourceRawBundle,
            SourceIngestBundle,
            ThemeAssembleBundle,
            ThemePromoteBundle,
            ReleaseCandidateBundle,
            ScratchBundle,
        ],
    )
    def test_from_base_uri_keeps_bucket_root(self, cls):
        segments = "/".join(f"{k}=x" for k in cls.PARTITION_KEYS)
        b = cls.from_base_uri(f"s3://bkt/{segments}")
        assert b.root_prefix == ""
        assert b.base_uri == f"s3://bkt/{segments}"


# ── Lifecycle ────────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_initialize_deletes_success_and_writes_metadata(self, s3):
        s3.put("bkt", "theme_stage/theme=places/run=r1/success")
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="acme", resource="widgets", bucket="src", version="1", run="a"
        )
        b = ThemeAssembleBundle(theme="places", bucket="bkt", run="r1")
        b.initialize(
            input_bundles=[upstream],
            capture_variables=False,
            custom_metadata={"k": "v"},
            metadata=meta,
        )
        assert "theme_stage/theme=places/run=r1/success" not in s3.keys()
        assert meta.calls == [
            ("init", [upstream.base_uri], False),
            ("add", "bundle_type", "theme_assemble"),
            ("add", "k", "v"),
        ]

    def test_initialize_enriches_inputs_when_datasets_dir_given(self, s3, tmp_path):
        (tmp_path / "acme.json").write_text(
            json.dumps(
                {
                    "provider": {"label": "acme", "name": "Acme Corp", "url": {}},
                    "resources": [
                        {
                            "label": "widgets",
                            "name": "Acme Widgets",
                            "collection": {"license": {"type": "MIT"}},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="acme", resource="widgets", bucket="src", version="7", run="a"
        )
        b = ThemeAssembleBundle(theme="places", bucket="bkt", run="r1")
        b.initialize(input_bundles=[upstream], metadata=meta, datasets_dir=tmp_path)
        (inp,) = meta.section["inputs"]
        assert inp["provider"]["label"] == "acme"
        assert inp["resource"]["label"] == "widgets"
        assert inp["resource"]["version"] == "7"

    def test_initialize_skips_enrichment_without_datasets_dir(self, s3):
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="acme", resource="widgets", bucket="src", version="7", run="a"
        )
        ThemeAssembleBundle(theme="t", bucket="bkt", run="r1").initialize(
            input_bundles=[upstream], metadata=meta
        )
        assert not any(c[0] == "update_inputs" for c in meta.calls)

    def test_enrichment_failure_is_not_fatal(self, s3, tmp_path):
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="missing", resource="x", bucket="src", version="1", run="a"
        )
        ThemeAssembleBundle(theme="t", bucket="bkt", run="r1").initialize(
            input_bundles=[upstream], metadata=meta, datasets_dir=tmp_path
        )
        assert meta.section["inputs"] == [{"path": upstream.base_uri}]

    def test_finalize_writes_success(self, s3):
        meta = RecordingMetadata()
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1")
        b.finalize(status="partial", metadata=meta)
        assert meta.calls == [("finalize", "partial")]
        assert "scratch/purpose=p/run=r1/success" in s3.keys()

    def test_batch_lifecycle_uses_metadata_factory(self, s3):
        bundles = [ScratchBundle(purpose=p, bucket="bkt", run="r1") for p in "ab"]
        made: dict[str, RecordingMetadata] = {}

        def factory(b):
            made[b.base_uri] = RecordingMetadata()
            return made[b.base_uri]

        Bundle.initialize_batch(bundles, metadata_factory=factory, max_workers=2)
        assert set(made) == {b.base_uri for b in bundles}
        assert all(m.calls[0][0] == "init" for m in made.values())

        made.clear()
        Bundle.finalize_batch(bundles, metadata_factory=factory, max_workers=2)
        assert all(m.calls == [("finalize", "success")] for m in made.values())
        assert {
            "scratch/purpose=a/run=r1/success",
            "scratch/purpose=b/run=r1/success",
        } <= s3.keys()

    @pytest.mark.parametrize("action", ["initialize_batch", "finalize_batch"])
    def test_batch_logs_every_failure_and_raises_first(self, s3, action, caplog):
        bundles = [ScratchBundle(purpose=p, bucket="bkt", run="r1") for p in "abc"]

        class Boom(RecordingMetadata):
            def init(self, *args, **kwargs):
                raise RuntimeError(self.name)

            def finalize(self, status="success"):
                raise RuntimeError(self.name)

        def factory(b):
            purpose = b.partitions["purpose"]
            m = Boom() if purpose != "b" else RecordingMetadata()
            m.name = f"boom {purpose}"
            return m

        with pytest.raises(RuntimeError, match="boom a"):
            getattr(Bundle, action)(bundles, metadata_factory=factory, max_workers=3)
        messages = [r.getMessage() for r in caplog.records]
        assert sum("failed for ScratchBundle" in m for m in messages) == 2
        assert any("failed for 2/3 bundle(s)" in m for m in messages)

    def test_batch_noop_on_empty(self):
        Bundle.initialize_batch([], metadata_factory=lambda b: RecordingMetadata())
        Bundle.finalize_batch([], metadata_factory=lambda b: RecordingMetadata())


# ── Partition resolution ─────────────────────────────────────────────────────


class TestResolvePartitions:
    def test_finds_latest_successful_version_and_run(self, s3):
        for v, r, ok in (("1", "a", True), ("2", "a", True), ("2", "b", False)):
            key = f"feeds/provider=p/resource=r/version={v}/run={r}"
            s3.put("bkt", f"{key}/data/x.parquet")
            if ok:
                s3.put("bkt", f"{key}/success")
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt")
        b.resolve_partitions()
        assert (b.version, b.run) == ("2", "a")

    def test_resolve_at_bucket_root(self, s3):
        for run in ("r1", "r2"):
            s3.put("bkt", f"run={run}/success")
        assert _resolve_partitions("bkt", "", ["run"], {}) == {"run": "r2"}
        b = Bundle.from_base_uri("s3://bkt/run=")
        b.resolve_partitions()
        assert (b.base_prefix, b.base_uri) == ("run=r2", "s3://bkt/run=r2")
        assert b.is_successful()

    def test_override_and_missing_raise(self, s3):
        s3.put("bkt", "root/version=1/success")
        assert _resolve_partitions("bkt", "root", ["version"], {"version": "1"}) == {
            "version": "1"
        }
        with pytest.raises(PartitionResolutionError):
            _resolve_partitions("bkt", "root", ["version"], {"version": "9"})

    def test_resolve_latest_released_with_namespace(self, s3):
        for run, released in (("r1", True), ("r2", True), ("r3", False)):
            prefix = f"ns/release_candidate/release=R/run={run}"
            s3.put("bkt", f"{prefix}/success")
            if released:
                s3.put("bkt", f"{prefix}/released")
        rc = ReleaseCandidateBundle.resolve_latest_released(
            release="R", bucket="bkt", namespace="ns"
        )
        assert isinstance(rc, ReleaseCandidateBundle)
        assert rc.base_uri == "s3://bkt/ns/release_candidate/release=R/run=r2"

    def test_resolve_latest_released_none_raises(self, s3):
        s3.put("bkt", "release_candidate/release=R/run=r1/success")
        with pytest.raises(PartitionResolutionError):
            ReleaseCandidateBundle.resolve_latest_released(release="R", bucket="bkt")


# ── Retention / delete / cleanup ─────────────────────────────────────────────


def _seed_versions(s3: FakeS3, now: dt.datetime) -> None:
    """v0 failed, v1 old success, v2 fresh success, under namespace ``ns``."""
    old = now - dt.timedelta(days=90)
    for v, ok in (("0", False), ("1", True), ("2", True)):
        base = f"ns/feeds/provider=p/resource=r/version={v}/run=r1"
        s3.put("bkt", f"{base}/metadata.json", b"{}")
        s3.put("bkt", f"{base}/data/part-0.parquet", b"x")
        if ok:
            s3.put("bkt", f"{base}/success", lm=old if v == "1" else now)


class TestRetention:
    def test_is_successful_and_age(self, s3):
        now = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        assert b.is_successful() is False
        assert b.age_days() is None
        s3.put("bkt", "root/version=1/success", lm=_DEFAULT_LM)
        assert b.is_successful() is True
        assert b.age_days(now=now) == 31.0

    def test_is_deletable_rules(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        bs = [
            SourceIngestBundle(
                provider="p",
                resource="r",
                bucket="bkt",
                version=v,
                run="r1",
                namespace="ns",
            )
            for v in "012"
        ]
        decisions = [b.is_deletable(30, bs, now=now)[0] for b in bs]
        assert decisions == [True, True, False]
        assert bs[1].is_deletable(365, bs, now=now)[0] is False

    def test_discover_all_honours_namespace(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        assert SourceIngestBundle.discover_all(bucket="bkt") == []
        found = SourceIngestBundle.discover_all(bucket="bkt", namespace="ns")
        assert sorted(b.partitions["version"] for b in found) == ["0", "1", "2"]
        assert all(type(b) is SourceIngestBundle for b in found)
        assert all(b.root_uri == "s3://bkt/ns/feeds" for b in found)

    def test_discover_all_requires_root_and_keys(self):
        with pytest.raises(NotImplementedError):
            Bundle.discover_all(bucket="bkt")

    def test_cleanup_all_dry_run(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        dropper = RecordingDropper()
        before = set(s3.keys())
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=True,
            namespace="ns",
            table_dropper=dropper,
            now=now,
        )
        assert isinstance(report, CleanupReport)
        would = sorted(
            d.bundle_uri.split("version=")[1].split("/")[0] for d in report.would_delete
        )
        assert would == ["0", "1"]
        assert len(report.kept) == 1
        assert all(isinstance(d, BundleDecision) for d in report.kept)
        assert report.deleted == []
        assert dropper.calls == []
        assert s3.keys() == before

    def test_cleanup_all_execute_deletes_and_drops_tables(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        dropper = RecordingDropper()
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=False,
            namespace="ns",
            table_dropper=dropper,
            now=now,
        )
        assert len(report.deleted) == 2
        assert all(r.deleted_tables == ["db.tbl"] for r in report.deleted)
        assert sorted(c["base_prefix"] for c in dropper.calls) == [
            "ns/feeds/provider=p/resource=r/version=0/run=r1",
            "ns/feeds/provider=p/resource=r/version=1/run=r1",
        ]
        remaining = {k.split("/version=")[1][0] for k in s3.keys()}
        assert remaining == {"2"}

    def test_delete_dry_run_counts_without_deleting(self, s3):
        s3.put("bkt", "root/version=1/a")
        s3.put("bkt", "root/version=1/b/c")
        s3.put("bkt", "root/version=10/other")
        dropper = RecordingDropper()
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        result = b.delete(dry_run=True, table_dropper=dropper)
        assert result == DeleteResult(
            bundle_uri="s3://bkt/root/version=1",
            deleted_object_count=2,
            deleted_tables=["db.tbl"],
            dry_run=True,
        )
        assert dropper.calls == [
            {"bucket": "bkt", "base_prefix": "root/version=1", "dry_run": True}
        ]
        assert len(s3.keys()) == 3

    def test_delete_only_removes_own_prefix(self, s3):
        s3.put("bkt", "root/version=1/a")
        s3.put("bkt", "root/version=10/other")
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        result = b.delete()
        assert result.deleted_object_count == 1
        assert result.deleted_tables == []
        assert s3.keys() == {"root/version=10/other"}

    def test_delete_table_dropper_failure_is_not_fatal(self, s3):
        s3.put("bkt", "root/version=1/a")
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        result = b.delete(table_dropper=RecordingDropper(fail=True))
        assert result.deleted_object_count == 1
        assert result.deleted_tables == []

    def test_from_base_uri_at_bucket_root_deletes_that_bundle(self, s3):
        s3.put("bkt", "theme=places/run=X/data/a.parquet")
        s3.put("bkt", "theme=places/run=X/success")
        s3.put("bkt", "theme_stage/theme=places/run=X/success")
        b = ThemeAssembleBundle.from_base_uri("s3://bkt/theme=places/run=X")
        assert b.base_uri == "s3://bkt/theme=places/run=X"
        assert b.delete().deleted_object_count == 2
        assert s3.keys() == {"theme_stage/theme=places/run=X/success"}

    def test_scratch_bundles_are_discoverable(self, s3):
        s3.put("bkt", "scratch/purpose=p/run=r1/success")
        found = ScratchBundle.discover_all(bucket="bkt")
        assert [b.base_uri for b in found] == ["s3://bkt/scratch/purpose=p/run=r1"]
        assert type(found[0]) is ScratchBundle

    def test_is_deletable_matches_successful_by_location(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        s3.put("bkt", "root/version=0/success", lm=now - dt.timedelta(days=60))
        bs = [
            Bundle(bucket="bkt", root_path="root", partitions={"version": v})
            for v in "01"
        ]
        latest = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        assert bs[0].is_deletable(30, bs, now=now, successful=[latest])[0] is True
        assert bs[1].is_deletable(30, bs, now=now, successful=[latest]) == (
            False,
            "is the latest successful bundle",
        )

    def test_delete_refuses_bundle_at_bucket_root(self, s3):
        with pytest.raises(ValueError, match="has no partitions"):
            Bundle(bucket="bkt", root_path="").delete()

    @pytest.mark.parametrize(
        "bundle",
        [
            Bundle(bucket="bkt", root_path="datasets"),
            Bundle(bucket="bkt", root_path="feeds"),
            Bundle(bucket="bkt", root_path="feeds", namespace="alice"),
        ],
        ids=["datasets", "feeds", "namespace-root"],
    )
    def test_delete_refuses_bundle_without_partitions(self, s3, bundle):
        s3.put("bkt", "datasets/provider=p/resource=r/version=1/run=r1/success")
        s3.put("bkt", "feeds/provider=p/resource=r/version=1/run=r1/success")
        s3.put("bkt", "alice/feeds/provider=p/resource=r/version=1/run=r1/success")
        dropper = RecordingDropper()
        with pytest.raises(ValueError, match="has no partitions"):
            bundle.delete(table_dropper=dropper)
        assert len(s3.keys()) == 3
        assert dropper.calls == []

    @pytest.mark.parametrize(
        ("bundle", "missing"),
        [
            (
                SourceIngestBundle(provider="p", resource="r", bucket="bkt"),
                "version, run",
            ),
            (
                SourceIngestBundle(
                    provider="p", resource="r", bucket="bkt", run="r1", namespace="ns"
                ),
                "version",
            ),
            (
                Bundle(
                    bucket="bkt",
                    root_path="feeds",
                    partitions={"provider": "p", "resource": ""},
                ),
                "resource",
            ),
        ],
        ids=["subclass-unresolved", "subclass-namespace", "generic-empty-value"],
    )
    def test_delete_refuses_unset_declared_partitions(self, s3, bundle, missing):
        for root in ("feeds", "ns/feeds"):
            s3.put("bkt", f"{root}/provider=p/resource=r/version=1/run=r1/success")
        dropper = RecordingDropper()
        with pytest.raises(ValueError, match=f"partition\\(s\\) {missing} have no"):
            bundle.delete(dry_run=False, table_dropper=dropper)
        assert len(s3.keys()) == 2
        assert dropper.calls == []

    def test_cleanup_all_checks_success_once_per_bundle(self, s3, monkeypatch):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        checked: list[str] = []
        original = Bundle.is_successful

        def counting(self):
            checked.append(self.base_uri)
            return original(self)

        monkeypatch.setattr(Bundle, "is_successful", counting)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt", min_age_days=30, dry_run=True, namespace="ns", now=now
        )
        assert len(report.would_delete) == 2
        assert len(checked) == 3
        assert len(set(checked)) == 3

    def test_is_deletable_uses_success_snapshot(self, s3):
        now = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        s3.put("bkt", "root/version=1/metadata.json", b"{}")
        old, new = (
            Bundle(bucket="bkt", root_path="root", partitions={"version": v})
            for v in "12"
        )
        group = [old, new]
        assert old.is_deletable(30, group, now=now, successful=[new]) == (
            True,
            "unsuccessful; superseded by ('2',); age 31.0d >= 30d",
        )
        assert old.is_deletable(30, group, now=now, successful=[])[0] is False


# ── Processing checks ────────────────────────────────────────────────────────


class TestNeedsProcessing:
    _PARENT = "feeds/provider=p/resource=r/version=1"

    def _bundle(self, run="r2"):
        return SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run=run
        )

    def test_non_latest_override_forces_true(self, s3):
        assert self._bundle().needs_processing({"version": "1"}) is True

    def test_no_runs_returns_true(self, s3):
        assert self._bundle().needs_processing({"version": "latest"}) is True

    def test_other_successful_run_returns_false(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r1/success")
        assert self._bundle().needs_processing() is False

    def test_unsuccessful_run_returns_true(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r1/data/x")
        assert self._bundle().needs_processing() is True

    def test_same_run_rerun_returns_true(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r2/success")
        assert self._bundle().needs_processing() is True

    def test_rerun_returns_true_when_an_earlier_run_also_succeeded(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r1/success")
        s3.put("bkt", f"{self._PARENT}/run=r2/success")
        assert self._bundle("r2").needs_processing() is True
        assert self._bundle("r0").needs_processing() is False

    def test_explicit_base_uri_is_checked(self, s3):
        s3.put("other", f"{self._PARENT}/run=r1/success")
        b = self._bundle()
        assert b.needs_processing() is True
        rendered = b.base_uri.replace("s3://bkt/", "s3://other/")
        assert b.needs_processing(base_uri=rendered) is False

    def test_similarly_named_artifacts_are_not_success_markers(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r1/success.log")
        s3.put("bkt", f"{self._PARENT}/run=r1/success/part-0")
        assert self._bundle("r2").needs_processing() is True

    def test_current_run_artifact_is_not_a_rerun(self, s3):
        s3.put("bkt", f"{self._PARENT}/run=r1/success")
        s3.put("bkt", f"{self._PARENT}/run=r2/success.log")
        assert self._bundle("r2").needs_processing() is False


class TestBatchHelpers:
    def test_resolve_partitions_batch_picks_latest_per_resource(self, s3):
        for res, v, r in (
            ("a", "1", "x"),
            ("a", "2", "x"),
            ("a", "2", "y"),
            ("b", "1", "x"),
        ):
            s3.put(
                "bkt", f"ns/feeds/provider=p/resource={res}/version={v}/run={r}/success"
            )
        s3.put("bkt", "ns/feeds/provider=p/resource=a/version=3/run=x/data/f")
        out = SourceIngestBundle.resolve_partitions_batch(
            provider="p", bucket="bkt", namespace="ns"
        )
        assert sorted(out) == ["a", "b"]
        assert isinstance(out["a"], SourceIngestBundle)
        assert out["a"].base_uri == (
            "s3://bkt/ns/feeds/provider=p/resource=a/version=2/run=y"
        )
        assert out["b"].base_uri == (
            "s3://bkt/ns/feeds/provider=p/resource=b/version=1/run=x"
        )

    def test_resolve_partitions_batch_rejects_other_layouts(self):
        with pytest.raises(TypeError):
            ThemeAssembleBundle.resolve_partitions_batch(provider="p", bucket="bkt")

    def test_resolve_partitions_batch_requires_exact_layout(self, s3):
        base = "feeds/provider=p/resource=r"
        for key in (
            f"{base}/version=1/run=a/success",
            f"{base}/version=2/run=b/data/child/success",
            f"{base}/version=3/run=c/data/success",
            f"{base}/version=4/success",
            f"{base}/version=5/run=/success",
            f"{base}/run=x/version=6/success",
            f"{base}/version=7/run=d/success.log",
        ):
            s3.put("bkt", key)
        out = SourceIngestBundle.resolve_partitions_batch(provider="p", bucket="bkt")
        assert out["r"].base_uri == f"s3://bkt/{base}/version=1/run=a"
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt")
        b.resolve_partitions()
        assert b.base_uri == out["r"].base_uri

    @pytest.mark.parametrize(
        ("marker", "needs"),
        [
            ("run=r2/success", True),
            ("run=r1/data/success", True),
            ("run=r1/success.log", True),
            ("run=r1/success/part-0", True),
            ("run=r1/success", False),
        ],
        ids=["rerun", "nested", "suffix", "subdir", "other-run-done"],
    )
    def test_needs_processing_batch_single_marker(self, s3, marker, needs):
        s3.put("bkt", f"feeds/provider=p/resource=a/version=1/{marker}")
        b = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="r2"
        )
        assert b.needs_processing() is needs
        assert Bundle.needs_processing_batch([b]) == ([b] if needs else [])

    def test_needs_processing_batch(self, s3):
        s3.put("bkt", "feeds/provider=p/resource=a/version=1/run=x/success")
        done = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="z"
        )
        todo = SourceIngestBundle(
            provider="p", resource="b", bucket="bkt", version="1", run="z"
        )
        assert Bundle.needs_processing_batch([done, todo]) == [todo]
        assert Bundle.needs_processing_batch([done, todo], {"version": "1"}) == [
            done,
            todo,
        ]
        assert Bundle.needs_processing_batch([]) == []

    def test_needs_processing_batch_matches_singular(self, s3):
        for key in (
            "feeds/provider=p/resource=a/version=1/run=x/success",
            "feeds/provider=p/resource=a/version=1/run=y/success",
            "feeds/provider=p/resource=b/version=1/run=x/data/f",
            "feeds/provider=p/resource=c/version=10/run=x/success",
            "theme_stage/theme=places/run=r1/success",
        ):
            s3.put("bkt", key)

        def ingest(resource, run, version="1"):
            return SourceIngestBundle(
                provider="p", resource=resource, bucket="bkt", version=version, run=run
            )

        bundles = [
            ingest("a", "z"),  # another run succeeded -> skip
            ingest("a", "y"),  # rerun of a successful run -> process
            ingest("b", "z"),  # no successful run -> process
            ingest("c", "z"),  # only version=10 succeeded -> process
            ThemeAssembleBundle(theme="places", bucket="bkt", run="r2"),  # skip
            ThemeAssembleBundle(theme="places", bucket="bkt", run="r1"),  # rerun
            ThemeAssembleBundle(theme="roads", bucket="bkt", run="r1"),  # process
        ]
        expected = [b for b in bundles if b.needs_processing()]
        assert [b.base_uri for b in expected] == [
            bundles[i].base_uri for i in (1, 2, 3, 5, 6)
        ]
        assert Bundle.needs_processing_batch(bundles) == expected


# ── Path helpers ─────────────────────────────────────────────────────────────


class TestPathHelpers:
    def _bundle(self, run="r1"):
        return Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1", "run": run}
        )

    def test_prefix_and_uri_through_partition(self):
        b = self._bundle()
        assert b.prefix_through_partition("version") == "root/version=1"
        assert b.prefix_through_partition("run") == "root/version=1/run=r1"
        assert b.uri_through_partition("version") == "s3://bkt/root/version=1"

    def test_prefix_through_unknown_partition_raises(self):
        with pytest.raises(ValueError, match="is not a partition"):
            self._bundle().prefix_through_partition("theme")

    def test_prefix_through_partition_with_unset_value_raises(self):
        with pytest.raises(ValueError, match="Partition 'run' has no value"):
            self._bundle(run="").prefix_through_partition("run")

    def test_base_prefix_with_unset_value_raises(self):
        with pytest.raises(ValueError, match="Partition 'run' has no value"):
            _ = self._bundle(run="").base_prefix

    def test_build_path(self):
        assert Bundle.build_path("s3://bkt/feeds/", "/a/", "b") == "s3://bkt/feeds/a/b"

    def test_from_base_uri_rejects_segment_after_partitions(self):
        with pytest.raises(ValueError, match="Non-partition segment 'data'"):
            Bundle.from_base_uri("s3://bkt/root/version=1/data")

    def test_base_uri_without_bucket_returns_root_path(self):
        b = Bundle(bucket="", root_path="templated/root", partitions={"run": "r1"})
        assert b.root_uri == "templated/root"
        assert b.base_uri == "templated/root/run=r1"

    @pytest.mark.parametrize(
        ("bundle", "dirs"),
        [
            (
                SourceIngestBundle(
                    provider="p", resource="r", bucket="bkt", version="1", run="a"
                ),
                ("changelog", "metrics", "metrics_summary"),
            ),
            (
                ThemeAssembleBundle(theme="t", bucket="bkt", run="a"),
                ("changelog", "metrics", "metrics_summary"),
            ),
            (
                ThemePromoteBundle(theme="t", bucket="bkt", schema="s", run="a"),
                (
                    "bridgefiles",
                    "pmtiles",
                    "changelog",
                    "metrics",
                    "metrics_summary",
                    "changelog_internal",
                    "partition_bboxes",
                    "validation",
                ),
            ),
            (
                ReleaseCandidateBundle(bucket="bkt", release="R", run="a"),
                (
                    "bridgefiles",
                    "pmtiles",
                    "changelog",
                    "metrics",
                    "metrics_summary",
                    "partition_bboxes",
                    "registry",
                ),
            ),
        ],
        ids=lambda v: type(v).__name__ if isinstance(v, Bundle) else "",
    )
    def test_sub_directory_accessors(self, bundle, dirs):
        for name in dirs:
            assert getattr(bundle, f"{name}_uri") == f"{bundle.base_uri}/{name}"
            assert getattr(bundle, f"{name}_prefix") == f"{bundle.base_prefix}/{name}"


# ── Partition resolution edge cases ──────────────────────────────────────────


class TestResolvePartitionsEdgeCases:
    def test_backtracks_to_older_candidate(self, s3):
        s3.put("bkt", "root/version=1/run=a/success")
        s3.put("bkt", "root/version=2/run=a/data/x")
        assert _resolve_partitions("bkt", "root", ["version", "run"]) == {
            "version": "1",
            "run": "a",
        }

    def test_no_required_markers_accepts_any_content(self, s3):
        s3.put("bkt", "root/version=1/data/x")
        assert _resolve_partitions("bkt", "root", ["version"], required_markers=()) == {
            "version": "1"
        }

    def test_override_on_non_terminal_partition(self, s3):
        s3.put("bkt", "root/version=1/run=a/success")
        s3.put("bkt", "root/version=2/run=b/success")
        result = _resolve_partitions(
            "bkt", "root", ["version", "run"], overrides={"version": "1"}
        )
        assert result == {"version": "1", "run": "a"}

    def test_override_with_no_viable_deeper_path_raises(self, s3):
        s3.put("bkt", "root/version=1/run=a/data/x")
        with pytest.raises(
            PartitionResolutionError,
            match=r"No run= directory .* under s3://bkt/root/version=1/ "
            r"\(1 candidate\(s\) checked\)",
        ):
            _resolve_partitions(
                "bkt", "root", ["version", "run"], overrides={"version": "1"}
            )

    def test_fully_set_bundle_without_marker_names_the_leaf(self, s3):
        s3.put("bkt", "feeds/provider=p/resource=r/version=1/run=r1/data/x")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="r1"
        )
        with pytest.raises(
            PartitionResolutionError,
            match=r"Override run='r1' missing required markers \['success'\] at "
            r"s3://bkt/feeds/provider=p/resource=r/version=1/run=r1",
        ):
            b.resolve_partitions()

    def test_resolve_with_no_partition_keys_raises(self, s3):
        with pytest.raises(PartitionResolutionError, match="No bundle with required"):
            _resolve_partitions("bkt", "root", [])

    def test_latest_with_nothing_found_raises(self, s3):
        with pytest.raises(PartitionResolutionError, match="No version= directory"):
            _resolve_partitions("bkt", "root", ["version"])

    def test_resolve_partitions_keeps_set_values_and_applies_overrides(self, s3):
        s3.put("bkt", "root/version=1/run=a/success")
        s3.put("bkt", "root/version=1/run=b/success")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1", "run": ""}
        )
        b.resolve_partitions()
        assert b.run == "b"
        b.resolve_partitions(overrides={"run": "a"})
        assert b.run == "a"

    def test_resolve_partitions_noop_without_partitions(self, s3):
        b = Bundle(bucket="bkt", root_path="root")
        b.resolve_partitions()
        assert b.base_uri == "s3://bkt/root"


class TestFindPreviousPartition:
    """Layout: ``feeds/provider=esri/resource=buildings/version=<v>/run=<r>``."""

    ROOT = "feeds/provider=esri/resource=buildings"
    CURRENT_VERSION = "2026-02-01"
    CURRENT_RUN = "20260201T000000"

    def _bundle(self):
        return SourceIngestBundle(
            bucket="bkt",
            provider="esri",
            resource="buildings",
            version=self.CURRENT_VERSION,
            run=self.CURRENT_RUN,
        )

    def _run(self, s3, version, run, success=True):
        prefix = f"{self.ROOT}/version={version}/run={run}"
        s3.put("bkt", f"{prefix}/data/part-0.parquet")
        if success:
            s3.put("bkt", f"{prefix}/success")

    def test_previous_version_preferred_over_same_version_run(self, s3):
        self._run(s3, "2026-01-01", "20260101T000000")
        self._run(s3, self.CURRENT_VERSION, "20260115T000000")
        self._run(s3, self.CURRENT_VERSION, self.CURRENT_RUN)
        found = self._bundle().find_previous_partition(same_value_fallback=True)
        assert found["baseline_kind"] == "previous"
        assert found["data_uri"] == (
            f"s3://bkt/{self.ROOT}/version=2026-01-01/run=20260101T000000/data"
        )

    def test_skips_incomplete_previous_version(self, s3):
        self._run(s3, "2026-01-01", "20260101T000000")
        self._run(s3, "2026-01-15", "20260115T000000", success=False)
        found = self._bundle().find_previous_partition()
        assert "version=2026-01-01" in found["data_uri"]
        assert found["prior_values"] == ["2026-01-15", "2026-01-01"]
        assert found["rejected"] == ["2026-01-15"]

    def test_falls_back_to_earlier_run_of_current_version(self, s3):
        self._run(s3, self.CURRENT_VERSION, "20260110T000000")
        self._run(s3, self.CURRENT_VERSION, "20260120T000000", success=False)
        self._run(s3, self.CURRENT_VERSION, self.CURRENT_RUN)
        found = self._bundle().find_previous_partition(same_value_fallback=True)
        assert found["baseline_kind"] == "same"
        assert found["data_uri"] == (
            f"s3://bkt/{self.ROOT}/version={self.CURRENT_VERSION}"
            "/run=20260110T000000/data"
        )
        assert found["prior_values"] == []

    def test_fallback_ignores_current_and_later_runs(self, s3):
        self._run(s3, self.CURRENT_VERSION, self.CURRENT_RUN)
        self._run(s3, self.CURRENT_VERSION, "20260301T000000")
        found = self._bundle().find_previous_partition(same_value_fallback=True)
        assert found["data_uri"] is None
        assert found["baseline_kind"] is None

    def test_fallback_is_opt_in(self, s3):
        self._run(s3, self.CURRENT_VERSION, "20260115T000000")
        found = self._bundle().find_previous_partition()
        assert found["data_uri"] is None

    def test_unusable_previous_versions_still_reported(self, s3):
        self._run(s3, "2026-01-01", "20260101T000000", success=False)
        self._run(s3, self.CURRENT_VERSION, "20260115T000000")
        found = self._bundle().find_previous_partition(same_value_fallback=True)
        assert found["baseline_kind"] == "same"
        assert found["prior_values"] == ["2026-01-01"]
        assert found["rejected"] == ["2026-01-01"]

    def test_fallback_without_sub_partitions_returns_none(self, s3):
        s3.put("bkt", "root/version=0/data/x")
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        found = b.find_previous_partition(same_value_fallback=True)
        assert found["data_uri"] is None
        assert found["rejected"] == ["0"]

    def test_last_partition_checks_markers_directly(self, s3):
        s3.put("bkt", "root/version=0/success")
        b = Bundle(bucket="bkt", root_path="root", partitions={"version": "1"})
        assert b.resolve_previous_partition() == "s3://bkt/root/version=0/data"

    def test_unknown_partition_raises(self):
        with pytest.raises(ValueError, match="Partition 'theme' not found"):
            self._bundle().find_previous_partition(partition_key="theme")

    def test_unset_target_raises(self):
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt")
        with pytest.raises(ValueError, match="Partition 'version' has no value"):
            b.find_previous_partition()

    def test_unset_earlier_partition_raises(self):
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"theme": "", "run": "r1"}
        )
        with pytest.raises(ValueError, match="Partition 'theme' has no value"):
            b.find_previous_partition(partition_key="run")


# ── Retention / cleanup edge cases ───────────────────────────────────────────


class TestRetentionEdgeCases:
    def _bundle(self, version, run="r1"):
        return Bundle(
            bucket="bkt",
            root_path="root",
            partitions={"version": version, "run": run},
        )

    def test_no_successful_bundle_in_group(self, s3):
        b = self._bundle("1")
        assert b.is_deletable(0, [b]) == (
            False,
            "no successful bundle exists in partition group",
        )

    def test_newer_unsuccessful_bundle_is_kept(self, s3):
        s3.put("bkt", "root/version=1/run=r1/success")
        old, new = self._bundle("1"), self._bundle("2")
        deletable, reason = new.is_deletable(0, [old, new])
        assert deletable is False
        assert "is not older than" in reason

    def test_young_successful_bundle_is_kept(self, s3):
        now = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
        s3.put("bkt", "root/version=1/run=r1/success")
        s3.put("bkt", "root/version=2/run=r1/success")
        old, new = self._bundle("1"), self._bundle("2")
        deletable, reason = old.is_deletable(30, [old, new], now=now)
        assert deletable is False
        assert reason.startswith("age 1.0d < min_age_days")

    def test_missing_last_modified_keeps_bundle(self, s3, monkeypatch):
        s3.put("bkt", "root/version=1/run=r1/success")
        s3.put("bkt", "root/version=2/run=r1/success")
        old, new = self._bundle("1"), self._bundle("2")
        monkeypatch.setattr(Bundle, "age_days", lambda self, now=None: None)
        assert old.is_deletable(30, [old, new]) == (
            False,
            "cannot determine bundle age",
        )

    def test_age_falls_back_to_metadata(self, s3):
        now = dt.datetime(2026, 1, 3, tzinfo=dt.timezone.utc)
        s3.put("bkt", "root/version=1/run=r1/metadata.json", b"{}")
        assert self._bundle("1").age_days(now=now) == 2.0

    def test_recent_incomplete_older_bundle_is_kept(self, s3):
        # An in-flight rerun: initialize removed success and rewrote metadata.
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        s3.put("bkt", "root/version=2/run=r1/success")
        s3.put(
            "bkt",
            "root/version=1/run=r1/metadata.json",
            b"{}",
            lm=now - dt.timedelta(days=1),
        )
        old, new = self._bundle("1"), self._bundle("2")
        assert old.is_deletable(30, [old, new], now=now) == (
            False,
            "age 1.0d < min_age_days 30d",
        )
        assert old.is_deletable(0, [old, new], now=now) == (
            True,
            "unsuccessful; superseded by ('2', 'r1'); age 1.0d >= 0d",
        )

    def test_incomplete_bundle_without_metadata_is_kept(self, s3):
        s3.put("bkt", "root/version=2/run=r1/success")
        s3.put("bkt", "root/version=1/run=r1/data/part-0")
        old, new = self._bundle("1"), self._bundle("2")
        assert old.is_deletable(0, [old, new]) == (
            False,
            "cannot determine bundle age",
        )

    def test_cleanup_all_keeps_in_flight_rerun(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)
        rerun = "ns/feeds/provider=p/resource=r/version=0/run=r1/metadata.json"
        s3.put("bkt", rerun, b"{}", lm=now)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt", min_age_days=30, dry_run=False, namespace="ns", now=now
        )
        deleted = [r.bundle_uri.split("version=")[1][0] for r in report.deleted]
        assert deleted == ["1"]
        assert rerun in s3.keys()

    def test_retention_order_key_without_order_partitions_is_empty(self):
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"purpose": "p", "date": "d"}
        )
        assert b.retention_order_key() == ()

    def test_cleanup_report_summary(self):
        report = CleanupReport(bundle_type="t", dry_run=False)
        report.kept.append(BundleDecision("s3://k", "kept"))
        report.deleted.append(DeleteResult("s3://d", 3, [], False))
        report.deleted.append(DeleteResult("s3://e", 2, [], False))
        assert report.summary() == {
            "bundle_type": "t",
            "dry_run": False,
            "kept": 1,
            "would_delete": 0,
            "deleted": 2,
            "errors": 0,
            "objects_deleted": 5,
        }

    def test_cleanup_all_records_delete_failures(self, s3, monkeypatch):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(s3, now)

        def boom(self, dry_run=False, *, table_dropper=None):
            raise RuntimeError("s3 down")

        monkeypatch.setattr(Bundle, "delete", boom)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt", min_age_days=30, dry_run=False, namespace="ns", now=now
        )
        assert report.deleted == []
        assert len(report.errors) == 2
        assert all("delete failed: s3 down" in e.reason for e in report.errors)

    def test_discover_all_skips_foreign_and_empty_segments(self, s3):
        s3.put("bkt", "theme_stage/theme=places/run=r1/success")
        s3.put("bkt", "theme_stage/theme=/run=r1/success")
        s3.put("bkt", "theme_stage/_tmp/run=r1/success")
        found = ThemeAssembleBundle.discover_all(bucket="bkt")
        assert [b.partitions for b in found] == [{"theme": "places", "run": "r1"}]


# ── Enrichment and scan edge cases ───────────────────────────────────────────


class TestScanEdgeCases:
    def test_enrichment_skips_entries_with_provenance_or_without_ids(
        self, s3, tmp_path
    ):
        meta = RecordingMetadata()
        meta.section["inputs"] = [
            {"path": "s3://src/a", "provider": {}, "resource": {}},
            {"path": "s3://src/b"},
        ]
        meta.init = lambda input_paths=None, capture_variables=True: None
        b = ThemeAssembleBundle(theme="t", bucket="bkt", run="r1")
        b.initialize(
            input_bundles=[Bundle(bucket="src", root_path="b")],
            metadata=meta,
            datasets_dir=tmp_path,
        )
        assert not any(c[0] == "update_inputs" for c in meta.calls)

    def test_needs_processing_ignores_non_run_directories(self, s3):
        s3.put("bkt", "feeds/provider=p/resource=r/version=1/_tmp/success")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="r2"
        )
        assert b.needs_processing() is True

    def test_batch_scans_ignore_non_success_and_partial_keys(self, s3):
        s3.put("bkt", "feeds/provider=p/resource=a/version=1/run=x/data/f")
        s3.put("bkt", "feeds/provider=p/resource=a/success")
        assert (
            SourceIngestBundle.resolve_partitions_batch(provider="p", bucket="bkt")
            == {}
        )
        todo = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="x"
        )
        assert Bundle.needs_processing_batch([todo]) == [todo]


# ── Natural ordering of partition values ─────────────────────────────────────


class TestNaturalOrder:
    def test_natural_key_orders_numbers_numerically(self):
        values = ["v1.16.0", "v1.9.0", "v1.10.0", "v2.0.0"]
        assert sorted(values, key=_natural_key) == [
            "v1.9.0",
            "v1.10.0",
            "v1.16.0",
            "v2.0.0",
        ]

    def test_natural_key_keeps_fixed_width_order(self):
        values = [
            "2026-01-15",
            "2025-12-31",
            "20260101T000000",
            "manual__2026-01-01T00:00:00+00:00",
            "scheduled__2025-12-01T00:00:00+00:00",
        ]
        assert sorted(values, key=_natural_key) == sorted(values)
        assert _natural_key("01") != _natural_key("1")

    def _promote(self, s3, schema, lm=None):
        s3.put("bkt", f"theme_promote/theme=t/schema={schema}/run=r1/success", lm=lm)

    def test_resolve_latest_schema(self, s3):
        for schema in ("v1.9.0", "v1.16.0", "v1.10.0"):
            self._promote(s3, schema)
        b = ThemePromoteBundle(theme="t", bucket="bkt")
        b.resolve_partitions()
        assert b.partitions == {"theme": "t", "schema": "v1.16.0", "run": "r1"}

    def test_cleanup_keeps_newest_schema(self, s3):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        old = now - dt.timedelta(days=90)
        for schema in ("v1.9.0", "v1.16.0"):
            self._promote(s3, schema, lm=old)
        report = ThemePromoteBundle.cleanup_all(
            bucket="bkt", min_age_days=30, dry_run=True, now=now
        )
        assert [d.bundle_uri for d in report.would_delete] == [
            "s3://bkt/theme_promote/theme=t/schema=v1.9.0/run=r1"
        ]
        assert [d.bundle_uri for d in report.kept] == [
            "s3://bkt/theme_promote/theme=t/schema=v1.16.0/run=r1"
        ]

    def test_find_previous_partition(self, s3):
        for schema in ("v1.9.0", "v1.10.0", "v1.20.0"):
            self._promote(s3, schema)
        b = ThemePromoteBundle(theme="t", bucket="bkt", schema="v1.16.0", run="r1")
        found = b.find_previous_partition(partition_key="schema")
        assert found["prior_values"] == ["v1.10.0", "v1.9.0"]
        assert "schema=v1.10.0" in found["data_uri"]

    def test_same_value_fallback_orders_runs(self, s3):
        s3.put("bkt", "root/version=1/run=r9/success")
        s3.put("bkt", "root/version=1/run=r11/success")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1", "run": "r10"}
        )
        found = b.find_previous_partition(same_value_fallback=True)
        assert found["data_uri"] == "s3://bkt/root/version=1/run=r9/data"

    def test_resolve_partitions_batch(self, s3):
        for version in ("v1.9.0", "v1.16.0"):
            s3.put(
                "bkt", f"feeds/provider=p/resource=a/version={version}/run=r1/success"
            )
        out = SourceIngestBundle.resolve_partitions_batch(provider="p", bucket="bkt")
        assert out["a"].partitions["version"] == "v1.16.0"


# ── S3 client reuse ──────────────────────────────────────────────────────────


@pytest.fixture
def counted_s3(monkeypatch) -> tuple[FakeS3, list[tuple]]:
    """``FakeS3`` plus a record of every ``boto3.client`` call."""
    fake = FakeS3()
    calls: list[tuple] = []

    def _client(*args, **_kw):
        calls.append(args)
        return fake

    monkeypatch.setattr(boto3, "client", _client)
    return fake, calls


class TestClientReuse:
    """A walk or check opens one S3 client, however many requests it makes."""

    _ROOT = "feeds/provider=p/resource=r"

    def _seed(self, fake: FakeS3) -> None:
        for version in ("1", "2", "3"):
            for run in ("a", "b"):
                fake.put("bkt", f"{self._ROOT}/version={version}/run={run}/success")
        fake.put("bkt", "feeds/provider=p/resource=s/version=1/run=a/success")

    def test_discover_all(self, counted_s3):
        fake, calls = counted_s3
        self._seed(fake)
        assert len(SourceIngestBundle.discover_all(bucket="bkt")) == 7
        assert calls == [("s3",)]

    def test_resolve_partitions(self, counted_s3):
        fake, calls = counted_s3
        self._seed(fake)
        fake.put("bkt", f"{self._ROOT}/version=4/run=a/data/f")
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt")
        b.resolve_partitions()
        assert b.base_uri == f"s3://bkt/{self._ROOT}/version=3/run=b"
        assert calls == [("s3",)]

    def test_needs_processing(self, counted_s3):
        fake, calls = counted_s3
        for run in ("a", "b", "c"):
            fake.put("bkt", f"{self._ROOT}/version=1/run={run}/data/f")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="d"
        )
        assert b.needs_processing() is True
        assert calls == [("s3",)]

    def test_find_previous_partition(self, counted_s3):
        fake, calls = counted_s3
        self._seed(fake)
        fake.put("bkt", f"{self._ROOT}/version=3/run=c/data/f")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="4", run="a"
        )
        found = b.find_previous_partition()
        assert found["data_uri"] == f"s3://bkt/{self._ROOT}/version=3/run=b/data"
        assert calls == [("s3",)]

    def test_needs_processing_batch(self, counted_s3):
        fake, calls = counted_s3
        self._seed(fake)
        bundles = [
            SourceIngestBundle(
                provider="p", resource=res, bucket="bkt", version="9", run="z"
            )
            for res in ("r", "s")
        ] + [
            ThemeAssembleBundle(theme=theme, bucket="bkt", run="z")
            for theme in ("places", "buildings")
        ]
        assert Bundle.needs_processing_batch(bundles) == bundles
        assert calls == [("s3",)]
