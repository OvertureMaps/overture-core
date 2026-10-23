"""Tests for the bundle model."""

import dataclasses
import datetime as dt
import json

import pytest

from overture_core.bundle.bundle import (
    Bundle,
    BundleComponent,
    BundleDecision,
    BundleStatus,
    BundleStatusReport,
    CleanupReport,
    ComponentKind,
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
from overture_core.bundle.store import InMemoryBundleStore, S3BundleStore

_DEFAULT_LM = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


def keys(store: InMemoryBundleStore, bucket: str = "bkt") -> set[str]:
    """Every object key in *bucket*."""
    return set(store.iter_keys(bucket, ""))


# ── Fakes ────────────────────────────────────────────────────────────────────


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


class StoreMetadata:
    """`BundleMetadata` keeping one DAG's ``metadata.json`` section in a store.

    Mirrors tf-data-platform's ``Metadata``.
    """

    def __init__(self, bundle: Bundle, dag_id: str = "dag") -> None:
        self.store = bundle.store
        self.bucket = bundle.bucket
        self.key = f"{bundle.base_prefix}/metadata.json"
        self.dag_id = dag_id

    def _read(self) -> dict:
        body = self.store.read(self.bucket, self.key)
        return json.loads(body) if body is not None else {}

    def _update(self, updates: dict) -> None:
        document = self._read()
        document.setdefault(self.dag_id, {}).update(updates)
        self.store.write(self.bucket, self.key, json.dumps(document).encode())

    def init(self, input_paths=None, capture_variables=True):
        self._update({"status": "running", "completed_at": None})

    def add(self, key, value):
        self._update({key: value})

    def read_dag_section(self):
        return self._read().get(self.dag_id, {})

    def update_inputs(self, inputs):
        self._update({"inputs": inputs})

    def finalize(self, status="success"):
        self._update({"status": status, "completed_at": "2026-01-01T00:00:00Z"})


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


# ── Lifecycle ────────────────────────────────────────────────────────────────


class TestLifecycle:
    def test_initialize_deletes_success_and_writes_metadata(self, store):
        store.write("bkt", "theme_stage/theme=places/run=r1/success")
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="acme",
            resource="widgets",
            bucket="src",
            version="1",
            run="a",
            store=store,
        )
        b = ThemeAssembleBundle(theme="places", bucket="bkt", run="r1", store=store)
        b.initialize(
            input_bundles=[upstream],
            capture_variables=False,
            custom_metadata={"k": "v"},
            metadata=meta,
        )
        assert "theme_stage/theme=places/run=r1/success" not in keys(store)
        assert meta.calls == [
            ("init", [upstream.base_uri], False),
            ("add", "bundle_type", "theme_assemble"),
            ("add", "k", "v"),
        ]

    def test_initialize_enriches_inputs_when_datasets_dir_given(self, store, tmp_path):
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
            provider="acme",
            resource="widgets",
            bucket="src",
            version="7",
            run="a",
            store=store,
        )
        b = ThemeAssembleBundle(theme="places", bucket="bkt", run="r1", store=store)
        b.initialize(input_bundles=[upstream], metadata=meta, datasets_dir=tmp_path)
        (inp,) = meta.section["inputs"]
        assert inp["provider"]["label"] == "acme"
        assert inp["resource"]["label"] == "widgets"
        assert inp["resource"]["version"] == "7"

    def test_initialize_skips_enrichment_without_datasets_dir(self, store):
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="acme",
            resource="widgets",
            bucket="src",
            version="7",
            run="a",
            store=store,
        )
        ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=store).initialize(
            input_bundles=[upstream], metadata=meta
        )
        assert not any(c[0] == "update_inputs" for c in meta.calls)

    def test_enrichment_failure_is_not_fatal(self, store, tmp_path):
        meta = RecordingMetadata()
        upstream = SourceIngestBundle(
            provider="missing",
            resource="x",
            bucket="src",
            version="1",
            run="a",
            store=store,
        )
        ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=store).initialize(
            input_bundles=[upstream], metadata=meta, datasets_dir=tmp_path
        )
        assert meta.section["inputs"] == [{"path": upstream.base_uri}]

    def test_finalize_writes_success(self, store):
        meta = RecordingMetadata()
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1", store=store)
        b.finalize(metadata=meta)
        assert meta.calls == [("finalize", "success")]
        assert "scratch/purpose=p/run=r1/success" in keys(store)

    @pytest.mark.parametrize("status", ["failed", BundleStatus.FAILED, "partial"])
    def test_finalize_without_success_writes_no_marker(self, store, status):
        meta = RecordingMetadata()
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1", store=store)
        b.finalize(status=status, metadata=meta)
        assert meta.calls == [("finalize", status)]
        assert keys(store) == set()
        assert b.is_successful() is False

    def test_finalize_accepts_status_enum(self, store):
        meta = RecordingMetadata()
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1", store=store)
        b.finalize(status=BundleStatus.SUCCESS, metadata=meta)
        assert meta.calls == [("finalize", "success")]
        assert b.is_successful() is True

    def test_batch_lifecycle_uses_metadata_factory(self, store):
        bundles = [
            ScratchBundle(purpose=p, bucket="bkt", run="r1", store=store) for p in "ab"
        ]
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
        } <= keys(store)

    def test_finalize_batch_failed_writes_no_markers(self, store):
        bundles = [
            ScratchBundle(purpose=p, bucket="bkt", run="r1", store=store) for p in "ab"
        ]
        made: list[RecordingMetadata] = []

        def factory(_b):
            made.append(RecordingMetadata())
            return made[-1]

        Bundle.finalize_batch(
            bundles, metadata_factory=factory, status="failed", max_workers=2
        )
        assert [m.calls for m in made] == [[("finalize", "failed")]] * 2
        assert keys(store) == set()

    def test_batch_noop_on_empty(self):
        Bundle.initialize_batch([], metadata_factory=lambda b: RecordingMetadata())
        Bundle.finalize_batch([], metadata_factory=lambda b: RecordingMetadata())


# ── Status ───────────────────────────────────────────────────────────────────

_RUN = "theme_stage/theme=t/run=r1"


def _metadata(**sections) -> bytes:
    """A ``metadata.json`` body with the given top-level entries."""
    return json.dumps(sections).encode()


class CountingStore(InMemoryBundleStore):
    """`InMemoryBundleStore` that records each lookup `Bundle.status` makes."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    def exists(self, bucket, key):
        self.calls.append("exists")
        return super().exists(bucket, key)

    def read(self, bucket, key):
        self.calls.append("read")
        return super().read(bucket, key)

    def prefix_exists(self, bucket, prefix):
        self.calls.append("prefix_exists")
        return super().prefix_exists(bucket, prefix)


class TestStatus:
    @staticmethod
    def _bundle(store) -> ThemeAssembleBundle:
        return ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=store)

    def test_values(self):
        assert [s.value for s in BundleStatus] == [
            "in_progress",
            "success",
            "failed",
            "released",
        ]
        assert BundleStatus.FAILED == "failed"
        assert json.dumps({"status": BundleStatus.SUCCESS}) == '{"status": "success"}'

    def test_report_is_frozen_and_hashable(self):
        report = BundleStatusReport(
            BundleStatus.FAILED, True, False, True, {"dag": "failed"}
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            report.status = None  # type: ignore[misc]
        assert hash(report) == hash(
            BundleStatusReport(BundleStatus.FAILED, True, False, True)
        )

    def test_missing_bundle_has_no_status(self, store):
        report = self._bundle(store).status()
        assert report == BundleStatusReport(
            status=None, exists=False, has_success=False, has_metadata=False
        )
        assert report.sections == {}

    def test_sibling_prefix_is_not_the_bundle(self, store):
        store.write("bkt", "theme_stage/theme=t/run=r10/success")
        assert self._bundle(store).status().exists is False

    def test_success_marker(self, store):
        store.write("bkt", f"{_RUN}/success")
        assert self._bundle(store).status() == BundleStatusReport(
            status=BundleStatus.SUCCESS,
            exists=True,
            has_success=True,
            has_metadata=False,
        )

    def test_success_marker_wins_over_failed_metadata(self, store):
        store.write("bkt", f"{_RUN}/success")
        store.write("bkt", f"{_RUN}/metadata.json", _metadata(dag={"status": "failed"}))
        report = self._bundle(store).status()
        assert report.status is BundleStatus.SUCCESS
        assert report.sections == {"dag": "failed"}

    @pytest.mark.parametrize(
        ("sections", "expected"),
        [
            ({"dag": {"status": "running"}}, BundleStatus.IN_PROGRESS),
            ({"dag": {"status": "success"}}, BundleStatus.IN_PROGRESS),
            ({"dag": {"status": "partial"}}, BundleStatus.IN_PROGRESS),
            ({}, BundleStatus.IN_PROGRESS),
            ({"dag": {"status": "failed"}}, BundleStatus.FAILED),
            (
                {"a": {"status": "success"}, "b": {"status": "failed"}},
                BundleStatus.FAILED,
            ),
        ],
    )
    def test_metadata_without_marker(self, store, sections, expected):
        store.write("bkt", f"{_RUN}/metadata.json", _metadata(**sections))
        report = self._bundle(store).status()
        assert report.status is expected
        assert (report.exists, report.has_success, report.has_metadata) == (
            True,
            False,
            True,
        )
        assert report.sections == {k: v["status"] for k, v in sections.items()}

    def test_objects_without_marker_or_metadata(self, store):
        store.write("bkt", f"{_RUN}/data/part-0.parquet", b"x")
        assert self._bundle(store).status() == BundleStatusReport(
            status=BundleStatus.IN_PROGRESS,
            exists=True,
            has_success=False,
            has_metadata=False,
        )

    def test_malformed_sections_are_tolerated(self, store):
        body = _metadata(
            ok={"status": "running"},
            no_status={"run_id": "x"},
            bad_status={"status": ["failed"]},
            not_a_section="theme_assemble",
        )
        store.write("bkt", f"{_RUN}/metadata.json", body)
        report = self._bundle(store).status()
        assert report.status is BundleStatus.IN_PROGRESS
        assert report.sections == {
            "ok": "running",
            "no_status": None,
            "bad_status": None,
        }

    @pytest.mark.parametrize(
        ("body", "message"),
        [
            (b"{not json", "Unreadable metadata.json"),
            (b"\x80not utf-8", "Unreadable metadata.json"),
            (b'["failed"]', "is not a JSON object"),
            (b"null", "is not a JSON object"),
        ],
    )
    def test_unreadable_metadata(self, store, caplog, body, message):
        store.write("bkt", f"{_RUN}/metadata.json", body)
        report = self._bundle(store).status()
        assert report == BundleStatusReport(
            status=BundleStatus.IN_PROGRESS,
            exists=True,
            has_success=False,
            has_metadata=True,
        )
        assert f"s3://bkt/{_RUN}/metadata.json" in caplog.text
        assert message in caplog.text

    def test_follows_the_lifecycle(self, store):
        b = self._bundle(store)
        b.initialize(metadata=StoreMetadata(b))
        assert b.status().status is BundleStatus.IN_PROGRESS

        b.finalize(status="failed", metadata=StoreMetadata(b))
        report = b.status()
        assert report.status is BundleStatus.FAILED
        assert report.sections == {"dag": "failed"}
        assert b.is_successful() is False

        b.initialize(metadata=StoreMetadata(b))  # rerun
        assert b.status().status is BundleStatus.IN_PROGRESS

        b.finalize(metadata=StoreMetadata(b))
        assert b.status() == BundleStatusReport(
            status=BundleStatus.SUCCESS,
            exists=True,
            has_success=True,
            has_metadata=True,
            sections={"dag": "success"},
        )

        b.initialize(metadata=StoreMetadata(b, dag_id="other"))
        report = b.status()
        assert report.status is BundleStatus.IN_PROGRESS
        assert report.sections == {"dag": "success", "other": "running"}

    def test_namespaced_bundle(self, store):
        b = SourceIngestBundle(
            provider="p",
            resource="r",
            bucket="bkt",
            version="1",
            run="a",
            namespace="ns",
            store=store,
        )
        store.write(
            "bkt",
            "ns/feeds/provider=p/resource=r/version=1/run=a/metadata.json",
            _metadata(dag={"status": "failed"}),
        )
        assert b.status().status is BundleStatus.FAILED

    def test_bundle_at_bucket_root(self, store):
        b = Bundle(bucket="bkt", root_path="", store=store)
        assert b.status().exists is False
        store.write("bkt", "metadata.json", _metadata(dag={"status": "failed"}))
        assert b.status().status is BundleStatus.FAILED
        store.write("bkt", "success")
        assert b.status().status is BundleStatus.SUCCESS

    def test_requests(self):
        store = CountingStore()
        b = self._bundle(store)
        b.status()
        assert store.calls == ["exists", "read", "prefix_exists"]

        store.calls.clear()
        store.write("bkt", f"{_RUN}/success")
        b.status()
        assert store.calls == ["exists", "read"]


_RC = "release_candidate/release=R/run=r1"


class TestReleasedStatus:
    @staticmethod
    def _bundle(store, **kwargs) -> ReleaseCandidateBundle:
        return ReleaseCandidateBundle(
            bucket="bkt", release="R", run="r1", store=store, **kwargs
        )

    def test_success_and_released_markers(self, store):
        store.write("bkt", f"{_RC}/success")
        store.write("bkt", f"{_RC}/released")
        assert self._bundle(store).status() == BundleStatusReport(
            status=BundleStatus.RELEASED,
            exists=True,
            has_success=True,
            has_metadata=False,
        )

    def test_success_without_released_marker(self, store):
        store.write("bkt", f"{_RC}/success")
        assert self._bundle(store).status().status is BundleStatus.SUCCESS

    @pytest.mark.parametrize(
        ("sections", "expected"),
        [
            ({}, BundleStatus.IN_PROGRESS),
            ({"dag": {"status": "failed"}}, BundleStatus.FAILED),
        ],
    )
    def test_released_marker_needs_success(self, store, sections, expected):
        store.write("bkt", f"{_RC}/released")
        if sections:
            store.write("bkt", f"{_RC}/metadata.json", _metadata(**sections))
        assert self._bundle(store).status().status is expected

    def test_missing_bundle(self, store):
        assert self._bundle(store).status().status is None

    def test_namespaced(self, store):
        store.write("bkt", f"ns/{_RC}/success")
        store.write("bkt", f"ns/{_RC}/released")
        assert self._bundle(store, namespace="ns").status().status is (
            BundleStatus.RELEASED
        )

    def test_other_types_ignore_a_released_marker(self, store):
        store.write("bkt", f"{_RUN}/success")
        store.write("bkt", f"{_RUN}/released")
        b = ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=store)
        assert b.status().status is BundleStatus.SUCCESS

    def test_matches_resolve_latest_released(self, store):
        for run in ("r1", "r2"):
            store.write("bkt", f"release_candidate/release=R/run={run}/success")
        store.write("bkt", f"{_RC}/released")
        latest = ReleaseCandidateBundle.resolve_latest_released(
            release="R", bucket="bkt", store=store
        )
        assert latest.run == "r1"
        assert latest.status().status is BundleStatus.RELEASED

    def test_released_uri(self):
        b = self._bundle(None)
        assert b.released_uri == f"s3://bkt/{_RC}/released"

    def test_requests(self):
        store = CountingStore()
        b = self._bundle(store)
        store.write("bkt", f"{_RC}/released")
        b.status()
        assert store.calls == ["exists", "read", "prefix_exists"]

        store.calls.clear()
        store.write("bkt", f"{_RC}/success")
        b.status()
        assert store.calls == ["exists", "read", "exists"]


# ── Partition resolution ─────────────────────────────────────────────────────


class TestResolvePartitions:
    def test_finds_latest_successful_version_and_run(self, store):
        for v, r, ok in (("1", "a", True), ("2", "a", True), ("2", "b", False)):
            key = f"feeds/provider=p/resource=r/version={v}/run={r}"
            store.write("bkt", f"{key}/data/x.parquet")
            if ok:
                store.write("bkt", f"{key}/success")
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt", store=store)
        b.resolve_partitions()
        assert (b.version, b.run) == ("2", "a")

    def test_override_and_missing_raise(self, store):
        store.write("bkt", "root/version=1/success")
        assert _resolve_partitions(
            "bkt", "root", ["version"], {"version": "1"}, store=store
        ) == {"version": "1"}
        with pytest.raises(PartitionResolutionError):
            _resolve_partitions(
                "bkt", "root", ["version"], {"version": "9"}, store=store
            )

    def test_resolve_latest_released_with_namespace(self, store):
        for run, released in (("r1", True), ("r2", True), ("r3", False)):
            prefix = f"ns/release_candidate/release=R/run={run}"
            store.write("bkt", f"{prefix}/success")
            if released:
                store.write("bkt", f"{prefix}/released")
        rc = ReleaseCandidateBundle.resolve_latest_released(
            release="R", bucket="bkt", namespace="ns", store=store
        )
        assert isinstance(rc, ReleaseCandidateBundle)
        assert rc.base_uri == "s3://bkt/ns/release_candidate/release=R/run=r2"

    def test_resolve_latest_released_none_raises(self, store):
        store.write("bkt", "release_candidate/release=R/run=r1/success")
        with pytest.raises(PartitionResolutionError):
            ReleaseCandidateBundle.resolve_latest_released(
                release="R", bucket="bkt", store=store
            )


# ── Retention / delete / cleanup ─────────────────────────────────────────────


def _seed_versions(store: InMemoryBundleStore, now: dt.datetime) -> None:
    """v0 failed, v1 old success, v2 fresh success, under namespace ``ns``."""
    old = now - dt.timedelta(days=90)
    for v, ok in (("0", False), ("1", True), ("2", True)):
        base = f"ns/feeds/provider=p/resource=r/version={v}/run=r1"
        store.write("bkt", f"{base}/metadata.json", b"{}")
        store.write("bkt", f"{base}/data/part-0.parquet", b"x")
        if ok:
            store.write(
                "bkt", f"{base}/success", last_modified=old if v == "1" else now
            )


class TestRetention:
    def test_is_successful_and_age(self, store):
        now = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
        assert b.is_successful() is False
        assert b.age_days() is None
        store.write("bkt", "root/version=1/success", last_modified=_DEFAULT_LM)
        assert b.is_successful() is True
        assert b.age_days(now=now) == 31.0

    def test_is_deletable_rules(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        bs = [
            SourceIngestBundle(
                provider="p",
                resource="r",
                bucket="bkt",
                version=v,
                run="r1",
                namespace="ns",
                store=store,
            )
            for v in "012"
        ]
        decisions = [b.is_deletable(30, bs, now=now)[0] for b in bs]
        assert decisions == [True, True, False]
        assert bs[1].is_deletable(365, bs, now=now)[0] is False

    def test_discover_all_honours_namespace(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        assert SourceIngestBundle.discover_all(bucket="bkt", store=store) == []
        found = SourceIngestBundle.discover_all(
            bucket="bkt", namespace="ns", store=store
        )
        assert sorted(b.partitions["version"] for b in found) == ["0", "1", "2"]
        assert all(b.BUNDLE_TYPE == "source_ingest" for b in found)
        assert all(b.root_uri == "s3://bkt/ns/feeds" for b in found)

    @pytest.mark.parametrize("cls", [Bundle, ScratchBundle])
    def test_discover_all_requires_root_and_keys(self, cls):
        with pytest.raises(NotImplementedError):
            cls.discover_all(bucket="bkt")

    def test_cleanup_all_dry_run(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        dropper = RecordingDropper()
        before = set(keys(store))
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=True,
            namespace="ns",
            table_dropper=dropper,
            now=now,
            store=store,
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
        assert keys(store) == before

    def test_cleanup_all_execute_deletes_and_drops_tables(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        dropper = RecordingDropper()
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=False,
            namespace="ns",
            table_dropper=dropper,
            now=now,
            store=store,
        )
        assert len(report.deleted) == 2
        assert all(r.deleted_tables == ["db.tbl"] for r in report.deleted)
        assert sorted(c["base_prefix"] for c in dropper.calls) == [
            "ns/feeds/provider=p/resource=r/version=0/run=r1",
            "ns/feeds/provider=p/resource=r/version=1/run=r1",
        ]
        remaining = {k.split("/version=")[1][0] for k in keys(store)}
        assert remaining == {"2"}

    def test_delete_dry_run_counts_without_deleting(self, store):
        store.write("bkt", "root/version=1/a")
        store.write("bkt", "root/version=1/b/c")
        store.write("bkt", "root/version=10/other")
        dropper = RecordingDropper()
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
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
        assert len(keys(store)) == 3

    def test_delete_only_removes_own_prefix(self, store):
        store.write("bkt", "root/version=1/a")
        store.write("bkt", "root/version=10/other")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
        result = b.delete()
        assert result.deleted_object_count == 1
        assert result.deleted_tables == []
        assert keys(store) == {"root/version=10/other"}

    def test_delete_table_dropper_failure_is_not_fatal(self, store):
        store.write("bkt", "root/version=1/a")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
        result = b.delete(table_dropper=RecordingDropper(fail=True))
        assert result.deleted_object_count == 1
        assert result.deleted_tables == []

    def test_delete_refuses_bundle_at_bucket_root(self, store):
        with pytest.raises(ValueError, match="has no partitions"):
            Bundle(bucket="bkt", root_path="", store=store).delete()

    @pytest.mark.parametrize(
        "bundle",
        [
            Bundle(bucket="bkt", root_path="datasets"),
            Bundle(bucket="bkt", root_path="feeds"),
            Bundle(bucket="bkt", root_path="feeds", namespace="alice"),
        ],
        ids=["datasets", "feeds", "namespace-root"],
    )
    def test_delete_refuses_bundle_without_partitions(self, store, bundle):
        store.write("bkt", "datasets/provider=p/resource=r/version=1/run=r1/success")
        store.write("bkt", "feeds/provider=p/resource=r/version=1/run=r1/success")
        store.write("bkt", "alice/feeds/provider=p/resource=r/version=1/run=r1/success")
        dropper = RecordingDropper()
        with pytest.raises(ValueError, match="has no partitions"):
            bundle.delete(table_dropper=dropper)
        assert len(keys(store)) == 3
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
    def test_delete_refuses_unset_declared_partitions(self, store, bundle, missing):
        for root in ("feeds", "ns/feeds"):
            store.write("bkt", f"{root}/provider=p/resource=r/version=1/run=r1/success")
        dropper = RecordingDropper()
        with pytest.raises(ValueError, match=f"partition\\(s\\) {missing} have no"):
            bundle.delete(dry_run=False, table_dropper=dropper)
        assert len(keys(store)) == 2
        assert dropper.calls == []

    def test_cleanup_all_checks_success_once_per_bundle(self, store, monkeypatch):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        checked: list[str] = []
        original = Bundle.is_successful

        def counting(self):
            checked.append(self.base_uri)
            return original(self)

        monkeypatch.setattr(Bundle, "is_successful", counting)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=True,
            namespace="ns",
            now=now,
            store=store,
        )
        assert len(report.would_delete) == 2
        assert len(checked) == 3
        assert len(set(checked)) == 3

    def test_is_deletable_uses_success_snapshot(self, store):
        now = dt.datetime(2026, 2, 1, tzinfo=dt.timezone.utc)
        store.write("bkt", "root/version=1/metadata.json", b"{}")
        old, new = (
            Bundle(
                bucket="bkt", root_path="root", partitions={"version": v}, store=store
            )
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

    def _bundle(self, run="r2", *, store=None):
        return SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run=run, store=store
        )

    def test_non_latest_override_forces_true(self, store):
        assert self._bundle(store=store).needs_processing({"version": "1"}) is True

    def test_no_runs_returns_true(self, store):
        assert self._bundle(store=store).needs_processing({"version": "latest"}) is True

    def test_other_successful_run_returns_false(self, store):
        store.write("bkt", f"{self._PARENT}/run=r1/success")
        assert self._bundle(store=store).needs_processing() is False

    def test_unsuccessful_run_returns_true(self, store):
        store.write("bkt", f"{self._PARENT}/run=r1/data/x")
        assert self._bundle(store=store).needs_processing() is True

    def test_same_run_rerun_returns_true(self, store):
        store.write("bkt", f"{self._PARENT}/run=r2/success")
        assert self._bundle(store=store).needs_processing() is True

    def test_rerun_returns_true_when_an_earlier_run_also_succeeded(self, store):
        store.write("bkt", f"{self._PARENT}/run=r1/success")
        store.write("bkt", f"{self._PARENT}/run=r2/success")
        assert self._bundle("r2", store=store).needs_processing() is True
        assert self._bundle("r0", store=store).needs_processing() is False

    def test_explicit_base_uri_is_checked(self, store):
        store.write("other", f"{self._PARENT}/run=r1/success")
        b = self._bundle(store=store)
        assert b.needs_processing() is True
        rendered = b.base_uri.replace("s3://bkt/", "s3://other/")
        assert b.needs_processing(base_uri=rendered) is False

    def test_similarly_named_artifacts_are_not_success_markers(self, store):
        store.write("bkt", f"{self._PARENT}/run=r1/success.log")
        store.write("bkt", f"{self._PARENT}/run=r1/success/part-0")
        assert self._bundle("r2", store=store).needs_processing() is True

    def test_current_run_artifact_is_not_a_rerun(self, store):
        store.write("bkt", f"{self._PARENT}/run=r1/success")
        store.write("bkt", f"{self._PARENT}/run=r2/success.log")
        assert self._bundle("r2", store=store).needs_processing() is False


class TestBatchHelpers:
    def test_resolve_partitions_batch_picks_latest_per_resource(self, store):
        for res, v, r in (
            ("a", "1", "x"),
            ("a", "2", "x"),
            ("a", "2", "y"),
            ("b", "1", "x"),
        ):
            store.write(
                "bkt", f"ns/feeds/provider=p/resource={res}/version={v}/run={r}/success"
            )
        store.write("bkt", "ns/feeds/provider=p/resource=a/version=3/run=x/data/f")
        out = SourceIngestBundle.resolve_partitions_batch(
            provider="p", bucket="bkt", namespace="ns", store=store
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

    def test_resolve_partitions_batch_requires_exact_layout(self, store):
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
            store.write("bkt", key)
        out = SourceIngestBundle.resolve_partitions_batch(
            provider="p", bucket="bkt", store=store
        )
        assert out["r"].base_uri == f"s3://bkt/{base}/version=1/run=a"
        b = SourceIngestBundle(provider="p", resource="r", bucket="bkt", store=store)
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
    def test_needs_processing_batch_single_marker(self, store, marker, needs):
        store.write("bkt", f"feeds/provider=p/resource=a/version=1/{marker}")
        b = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="r2", store=store
        )
        assert b.needs_processing() is needs
        assert Bundle.needs_processing_batch([b], store=store) == ([b] if needs else [])

    def test_needs_processing_batch(self, store):
        store.write("bkt", "feeds/provider=p/resource=a/version=1/run=x/success")
        done = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="z", store=store
        )
        todo = SourceIngestBundle(
            provider="p", resource="b", bucket="bkt", version="1", run="z", store=store
        )
        assert Bundle.needs_processing_batch([done, todo], store=store) == [todo]
        assert Bundle.needs_processing_batch(
            [done, todo], {"version": "1"}, store=store
        ) == [
            done,
            todo,
        ]
        assert Bundle.needs_processing_batch([], store=store) == []

    def test_needs_processing_batch_matches_singular(self, store):
        for key in (
            "feeds/provider=p/resource=a/version=1/run=x/success",
            "feeds/provider=p/resource=a/version=1/run=y/success",
            "feeds/provider=p/resource=b/version=1/run=x/data/f",
            "feeds/provider=p/resource=c/version=10/run=x/success",
            "theme_stage/theme=places/run=r1/success",
        ):
            store.write("bkt", key)

        def ingest(resource, run, version="1"):
            return SourceIngestBundle(
                provider="p",
                resource=resource,
                bucket="bkt",
                version=version,
                run=run,
                store=store,
            )

        bundles = [
            ingest("a", "z"),  # another run succeeded -> skip
            ingest("a", "y"),  # rerun of a successful run -> process
            ingest("b", "z"),  # no successful run -> process
            ingest("c", "z"),  # only version=10 succeeded -> process
            ThemeAssembleBundle(
                theme="places", bucket="bkt", run="r2", store=store
            ),  # skip
            ThemeAssembleBundle(
                theme="places", bucket="bkt", run="r1", store=store
            ),  # rerun
            ThemeAssembleBundle(
                theme="roads", bucket="bkt", run="r1", store=store
            ),  # process
        ]
        expected = [b for b in bundles if b.needs_processing()]
        assert [b.base_uri for b in expected] == [
            bundles[i].base_uri for i in (1, 2, 3, 5, 6)
        ]
        assert Bundle.needs_processing_batch(bundles, store=store) == expected


# ── Path helpers ─────────────────────────────────────────────────────────────


class TestPathHelpers:
    def _bundle(self, run="r1", *, store=None):
        return Bundle(
            bucket="bkt",
            root_path="root",
            partitions={"version": "1", "run": run},
            store=store,
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


# ── Components ───────────────────────────────────────────────────────────────

_TYPES = [
    SourceRawBundle,
    SourceIngestBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
    ReleaseCandidateBundle,
    ScratchBundle,
]


def _instance(cls: type[Bundle]) -> Bundle:
    """An instance of *cls* with every partition set, under namespace ``ns``."""
    return cls._from_root(
        "bkt",
        f"ns/{cls.ROOT_PATH or 'scratch'}",
        {key: "x" for key in cls.PARTITION_KEYS},
    )


class TestBundleComponent:
    def test_directory_is_the_default_kind(self):
        assert BundleComponent("data") == BundleComponent.directory("data")
        assert BundleComponent("data").kind is ComponentKind.DIRECTORY

    def test_kinds(self):
        directory = BundleComponent.directory("changelog")
        file = BundleComponent.file("metadata.json")
        assert (directory.is_directory, file.is_directory) == (True, False)
        assert (directory.listing_name, file.listing_name) == (
            "changelog/",
            "metadata.json",
        )
        assert [k.value for k in ComponentKind] == ["directory", "file"]

    def test_kind_from_string(self):
        assert BundleComponent("success", "file").kind is ComponentKind.FILE  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            BundleComponent("success", "link")  # type: ignore[arg-type]

    @pytest.mark.parametrize("name", ["", "data/", "a/b"])
    def test_name_is_one_segment(self, name):
        with pytest.raises(ValueError, match="one non-empty path segment"):
            BundleComponent(name)

    def test_frozen_and_hashable(self):
        component = BundleComponent.file("success")
        with pytest.raises(dataclasses.FrozenInstanceError):
            component.name = "x"  # type: ignore[misc]
        assert {component, BundleComponent.file("success")} == {component}


class TestComponents:
    def test_base_components(self):
        assert [c.listing_name for c in Bundle.COMPONENTS] == [
            "data/",
            "metadata.json",
            "success",
        ]

    @pytest.mark.parametrize(
        ("cls", "extra"),
        [
            (SourceRawBundle, []),
            (SourceIngestBundle, ["changelog/", "metrics/", "metrics_summary/"]),
            (
                ThemeAssembleBundle,
                ["changelog/", "metrics/", "metrics_summary/", "summary.md"],
            ),
            (
                ThemePromoteBundle,
                [
                    "bridgefiles/",
                    "pmtiles/",
                    "changelog/",
                    "metrics/",
                    "metrics_summary/",
                    "changelog_internal/",
                    "validation/",
                    "partition_bboxes/",
                ],
            ),
            (
                ReleaseCandidateBundle,
                [
                    "released",
                    "bridgefiles/",
                    "pmtiles/",
                    "changelog/",
                    "metrics/",
                    "metrics_summary/",
                    "partition_bboxes/",
                    "registry/",
                ],
            ),
            (ScratchBundle, []),
        ],
        ids=lambda v: v.__name__ if isinstance(v, type) else "",
    )
    def test_type_components(self, cls, extra):
        names = [c.listing_name for c in cls.COMPONENTS]
        assert names == ["data/", "metadata.json", "success", *extra]

    @pytest.mark.parametrize("cls", _TYPES, ids=lambda c: c.__name__)
    def test_every_directory_has_accessors(self, cls):
        b = _instance(cls)
        for component in cls.COMPONENTS:
            uri = b.component_uri(component.name)
            prefix = b.component_prefix(component.name)
            assert uri == f"{b.base_uri}/{component.name}"
            assert prefix == f"{b.base_prefix}/{component.name}"
            assert prefix.startswith("ns/")
            if component.is_directory:
                assert getattr(b, f"{component.name}_uri") == uri
                assert getattr(b, f"{component.name}_prefix") == prefix

    def test_file_components_match_the_marker_accessors(self):
        b = _instance(ReleaseCandidateBundle)
        assert b.component_uri("metadata.json") == b.metadata_uri
        assert b.component_uri("success") == b.success_uri
        assert b.component_uri("released") == b.released_uri
        assert not hasattr(b, "released_prefix")
        assert not hasattr(ThemeAssembleBundle, "summary_uri")

    def test_accessors_belong_to_their_type(self):
        assert not hasattr(SourceRawBundle, "changelog_uri")
        assert not hasattr(SourceIngestBundle, "bridgefiles_uri")
        assert not hasattr(ThemePromoteBundle, "registry_uri")
        assert not hasattr(ReleaseCandidateBundle, "validation_uri")
        assert not hasattr(Bundle, "changelog_prefix")

    def test_generated_accessors_are_documented_properties(self):
        accessor = ThemePromoteBundle.__dict__["validation_uri"]
        assert isinstance(accessor, property)
        assert accessor.fget.__name__ == "validation_uri"
        assert "validation/" in accessor.__doc__
        assert "validation/" in ThemePromoteBundle.validation_prefix.__doc__

    def test_subclasses_inherit_and_override_accessors(self):
        class Custom(ThemePromoteBundle):
            COMPONENTS = (
                *ThemePromoteBundle.COMPONENTS,
                BundleComponent.directory("logs"),
                BundleComponent.directory("not-an-identifier"),
            )

            @property
            def logs_uri(self) -> str:
                return "custom"

        b = Custom(theme="t", bucket="bkt", schema="s", run="r")
        assert b.validation_uri == f"{b.base_uri}/validation"
        assert "validation_uri" not in Custom.__dict__
        assert b.logs_uri == "custom"
        assert b.logs_prefix == f"{b.base_prefix}/logs"
        assert b.data_uri == f"{b.base_uri}/data"
        assert b.component_uri("not-an-identifier").endswith("/not-an-identifier")

    @pytest.mark.parametrize("name", ["data", "data/", "metadata.json"])
    def test_get_component(self, name):
        component = ThemePromoteBundle.get_component(name)
        assert component.name == name.rstrip("/")
        assert _instance(ThemePromoteBundle).get_component(name) is component

    @pytest.mark.parametrize("name", ["registry", "nope", ""])
    def test_get_unknown_component_raises(self, name):
        with pytest.raises(KeyError, match="ThemePromoteBundle has no component"):
            ThemePromoteBundle.get_component(name)
        with pytest.raises(KeyError):
            _instance(ThemePromoteBundle).component_uri(name)

    def test_component_names_are_unique(self):
        for cls in _TYPES:
            names = [c.name for c in cls.COMPONENTS]
            assert len(names) == len(set(names)), cls.__name__


# ── Partition resolution edge cases ──────────────────────────────────────────


class TestResolvePartitionsEdgeCases:
    def test_backtracks_to_older_candidate(self, store):
        store.write("bkt", "root/version=1/run=a/success")
        store.write("bkt", "root/version=2/run=a/data/x")
        assert _resolve_partitions("bkt", "root", ["version", "run"], store=store) == {
            "version": "1",
            "run": "a",
        }

    def test_no_required_markers_accepts_any_content(self, store):
        store.write("bkt", "root/version=1/data/x")
        assert _resolve_partitions(
            "bkt", "root", ["version"], required_markers=(), store=store
        ) == {"version": "1"}

    def test_override_on_non_terminal_partition(self, store):
        store.write("bkt", "root/version=1/run=a/success")
        store.write("bkt", "root/version=2/run=b/success")
        result = _resolve_partitions(
            "bkt", "root", ["version", "run"], overrides={"version": "1"}, store=store
        )
        assert result == {"version": "1", "run": "a"}

    def test_override_with_no_viable_deeper_path_raises(self, store):
        store.write("bkt", "root/version=1/run=a/data/x")
        with pytest.raises(
            PartitionResolutionError,
            match=r"No run= directory .* under s3://bkt/root/version=1/ "
            r"\(1 candidate\(s\) checked\)",
        ):
            _resolve_partitions(
                "bkt",
                "root",
                ["version", "run"],
                overrides={"version": "1"},
                store=store,
            )

    def test_fully_set_bundle_without_marker_names_the_leaf(self, store):
        store.write("bkt", "feeds/provider=p/resource=r/version=1/run=r1/data/x")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="r1", store=store
        )
        with pytest.raises(
            PartitionResolutionError,
            match=r"Override run='r1' missing required markers \['success'\] at "
            r"s3://bkt/feeds/provider=p/resource=r/version=1/run=r1",
        ):
            b.resolve_partitions()

    def test_resolve_with_no_partition_keys_raises(self, store):
        with pytest.raises(PartitionResolutionError, match="No bundle with required"):
            _resolve_partitions("bkt", "root", [], store=store)

    def test_latest_with_nothing_found_raises(self, store):
        with pytest.raises(PartitionResolutionError, match="No version= directory"):
            _resolve_partitions("bkt", "root", ["version"], store=store)

    def test_resolve_partitions_keeps_set_values_and_applies_overrides(self, store):
        store.write("bkt", "root/version=1/run=a/success")
        store.write("bkt", "root/version=1/run=b/success")
        b = Bundle(
            bucket="bkt",
            root_path="root",
            partitions={"version": "1", "run": ""},
            store=store,
        )
        b.resolve_partitions()
        assert b.run == "b"
        b.resolve_partitions(overrides={"run": "a"})
        assert b.run == "a"

    def test_resolve_partitions_noop_without_partitions(self, store):
        b = Bundle(bucket="bkt", root_path="root", store=store)
        b.resolve_partitions()
        assert b.base_uri == "s3://bkt/root"


class TestFindPreviousPartition:
    """Layout: ``feeds/provider=esri/resource=buildings/version=<v>/run=<r>``."""

    ROOT = "feeds/provider=esri/resource=buildings"
    CURRENT_VERSION = "2026-02-01"
    CURRENT_RUN = "20260201T000000"

    def _bundle(self, *, store=None):
        return SourceIngestBundle(
            bucket="bkt",
            provider="esri",
            resource="buildings",
            version=self.CURRENT_VERSION,
            run=self.CURRENT_RUN,
            store=store,
        )

    def _run(self, store, version, run, success=True):
        prefix = f"{self.ROOT}/version={version}/run={run}"
        store.write("bkt", f"{prefix}/data/part-0.parquet")
        if success:
            store.write("bkt", f"{prefix}/success")

    def test_previous_version_preferred_over_same_version_run(self, store):
        self._run(store, "2026-01-01", "20260101T000000")
        self._run(store, self.CURRENT_VERSION, "20260115T000000")
        self._run(store, self.CURRENT_VERSION, self.CURRENT_RUN)
        found = self._bundle(store=store).find_previous_partition(
            same_value_fallback=True
        )
        assert found["baseline_kind"] == "previous"
        assert found["data_uri"] == (
            f"s3://bkt/{self.ROOT}/version=2026-01-01/run=20260101T000000/data"
        )

    def test_skips_incomplete_previous_version(self, store):
        self._run(store, "2026-01-01", "20260101T000000")
        self._run(store, "2026-01-15", "20260115T000000", success=False)
        found = self._bundle(store=store).find_previous_partition()
        assert "version=2026-01-01" in found["data_uri"]
        assert found["prior_values"] == ["2026-01-15", "2026-01-01"]
        assert found["rejected"] == ["2026-01-15"]

    def test_falls_back_to_earlier_run_of_current_version(self, store):
        self._run(store, self.CURRENT_VERSION, "20260110T000000")
        self._run(store, self.CURRENT_VERSION, "20260120T000000", success=False)
        self._run(store, self.CURRENT_VERSION, self.CURRENT_RUN)
        found = self._bundle(store=store).find_previous_partition(
            same_value_fallback=True
        )
        assert found["baseline_kind"] == "same"
        assert found["data_uri"] == (
            f"s3://bkt/{self.ROOT}/version={self.CURRENT_VERSION}"
            "/run=20260110T000000/data"
        )
        assert found["prior_values"] == []

    def test_fallback_ignores_current_and_later_runs(self, store):
        self._run(store, self.CURRENT_VERSION, self.CURRENT_RUN)
        self._run(store, self.CURRENT_VERSION, "20260301T000000")
        found = self._bundle(store=store).find_previous_partition(
            same_value_fallback=True
        )
        assert found["data_uri"] is None
        assert found["baseline_kind"] is None

    def test_fallback_is_opt_in(self, store):
        self._run(store, self.CURRENT_VERSION, "20260115T000000")
        found = self._bundle(store=store).find_previous_partition()
        assert found["data_uri"] is None

    def test_unusable_previous_versions_still_reported(self, store):
        self._run(store, "2026-01-01", "20260101T000000", success=False)
        self._run(store, self.CURRENT_VERSION, "20260115T000000")
        found = self._bundle(store=store).find_previous_partition(
            same_value_fallback=True
        )
        assert found["baseline_kind"] == "same"
        assert found["prior_values"] == ["2026-01-01"]
        assert found["rejected"] == ["2026-01-01"]

    def test_fallback_without_sub_partitions_returns_none(self, store):
        store.write("bkt", "root/version=0/data/x")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
        found = b.find_previous_partition(same_value_fallback=True)
        assert found["data_uri"] is None
        assert found["rejected"] == ["0"]

    def test_last_partition_checks_markers_directly(self, store):
        store.write("bkt", "root/version=0/success")
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"version": "1"}, store=store
        )
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
    def _bundle(self, version, run="r1", *, store=None):
        return Bundle(
            bucket="bkt",
            root_path="root",
            partitions={"version": version, "run": run},
            store=store,
        )

    def test_no_successful_bundle_in_group(self, store):
        b = self._bundle("1", store=store)
        assert b.is_deletable(0, [b]) == (
            False,
            "no successful bundle exists in partition group",
        )

    def test_newer_unsuccessful_bundle_is_kept(self, store):
        store.write("bkt", "root/version=1/run=r1/success")
        old, new = self._bundle("1", store=store), self._bundle("2", store=store)
        deletable, reason = new.is_deletable(0, [old, new])
        assert deletable is False
        assert "is not older than" in reason

    def test_young_successful_bundle_is_kept(self, store):
        now = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
        store.write("bkt", "root/version=1/run=r1/success")
        store.write("bkt", "root/version=2/run=r1/success")
        old, new = self._bundle("1", store=store), self._bundle("2", store=store)
        deletable, reason = old.is_deletable(30, [old, new], now=now)
        assert deletable is False
        assert reason.startswith("age 1.0d < min_age_days")

    def test_missing_last_modified_keeps_bundle(self, store, monkeypatch):
        store.write("bkt", "root/version=1/run=r1/success")
        store.write("bkt", "root/version=2/run=r1/success")
        old, new = self._bundle("1", store=store), self._bundle("2", store=store)
        monkeypatch.setattr(Bundle, "age_days", lambda self, now=None: None)
        assert old.is_deletable(30, [old, new]) == (
            False,
            "cannot determine bundle age",
        )

    def test_age_falls_back_to_metadata(self, store):
        now = dt.datetime(2026, 1, 3, tzinfo=dt.timezone.utc)
        store.write("bkt", "root/version=1/run=r1/metadata.json", b"{}")
        assert self._bundle("1", store=store).age_days(now=now) == 2.0

    def test_recent_incomplete_older_bundle_is_kept(self, store):
        # An in-flight rerun: initialize removed success and rewrote metadata.
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        store.write("bkt", "root/version=2/run=r1/success")
        store.write(
            "bkt",
            "root/version=1/run=r1/metadata.json",
            b"{}",
            last_modified=now - dt.timedelta(days=1),
        )
        old, new = self._bundle("1", store=store), self._bundle("2", store=store)
        assert old.is_deletable(30, [old, new], now=now) == (
            False,
            "age 1.0d < min_age_days 30d",
        )
        assert old.is_deletable(0, [old, new], now=now) == (
            True,
            "unsuccessful; superseded by ('2', 'r1'); age 1.0d >= 0d",
        )

    def test_incomplete_bundle_without_metadata_is_kept(self, store):
        store.write("bkt", "root/version=2/run=r1/success")
        store.write("bkt", "root/version=1/run=r1/data/part-0")
        old, new = self._bundle("1", store=store), self._bundle("2", store=store)
        assert old.is_deletable(0, [old, new]) == (
            False,
            "cannot determine bundle age",
        )

    def test_cleanup_all_keeps_in_flight_rerun(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)
        rerun = "ns/feeds/provider=p/resource=r/version=0/run=r1/metadata.json"
        store.write("bkt", rerun, b"{}", last_modified=now)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=False,
            namespace="ns",
            now=now,
            store=store,
        )
        deleted = [r.bundle_uri.split("version=")[1][0] for r in report.deleted]
        assert deleted == ["1"]
        assert rerun in keys(store)

    def test_retention_order_key_falls_back_to_sorted_partitions(self):
        b = Bundle(
            bucket="bkt", root_path="root", partitions={"purpose": "p", "date": "d"}
        )
        assert b.retention_order_key() == ("d", "p")

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

    def test_cleanup_all_records_delete_failures(self, store, monkeypatch):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        _seed_versions(store, now)

        def boom(self, dry_run=False, *, table_dropper=None):
            raise RuntimeError("store down")

        monkeypatch.setattr(Bundle, "delete", boom)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt",
            min_age_days=30,
            dry_run=False,
            namespace="ns",
            now=now,
            store=store,
        )
        assert report.deleted == []
        assert len(report.errors) == 2
        assert all("delete failed: store down" in e.reason for e in report.errors)

    def test_discover_all_skips_foreign_and_empty_segments(self, store):
        store.write("bkt", "theme_stage/theme=places/run=r1/success")
        store.write("bkt", "theme_stage/theme=/run=r1/success")
        store.write("bkt", "theme_stage/_tmp/run=r1/success")
        found = ThemeAssembleBundle.discover_all(bucket="bkt", store=store)
        assert [b.partitions for b in found] == [{"theme": "places", "run": "r1"}]


# ── Enrichment and scan edge cases ───────────────────────────────────────────


class TestScanEdgeCases:
    def test_enrichment_skips_entries_with_provenance_or_without_ids(
        self, store, tmp_path
    ):
        meta = RecordingMetadata()
        meta.section["inputs"] = [
            {"path": "s3://src/a", "provider": {}, "resource": {}},
            {"path": "s3://src/b"},
        ]
        meta.init = lambda input_paths=None, capture_variables=True: None
        b = ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=store)
        b.initialize(
            input_bundles=[Bundle(bucket="src", root_path="b", store=store)],
            metadata=meta,
            datasets_dir=tmp_path,
        )
        assert not any(c[0] == "update_inputs" for c in meta.calls)

    def test_needs_processing_ignores_non_run_directories(self, store):
        store.write("bkt", "feeds/provider=p/resource=r/version=1/_tmp/success")
        b = SourceIngestBundle(
            provider="p", resource="r", bucket="bkt", version="1", run="r2", store=store
        )
        assert b.needs_processing() is True

    def test_batch_scans_ignore_non_success_and_partial_keys(self, store):
        store.write("bkt", "feeds/provider=p/resource=a/version=1/run=x/data/f")
        store.write("bkt", "feeds/provider=p/resource=a/success")
        assert (
            SourceIngestBundle.resolve_partitions_batch(
                provider="p", bucket="bkt", store=store
            )
            == {}
        )
        todo = SourceIngestBundle(
            provider="p", resource="a", bucket="bkt", version="1", run="x", store=store
        )
        assert Bundle.needs_processing_batch([todo], store=store) == [todo]


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

    def _promote(self, store, schema, lm=None):
        store.write(
            "bkt",
            f"theme_promote/theme=t/schema={schema}/run=r1/success",
            last_modified=lm,
        )

    def test_resolve_latest_schema(self, store):
        for schema in ("v1.9.0", "v1.16.0", "v1.10.0"):
            self._promote(store, schema)
        b = ThemePromoteBundle(theme="t", bucket="bkt", store=store)
        b.resolve_partitions()
        assert b.partitions == {"theme": "t", "schema": "v1.16.0", "run": "r1"}

    def test_cleanup_keeps_newest_schema(self, store):
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        old = now - dt.timedelta(days=90)
        for schema in ("v1.9.0", "v1.16.0"):
            self._promote(store, schema, lm=old)
        report = ThemePromoteBundle.cleanup_all(
            bucket="bkt", min_age_days=30, dry_run=True, now=now, store=store
        )
        assert [d.bundle_uri for d in report.would_delete] == [
            "s3://bkt/theme_promote/theme=t/schema=v1.9.0/run=r1"
        ]
        assert [d.bundle_uri for d in report.kept] == [
            "s3://bkt/theme_promote/theme=t/schema=v1.16.0/run=r1"
        ]

    def test_find_previous_partition(self, store):
        for schema in ("v1.9.0", "v1.10.0", "v1.20.0"):
            self._promote(store, schema)
        b = ThemePromoteBundle(
            theme="t", bucket="bkt", schema="v1.16.0", run="r1", store=store
        )
        found = b.find_previous_partition(partition_key="schema")
        assert found["prior_values"] == ["v1.10.0", "v1.9.0"]
        assert "schema=v1.10.0" in found["data_uri"]

    def test_same_value_fallback_orders_runs(self, store):
        store.write("bkt", "root/version=1/run=r9/success")
        store.write("bkt", "root/version=1/run=r11/success")
        b = Bundle(
            bucket="bkt",
            root_path="root",
            partitions={"version": "1", "run": "r10"},
            store=store,
        )
        found = b.find_previous_partition(same_value_fallback=True)
        assert found["data_uri"] == "s3://bkt/root/version=1/run=r9/data"

    def test_resolve_partitions_batch(self, store):
        for version in ("v1.9.0", "v1.16.0"):
            store.write(
                "bkt", f"feeds/provider=p/resource=a/version={version}/run=r1/success"
            )
        out = SourceIngestBundle.resolve_partitions_batch(
            provider="p", bucket="bkt", store=store
        )
        assert out["a"].partitions["version"] == "v1.16.0"


# ── S3 client reuse ──────────────────────────────────────────────────────────


class TestClientReuse:
    """A walk or check opens one S3 client, however many requests it makes."""

    _ROOT = "feeds/provider=p/resource=r"

    def _seed(self, fake) -> None:
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

    def test_cleanup_all(self, counted_s3):
        # One client for the walk, one shared by every check and delete.
        fake, calls = counted_s3
        self._seed(fake)
        report = SourceIngestBundle.cleanup_all(bucket="bkt", min_age_days=0)
        assert len(report.kept) + len(report.would_delete) == 7
        assert calls == [("s3",), ("s3",)]

    def test_lifecycle_and_age(self, counted_s3):
        fake, calls = counted_s3
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1")
        b.finalize(metadata=RecordingMetadata())
        b.initialize(metadata=RecordingMetadata())  # exists, then delete
        assert b.age_days() is None  # two lookups, both missing
        assert calls == [("s3",)] * 3

    def test_status(self, counted_s3):
        fake, calls = counted_s3
        b = ScratchBundle(purpose="p", bucket="bkt", run="r1")
        assert b.status().exists is False  # marker, metadata, then a listing
        assert calls == [("s3",)]

        fake.put("bkt", "scratch/purpose=p/run=r1/metadata.json", b'{"d": {}}')
        assert b.status().has_metadata is True
        assert calls == [("s3",)] * 2

    def test_status_subclass_extends_on_the_same_store(self, counted_s3):
        """A subclass extending `_status_report` adds requests, not clients."""
        fake, calls = counted_s3
        prefix = "release_candidate/release=R/run=r1"
        fake.put("bkt", f"{prefix}/success")
        fake.put("bkt", f"{prefix}/released")

        b = ReleaseCandidateBundle(bucket="bkt", release="R", run="r1")
        assert b.status().status is BundleStatus.RELEASED
        assert calls == [("s3",)]

    def test_explicit_s3_store_opens_no_client(self, counted_s3):
        fake, calls = counted_s3
        self._seed(fake)
        store = S3BundleStore(client=fake)
        report = SourceIngestBundle.cleanup_all(
            bucket="bkt", min_age_days=0, store=store
        )
        assert len(report.kept) + len(report.would_delete) == 7
        assert calls == []


# ── Store injection ──────────────────────────────────────────────────────────


class TestStoreInjection:
    @pytest.mark.parametrize(
        "make",
        [
            lambda s: Bundle(bucket="bkt", root_path="r", store=s),
            lambda s: SourceRawBundle(provider="p", resource="r", bucket="b", store=s),
            lambda s: SourceIngestBundle(
                provider="p", resource="r", bucket="b", store=s
            ),
            lambda s: ThemeAssembleBundle(theme="t", bucket="b", store=s),
            lambda s: ThemePromoteBundle(theme="t", bucket="b", store=s),
            lambda s: ReleaseCandidateBundle(bucket="b", store=s),
            lambda s: ScratchBundle(purpose="p", bucket="b", store=s),
        ],
    )
    def test_constructors_take_store(self, make, store):
        assert make(store).store is store
        assert make(None).store is None

    def test_from_path_and_from_base_uri(self, store):
        assert Bundle.from_path("s3://bkt/root").store is None
        assert Bundle.from_path("s3://bkt/root", store=store).store is store
        b = ThemeAssembleBundle.from_base_uri(
            "s3://bkt/theme_stage/theme=t/run=r", store=store
        )
        assert b.store is store
        b = SourceIngestBundle.from_path(
            "s3://bkt/feeds", provider="p", resource="r", store=store
        )
        assert b.store is store

    def test_subclass_init_without_store(self, store):
        """Classmethods attach the store, so a subclass ``__init__`` needn't take it."""

        class Legacy(ThemeAssembleBundle):
            def __init__(self, theme, bucket, run="", namespace="", root_path=None):
                super().__init__(theme, bucket, run, namespace, root_path)

        store.write("bkt", "theme_stage/theme=t/run=r1/success")
        b = Legacy.from_base_uri("s3://bkt/theme_stage/theme=t/run=r1", store=store)
        assert type(b) is Legacy
        assert b.is_successful() is True

    def test_discover_all_attaches_explicit_store(self, store):
        store.write("bkt", "theme_stage/theme=t/run=r1/success")
        (found,) = ThemeAssembleBundle.discover_all(bucket="bkt", store=store)
        assert found.store is store
        assert found.is_successful() is True

    def test_classmethod_results_carry_store(self, store):
        store.write("bkt", "feeds/provider=p/resource=a/version=1/run=r1/success")
        store.write("bkt", "release_candidate/release=R/run=r1/success")
        store.write("bkt", "release_candidate/release=R/run=r1/released")
        batch = SourceIngestBundle.resolve_partitions_batch(
            provider="p", bucket="bkt", store=store
        )
        assert batch["a"].store is store
        rc = ReleaseCandidateBundle.resolve_latest_released(
            release="R", bucket="bkt", store=store
        )
        assert rc.store is store

    def test_cleanup_all_with_discover_all_override(self, counted_s3):
        """Overrides with no ``store`` parameter, like tf-data-platform's adapter, work."""
        fake, calls = counted_s3
        now = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)
        for run in ("r1", "r2"):
            fake.put("bkt", f"theme_stage/theme=t/run={run}/success", lm=_DEFAULT_LM)

        class Adapter(ThemeAssembleBundle):
            @classmethod
            def discover_all(cls, bucket, namespace=""):
                return [
                    Bundle.from_path(b.root_uri, partitions=b.partitions)
                    for b in super().discover_all(bucket=bucket, namespace=namespace)
                ]

        report = Adapter.cleanup_all(bucket="bkt", min_age_days=30, now=now)
        assert [d.bundle_uri for d in report.would_delete] == [
            "s3://bkt/theme_stage/theme=t/run=r1"
        ]
        assert calls == [("s3",), ("s3",)]

    def test_needs_processing_batch_uses_each_bundles_store(self):
        done, empty = InMemoryBundleStore(), InMemoryBundleStore()
        done.write("bkt", "theme_stage/theme=t/run=r0/success")

        def make(s):
            return ThemeAssembleBundle(theme="t", bucket="bkt", run="r1", store=s)

        a, b = make(done), make(empty)
        assert Bundle.needs_processing_batch([a, b]) == [b]
        # An explicit store overrides each bundle's own.
        assert Bundle.needs_processing_batch([a, b], store=empty) == [a, b]
        assert Bundle.needs_processing_batch([a, b], store=done) == []
