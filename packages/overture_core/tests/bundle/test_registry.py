"""Tests for the bundle type registry."""

import doctest

import pytest

from overture_core.bundle import bundle as bundle_module
from overture_core.bundle import registry as registry_module
from overture_core.bundle.bundle import (
    Bundle,
    BundleComponent,
    BundleStatus,
    ComponentKind,
    ReleaseCandidateBundle,
    ScratchBundle,
    SourceIngestBundle,
    SourceRawBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
)
from overture_core.bundle.registry import (
    DEFAULT_REGISTRY,
    BundleLocation,
    BundleRegistry,
)

_TA = "theme_stage/theme=places/run=r1"


class TestDefaultRegistry:
    def test_types(self):
        assert list(DEFAULT_REGISTRY) == [
            SourceRawBundle,
            SourceIngestBundle,
            ThemeAssembleBundle,
            ThemePromoteBundle,
            ReleaseCandidateBundle,
            ScratchBundle,
        ]
        assert len(DEFAULT_REGISTRY) == 6

    def test_registers_every_bundle_type(self):
        types = {
            t
            for t in vars(bundle_module).values()
            if isinstance(t, type) and issubclass(t, Bundle) and t is not Bundle
        }
        assert types == set(DEFAULT_REGISTRY)

    def test_layout_of_each_type(self):
        layout = {
            t.BUNDLE_TYPE: (t.ROOT_PATH, t.PARTITION_KEYS) for t in DEFAULT_REGISTRY
        }
        assert layout == {
            "source_raw": ("datasets", ("provider", "resource", "version", "run")),
            "source_ingest": ("feeds", ("provider", "resource", "version", "run")),
            "theme_assemble": ("theme_stage", ("theme", "run")),
            "theme_promote": ("theme_promote", ("theme", "schema", "run")),
            "release_candidate": ("release_candidate", ("release", "run")),
            "scratch": ("scratch", ("purpose", "run")),
        }

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("theme_assemble", ThemeAssembleBundle),
            ("theme_stage", ThemeAssembleBundle),
            ("theme_stage/", ThemeAssembleBundle),
            ("feeds", SourceIngestBundle),
            ("source_ingest", SourceIngestBundle),
            ("release_candidate", ReleaseCandidateBundle),
            ("scratch", ScratchBundle),
        ],
    )
    def test_get_by_type_or_root(self, name, expected):
        assert DEFAULT_REGISTRY.get(name) is expected
        assert name in DEFAULT_REGISTRY

    def test_get_unknown_raises(self):
        with pytest.raises(KeyError, match="No registered bundle type 'release'"):
            DEFAULT_REGISTRY.get("release")

    def test_contains(self):
        assert ThemePromoteBundle in DEFAULT_REGISTRY
        assert Bundle not in DEFAULT_REGISTRY
        assert "release" not in DEFAULT_REGISTRY
        assert 3 not in DEFAULT_REGISTRY

    def test_repr(self):
        assert repr(BundleRegistry([SourceRawBundle, ThemeAssembleBundle])) == (
            "BundleRegistry([SourceRawBundle, ThemeAssembleBundle])"
        )

    def test_module_example(self):
        result = doctest.testmod(registry_module)
        assert result.attempted > 0
        assert result.failed == 0


class TestValidation:
    def test_rejects_non_bundle_types(self):
        with pytest.raises(TypeError, match="Expected a Bundle subclass"):
            BundleRegistry([SourceRawBundle, dict])  # type: ignore[list-item]
        with pytest.raises(TypeError, match="Expected a Bundle subclass"):
            BundleRegistry(
                [SourceRawBundle(provider="p", resource="r", bucket="bkt")]  # type: ignore[list-item]
            )

    def test_rejects_types_without_a_layout(self):
        with pytest.raises(ValueError, match="needs a ROOT_PATH and PARTITION_KEYS"):
            BundleRegistry([Bundle])

    def test_rejects_duplicates(self):
        with pytest.raises(ValueError, match="registered twice"):
            BundleRegistry([SourceRawBundle, SourceRawBundle])

    def test_rejects_shared_root(self):
        class Other(Bundle):
            BUNDLE_TYPE = "other"
            ROOT_PATH = "theme_stage"
            PARTITION_KEYS = ("run",)

        with pytest.raises(
            ValueError, match="Other and ThemeAssembleBundle both use the name"
        ):
            BundleRegistry([ThemeAssembleBundle, Other])

    def test_rejects_root_matching_another_type_name(self):
        class Other(Bundle):
            BUNDLE_TYPE = "other"
            ROOT_PATH = "theme_assemble"
            PARTITION_KEYS = ("run",)

        with pytest.raises(ValueError, match="both use the name 'theme_assemble'"):
            BundleRegistry([ThemeAssembleBundle, Other])

    def test_empty_registry(self):
        empty = BundleRegistry([])
        assert len(empty) == 0
        assert empty.locate(f"s3://bkt/{_TA}") is None


class TestLocate:
    @pytest.mark.parametrize(
        ("path", "bucket", "namespace"),
        [
            (f"s3://bkt/{_TA}", "bkt", ""),
            (f"s3://bkt/{_TA}/", "bkt", ""),
            (f"s3://bkt/ns/{_TA}", "bkt", "ns"),
            (f"s3://bkt/a/b/{_TA}", "bkt", "a/b"),
            (_TA, "", ""),
            (f"/ns/{_TA}/", "", "ns"),
        ],
    )
    def test_complete_bundle(self, path, bucket, namespace):
        loc = DEFAULT_REGISTRY.locate(path)
        assert loc == BundleLocation(
            bundle_type=ThemeAssembleBundle,
            bucket=bucket,
            namespace=namespace,
            partitions={"theme": "places", "run": "r1"},
        )
        assert loc.is_complete
        assert loc.next_partition_key is None
        assert loc.component is None

    @pytest.mark.parametrize(
        ("path", "partitions", "next_key"),
        [
            ("s3://bkt/ns/theme_promote", {}, "theme"),
            ("s3://bkt/ns/theme_promote/", {}, "theme"),
            ("s3://bkt/ns/theme_promote/theme=places", {"theme": "places"}, "schema"),
            (
                "s3://bkt/ns/theme_promote/theme=places/schema=1.0",
                {"theme": "places", "schema": "1.0"},
                "run",
            ),
        ],
    )
    def test_partial_position(self, path, partitions, next_key):
        loc = DEFAULT_REGISTRY.locate(path)
        assert loc.bundle_type is ThemePromoteBundle
        assert (loc.namespace, dict(loc.partitions)) == ("ns", partitions)
        assert loc.is_complete is False
        assert loc.next_partition_key == next_key
        assert loc.root_prefix == "ns/theme_promote"
        assert loc.root_uri == "s3://bkt/ns/theme_promote"
        assert loc.uri == path.rstrip("/")

    @pytest.mark.parametrize(
        ("relative_path", "component"),
        [
            ("data", BundleComponent("data")),
            ("data/part-0.parquet", BundleComponent("data")),
            ("metadata.json", BundleComponent("metadata.json", ComponentKind.FILE)),
            ("summary.md", BundleComponent("summary.md", ComponentKind.FILE)),
            ("changelog/type=x/f.parquet", BundleComponent("changelog")),
            ("success/x", None),
            ("unlisted/f", None),
            ("theme_stage/theme=a/run=b", None),
        ],
    )
    def test_path_inside_a_bundle(self, relative_path, component):
        loc = DEFAULT_REGISTRY.locate(f"s3://bkt/ns/{_TA}/{relative_path}")
        assert loc.bundle_type is ThemeAssembleBundle
        assert loc.namespace == "ns"
        assert loc.is_complete
        assert loc.relative_path == relative_path
        assert loc.component == component
        assert loc.uri == f"s3://bkt/ns/{_TA}/{relative_path}"

    @pytest.mark.parametrize(
        ("path", "expected", "namespace"),
        [
            # A partial root followed by other segments is a namespace.
            ("s3://bkt/feeds/theme_stage/theme=a", ThemeAssembleBundle, "feeds"),
            ("s3://bkt/theme_stage/feeds", SourceIngestBundle, "theme_stage"),
            ("s3://bkt/feeds", SourceIngestBundle, ""),
            # A bad partition segment ends the match.
            ("s3://bkt/feeds/x/release_candidate", ReleaseCandidateBundle, "feeds/x"),
        ],
    )
    def test_namespace_containing_a_root(self, path, expected, namespace):
        loc = DEFAULT_REGISTRY.locate(path)
        assert (loc.bundle_type, loc.namespace) == (expected, namespace)

    @pytest.mark.parametrize(
        "path",
        [
            "s3://bkt",
            "s3://bkt/",
            "s3://bkt/theme_stage/run=r1/theme=places",
            "s3://bkt/theme_stage/theme=/run=r1",
            "s3://bkt/theme_stage/theme/run=r1",
            "s3://bkt/theme_stage_old/theme=a/run=b",
            "",
        ],
    )
    def test_unknown_paths(self, path):
        assert DEFAULT_REGISTRY.locate(path) is None

    @pytest.mark.parametrize(
        "path",
        [
            "s3://bkt/release/2026-05-20.0/theme=places/type=place/part-0.parquet",
            "s3://bkt/ns/changelog/2026-05-20.0/theme=places/type=place/f.parquet",
            "s3://bkt/bridgefiles/2026-05-20.0/",
            "registry/2026-05-20.0/part-0.parquet",
        ],
    )
    def test_published_releases_are_not_bundles(self, path):
        assert DEFAULT_REGISTRY.locate(path) is None

    def test_keys_may_contain_hash_and_query_characters(self):
        loc = DEFAULT_REGISTRY.locate("s3://bkt/n#s/theme_stage/theme=a?b/run=c#d/e?f")
        assert (loc.bucket, loc.namespace) == ("bkt", "n#s")
        assert dict(loc.partitions) == {"theme": "a?b", "run": "c#d"}
        assert loc.relative_path == "e?f"

    @pytest.mark.parametrize("path", ["gs://bkt/theme_stage", "file:///tmp/x"])
    def test_rejects_other_schemes(self, path):
        with pytest.raises(ValueError, match="Expected an s3:// URI or a key"):
            DEFAULT_REGISTRY.locate(path)

    def test_longest_root_wins(self):
        class Short(Bundle):
            BUNDLE_TYPE = "short"
            ROOT_PATH = "a"
            PARTITION_KEYS = ("run",)

        class Long(Bundle):
            BUNDLE_TYPE = "long"
            ROOT_PATH = "a/b"
            PARTITION_KEYS = ("run",)

        registry = BundleRegistry([Short, Long])
        assert registry.locate("s3://bkt/a/b/run=1").bundle_type is Long
        assert registry.locate("s3://bkt/a/run=1").bundle_type is Short
        loc = registry.locate("s3://bkt/a/b")
        assert (loc.bundle_type, loc.root_prefix) == (Long, "a/b")

    def test_location_is_hashable(self):
        loc = DEFAULT_REGISTRY.locate(f"s3://bkt/{_TA}")
        assert hash(loc) == hash(DEFAULT_REGISTRY.locate(f"s3://bkt/{_TA}/"))


class TestBundleFromUri:
    @pytest.mark.parametrize(
        ("uri", "cls", "base_uri"),
        [
            (
                "s3://bkt/ns/datasets/provider=p/resource=r/version=1/run=a",
                SourceRawBundle,
                "s3://bkt/ns/datasets/provider=p/resource=r/version=1/run=a",
            ),
            (
                "s3://bkt/feeds/provider=p/resource=r/version=1/run=a/data/f.parquet",
                SourceIngestBundle,
                "s3://bkt/feeds/provider=p/resource=r/version=1/run=a",
            ),
            (f"s3://bkt/ns/{_TA}", ThemeAssembleBundle, f"s3://bkt/ns/{_TA}"),
            (
                "s3://bkt/theme_promote/theme=t/schema=1.0/run=a/success",
                ThemePromoteBundle,
                "s3://bkt/theme_promote/theme=t/schema=1.0/run=a",
            ),
            (
                "s3://bkt/x/release_candidate/release=R/run=a/",
                ReleaseCandidateBundle,
                "s3://bkt/x/release_candidate/release=R/run=a",
            ),
            (
                "s3://bkt/scratch/purpose=p/run=a/data",
                ScratchBundle,
                "s3://bkt/scratch/purpose=p/run=a",
            ),
        ],
    )
    def test_typed_bundle(self, uri, cls, base_uri):
        bundle = DEFAULT_REGISTRY.bundle_from_uri(uri)
        assert type(bundle) is cls
        assert bundle.base_uri == base_uri
        assert bundle.BUNDLE_TYPE == cls.BUNDLE_TYPE

    def test_matches_the_constructor(self):
        bundle = DEFAULT_REGISTRY.bundle_from_uri(
            "s3://bkt/ns/theme_promote/theme=t/schema=1.0/run=a"
        )
        expected = ThemePromoteBundle(
            theme="t", bucket="bkt", schema="1.0", run="a", namespace="ns"
        )
        assert bundle.base_uri == expected.base_uri
        assert bundle.partitions == expected.partitions
        assert bundle.validation_prefix == expected.validation_prefix

    def test_bare_key(self):
        bundle = DEFAULT_REGISTRY.bundle_from_uri(_TA)
        assert (bundle.bucket, bundle.base_uri) == ("", _TA)

    def test_store_is_attached(self, store):
        store.write("bkt", "ns/release_candidate/release=R/run=a/success")
        store.write("bkt", "ns/release_candidate/release=R/run=a/released")
        bundle = DEFAULT_REGISTRY.bundle_from_uri(
            "s3://bkt/ns/release_candidate/release=R/run=a", store=store
        )
        assert bundle.store is store
        assert bundle.status().status is BundleStatus.RELEASED

    def test_location_bundle(self, store):
        loc = DEFAULT_REGISTRY.locate(f"s3://bkt/{_TA}/data/f")
        bundle = loc.bundle(store=store)
        assert bundle.store is store
        assert bundle.component_uri(loc.component.name) == f"s3://bkt/{_TA}/data"

    def test_partial_path_raises(self):
        with pytest.raises(ValueError, match="has no 'run' partition"):
            DEFAULT_REGISTRY.bundle_from_uri("s3://bkt/theme_stage/theme=places")

    def test_unknown_path_raises(self):
        with pytest.raises(ValueError, match="not under a registered bundle root"):
            DEFAULT_REGISTRY.bundle_from_uri("s3://bkt/release/2026-05-20.0/")
