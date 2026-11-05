"""Tests for the bundle type registry."""

import pytest

from overture_core.bundle import bundle_types
from overture_core.bundle.bundle import (
    Bundle,
    ModelBundle,
    ReleaseCandidateBundle,
    ScratchBundle,
    SourceIngestBundle,
    SourceRawBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
)
from overture_core.bundle.components import (
    CHANGELOG,
    DATA,
    DIFF_REPORT,
    METADATA,
    SUCCESS,
    SUMMARY,
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

    def test_registers_every_rooted_bundle_type(self):
        types = {
            t
            for t in vars(bundle_types).values()
            if isinstance(t, type) and issubclass(t, Bundle) and t.ROOT_PATH
        }
        assert types == set(DEFAULT_REGISTRY)

    def test_model_bundle_is_not_registered(self):
        assert ModelBundle not in list(DEFAULT_REGISTRY)
        with pytest.raises(KeyError):
            DEFAULT_REGISTRY.get(ModelBundle.BUNDLE_TYPE)

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

    def test_get_unknown_raises(self):
        with pytest.raises(KeyError, match="No registered bundle type 'release'"):
            DEFAULT_REGISTRY.get("release")


class TestValidation:
    def test_rejects_types_without_a_layout(self):
        with pytest.raises(ValueError, match="needs a ROOT_PATH and PARTITION_KEYS"):
            BundleRegistry([Bundle])

    @pytest.mark.parametrize("root", ["theme_stage", "theme_assemble"])
    def test_rejects_name_collisions(self, root):
        class Other(Bundle):
            BUNDLE_TYPE = "other"
            ROOT_PATH = root
            PARTITION_KEYS = ("run",)

        with pytest.raises(
            ValueError,
            match=f"Other and ThemeAssembleBundle both use the name '{root}'",
        ):
            BundleRegistry([ThemeAssembleBundle, Other])


_TA_PARTITIONS = {"theme": "places", "run": "r1"}


class TestLocate:
    @pytest.mark.parametrize(
        ("path", "bucket", "namespace", "partitions"),
        [
            (f"s3://bkt/{_TA}", "bkt", "", _TA_PARTITIONS),
            (f"s3://bkt/{_TA}/", "bkt", "", _TA_PARTITIONS),
            (f"s3://bkt/ns/{_TA}", "bkt", "ns", _TA_PARTITIONS),
            (f"s3://bkt/a/b/{_TA}", "bkt", "a/b", _TA_PARTITIONS),
            (_TA, "", "", _TA_PARTITIONS),
            (f"/ns/{_TA}/", "", "ns", _TA_PARTITIONS),
            (f"S3://bkt/{_TA}", "bkt", "", _TA_PARTITIONS),
            (
                "s3://bkt//ns//theme_stage//theme=places//run=r1",
                "bkt",
                "ns",
                _TA_PARTITIONS,
            ),
            ("theme_stage/theme=a=b/run=c", "", "", {"theme": "a=b", "run": "c"}),
        ],
    )
    def test_complete_bundle(self, path, bucket, namespace, partitions):
        loc = DEFAULT_REGISTRY.locate(path)
        assert loc == BundleLocation(
            bundle_type=ThemeAssembleBundle,
            bucket=bucket,
            namespace=namespace,
            partitions=partitions,
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
            ("data", DATA),
            ("data/part-0.parquet", DATA),
            ("metadata.json", METADATA),
            ("success", SUCCESS),
            ("summary.md", SUMMARY),
            ("bundle_diff_report.md", DIFF_REPORT),
            ("changelog/type=x/f.parquet", CHANGELOG),
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

    def test_rejects_a_uri_without_a_bucket(self):
        with pytest.raises(ValueError, match="Expected a bucket"):
            DEFAULT_REGISTRY.locate(f"s3:///{_TA}")

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
        assert bundle.is_released()

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
