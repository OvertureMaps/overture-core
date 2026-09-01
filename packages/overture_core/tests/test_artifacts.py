"""Tests for the artifacts module (tree traversal + LicenseArtifact)."""

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from overture_core.artifacts import (
    ARTIFACT_REGISTRY,
    AttributionArtifact,
    LicenseArtifact,
    MetadataArtifact,
    find_nodes,
    has_keys,
    match_node,
    render_attribution_mdx,
)


class TestMatchNode:
    def test_literal_match(self):
        assert match_node({"a": 1, "b": 2}, {"a": 1}) is True

    def test_literal_mismatch(self):
        assert match_node({"a": 1}, {"a": 2}) is False

    def test_callable_predicate(self):
        assert match_node({"a": [1, 2]}, {"a": lambda v: len(v) == 2}) is True

    def test_callable_predicate_fails(self):
        assert match_node({"a": [1]}, {"a": lambda v: len(v) == 2}) is False

    def test_missing_key(self):
        assert match_node({"a": 1}, {"b": 1}) is False

    def test_non_dict_returns_false(self):
        assert match_node("not a dict", {"a": 1}) is False
        assert match_node(42, {"a": 1}) is False
        assert match_node(None, {"a": 1}) is False

    def test_empty_pattern_matches_any_dict(self):
        assert match_node({"a": 1}, {}) is True

    def test_multiple_constraints(self):
        node = {"x": 10, "y": "hello", "z": [1]}
        assert match_node(node, {"x": 10, "y": "hello"}) is True
        assert match_node(node, {"x": 10, "y": "wrong"}) is False


class TestFindNodes:
    def test_flat_dict_match(self):
        tree = {"a": 1, "b": 2}
        results = list(find_nodes(tree, {"a": 1}))
        assert results == [{"a": 1, "b": 2}]

    def test_nested_dict(self):
        tree = {"outer": {"a": 1, "b": 2}}
        results = list(find_nodes(tree, {"a": 1}))
        assert results == [{"a": 1, "b": 2}]

    def test_list_of_dicts(self):
        tree = [{"a": 1}, {"a": 2}, {"a": 1, "extra": True}]
        results = list(find_nodes(tree, {"a": 1}))
        assert len(results) == 2
        assert results[0] == {"a": 1}
        assert results[1] == {"a": 1, "extra": True}

    def test_deeply_nested(self):
        tree = {"l1": {"l2": {"l3": {"target": True, "val": 42}}}}
        results = list(find_nodes(tree, {"target": True}))
        assert results == [{"target": True, "val": 42}]

    def test_no_matches(self):
        tree = {"a": 1, "nested": {"b": 2}}
        assert list(find_nodes(tree, {"c": 3})) == []

    def test_empty_tree(self):
        assert list(find_nodes({}, {"a": 1})) == []
        assert list(find_nodes([], {"a": 1})) == []

    def test_multiple_matches_at_different_depths(self):
        tree = {
            "x": 1,
            "child": {"x": 1, "deeper": {"x": 1}},
        }
        results = list(find_nodes(tree, {"x": 1}))
        assert len(results) == 3


class TestHasKeys:
    def test_all_present(self):
        pred = has_keys("a", "b")
        assert pred({"a": 1, "b": 2, "c": 3}) is True

    def test_missing_key(self):
        pred = has_keys("a", "b")
        assert pred({"a": 1}) is False

    def test_non_dict(self):
        pred = has_keys("a")
        assert pred("string") is False
        assert pred(42) is False
        assert pred(None) is False

    def test_empty_keys(self):
        pred = has_keys()
        assert pred({}) is True
        assert pred({"a": 1}) is True


# Realistic metadata tree shaped like what the pipeline produces.
SAMPLE_METADATA = {
    "theme_assemble_dag": {
        "status": "success",
        "inputs": [
            {
                "path": "s3://bucket/feeds/provider=esri/resource=buildings/version=2026-01-01/run=r1",
                "source_ingest_dag": {
                    "status": "success",
                    "inputs": [
                        {
                            "path": "s3://bucket/datasets/provider=esri/resource=buildings/version=2026-01-01/run=r1",
                            "provider": {
                                "label": "esri",
                                "name": "Esri",
                                "url": {"primary": "https://esri.com"},
                            },
                            "resource": {
                                "label": "community_maps_buildings",
                                "name": "Esri Community Maps",
                                "collection": {
                                    "license": {
                                        "type": "CC-BY-4.0",
                                        "url": {
                                            "primary": "https://creativecommons.org/licenses/by/4.0/"
                                        },
                                        "requires_attribution": True,
                                        "attribution": "Esri Community Maps contributors",
                                    },
                                    "coverage": {
                                        "description": "Global",
                                        "areas": [{"iso_3166_1": "GLOBAL"}],
                                    },
                                },
                            },
                        }
                    ],
                },
            },
            {
                "path": "s3://bucket/feeds/provider=osm/resource=buildings/version=2026-01-01/run=r1",
                "source_ingest_dag": {
                    "status": "success",
                    "inputs": [
                        {
                            "path": "s3://bucket/datasets/provider=osm/resource=buildings/version=2026-01-01/run=r1",
                            "provider": {
                                "label": "osm",
                                "name": "OpenStreetMap",
                                "url": {"primary": "https://openstreetmap.org"},
                            },
                            "resource": {
                                "label": "buildings",
                                "name": "OSM Buildings",
                                "collection": {
                                    "license": {
                                        "type": "ODbL-1.0",
                                        "url": {
                                            "primary": "https://opendatacommons.org/licenses/odbl/"
                                        },
                                        "requires_attribution": True,
                                        "attribution": "OpenStreetMap contributors",
                                    },
                                    "coverage": {
                                        "description": "Global",
                                        "areas": [{"iso_3166_1": "GLOBAL"}],
                                    },
                                },
                            },
                        }
                    ],
                },
            },
        ],
    }
}


class TestLicenseArtifact:
    @pytest.fixture()
    def s3_store(self, monkeypatch):
        """Replace boto3.client("s3") with an in-memory fake."""
        store: dict[tuple[str, str], bytes] = {}
        mock_client = MagicMock()

        def _put_object(Bucket, Key, Body, **_kwargs):
            store[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()

        mock_client.put_object = MagicMock(side_effect=_put_object)
        monkeypatch.setattr(
            "overture_core.cloud.aws.object.boto3",
            MagicMock(client=MagicMock(return_value=mock_client)),
        )
        return store

    def test_extracts_two_licenses(self, s3_store):
        artifact = LicenseArtifact()
        artifact.generate(SAMPLE_METADATA, "s3://bucket/output")

        written = s3_store[("bucket", "output/license.json")]
        entries = json.loads(written)
        assert len(entries) == 2

        names = {e["provider_name"] for e in entries}
        assert names == {"Esri", "OpenStreetMap"}

    def test_extracts_license_fields(self, s3_store):
        artifact = LicenseArtifact()
        artifact.generate(SAMPLE_METADATA, "s3://bucket/output")

        entries = json.loads(s3_store[("bucket", "output/license.json")])
        esri = next(e for e in entries if e["provider_name"] == "Esri")

        assert esri["provider_label"] == "esri"
        assert esri["provider_url"] == "https://esri.com"
        assert esri["resource_name"] == "Esri Community Maps"
        assert esri["resource_label"] == "community_maps_buildings"
        assert esri["license_type"] == "CC-BY-4.0"
        assert esri["requires_attribution"] is True
        assert esri["attribution"] == "Esri Community Maps contributors"
        assert esri["coverage_description"] == "Global"
        assert esri["coverage_areas"] == ["GLOBAL"]

    def test_no_matches_writes_nothing(self, s3_store):
        artifact = LicenseArtifact()
        artifact.generate({"empty_dag": {"status": "success"}}, "s3://bucket/output")

        assert len(s3_store) == 0

    def test_handles_missing_optional_fields(self, s3_store):
        """Provider/resource present but no collection/license/coverage."""
        metadata = {
            "dag": {
                "inputs": [
                    {
                        "provider": {"label": "test", "name": "Test"},
                        "resource": {"label": "res", "name": "Test Resource"},
                    }
                ]
            }
        }
        artifact = LicenseArtifact()
        artifact.generate(metadata, "s3://bucket/out")

        entries = json.loads(s3_store[("bucket", "out/license.json")])
        assert len(entries) == 1
        assert entries[0]["license_type"] == ""
        assert entries[0]["license_url"] == ""
        assert entries[0]["requires_attribution"] is False
        assert entries[0]["coverage_areas"] == []
        assert entries[0]["themes"] == []

    def test_extracts_themes_from_ancestor_path(self, s3_store):
        """A theme= segment in an ancestor path is attached to the entry."""
        metadata = {
            "release_candidate_dag": {
                "inputs": [
                    {
                        "path": "s3://bucket/theme_promote/theme=places/run=r1",
                        "theme_assemble_dag": {
                            "inputs": [
                                {
                                    "provider": {"label": "p", "name": "P"},
                                    "resource": {"label": "r", "name": "R"},
                                }
                            ]
                        },
                    }
                ]
            }
        }
        LicenseArtifact().generate(metadata, "s3://bucket/out")
        entries = json.loads(s3_store[("bucket", "out/license.json")])
        assert entries[0]["themes"] == ["places"]

    def test_merges_themes_for_shared_provider_resource(self, s3_store):
        """Same provider+resource used by two themes produces one merged entry."""
        provider = {"label": "p", "name": "P"}
        resource = {"label": "r", "name": "R"}
        metadata = {
            "release_candidate_dag": {
                "inputs": [
                    {
                        "path": "s3://b/theme_promote/theme=places/run=r1",
                        "inner": {
                            "inputs": [{"provider": provider, "resource": resource}]
                        },
                    },
                    {
                        "path": "s3://b/theme_promote/theme=transportation/run=r1",
                        "inner": {
                            "inputs": [{"provider": provider, "resource": resource}]
                        },
                    },
                ]
            }
        }
        LicenseArtifact().generate(metadata, "s3://bucket/out")
        entries = json.loads(s3_store[("bucket", "out/license.json")])
        assert len(entries) == 1
        assert entries[0]["themes"] == ["places", "transportation"]


class TestRenderAttributionMdx:
    def test_groups_entries_by_theme(self):
        entries = [
            {
                "provider_name": "Esri",
                "resource_name": "Esri Community Maps",
                "provider_url": "https://esri.com",
                "license_type": "CC-BY-4.0",
                "license_url": "https://creativecommons.org/licenses/by/4.0/",
                "requires_attribution": True,
                "attribution": "Esri contributors",
                "themes": ["buildings"],
            },
            {
                "provider_name": "OSM",
                "resource_name": "OpenStreetMap",
                "provider_url": "https://openstreetmap.org",
                "license_type": "ODbL",
                "license_url": "https://opendatacommons.org/licenses/odbl/",
                "requires_attribution": True,
                "attribution": "© OpenStreetMap contributors",
                "themes": ["buildings", "transportation"],
            },
        ]
        body = render_attribution_mdx(entries)

        assert "### Buildings" in body
        assert "### Transportation" in body
        # OSM appears in both sections
        assert body.count("OpenStreetMap") == 2
        # Esri only in buildings
        assert body.count("Esri contributors") == 1

    def test_section_structure(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "R",
                "provider_url": "https://p.example",
                "license_type": "MIT",
                "license_url": "https://mit.example",
                "requires_attribution": False,
                "attribution": "",
                "themes": ["places"],
            }
        ]
        body = render_attribution_mdx(entries)
        expected = (
            "<details>\n"
            "    <summary>\n"
            "### Places\n"
            "    </summary>\n"
            "- [R](https://p.example). Available under [MIT](https://mit.example).\n"
            "</details>\n"
        )
        assert body == expected

    def test_uses_attribution_text_when_required(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "Some Resource",
                "provider_url": "https://p.example",
                "license_type": "ODbL",
                "license_url": "https://odbl.example",
                "requires_attribution": True,
                "attribution": "© P contributors",
                "themes": ["base"],
            }
        ]
        body = render_attribution_mdx(entries)
        assert "[© P contributors](https://p.example)" in body
        assert "Some Resource" not in body

    def test_falls_back_to_resource_name(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "Plain Resource",
                "provider_url": "https://p.example",
                "license_type": "CC0",
                "license_url": "https://cc0.example",
                "requires_attribution": False,
                "attribution": "",
                "themes": ["places"],
            }
        ]
        body = render_attribution_mdx(entries)
        assert "[Plain Resource](https://p.example)" in body

    def test_skips_entries_without_themes(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "R",
                "provider_url": "",
                "license_type": "",
                "license_url": "",
                "requires_attribution": False,
                "attribution": "",
                "themes": [],
            }
        ]
        assert render_attribution_mdx(entries) == ""

    def test_handles_missing_url_or_license(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "R",
                "provider_url": "",
                "license_type": "",
                "license_url": "",
                "requires_attribution": False,
                "attribution": "",
                "themes": ["places"],
            }
        ]
        body = render_attribution_mdx(entries)
        assert "- R." in body

    def test_themes_with_underscores_are_titled(self):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "R",
                "provider_url": "https://p.example",
                "license_type": "MIT",
                "license_url": "https://mit.example",
                "requires_attribution": False,
                "attribution": "",
                "themes": ["release_candidate"],
            }
        ]
        assert "### Release Candidate" in render_attribution_mdx(entries)


class TestAttributionArtifact:
    @pytest.fixture()
    def s3_store(self, monkeypatch):
        store: dict[tuple[str, str], bytes] = {}
        mock_client = MagicMock()

        def _put_object(Bucket, Key, Body, **_kwargs):
            store[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()

        def _get_object(Bucket, Key):
            if (Bucket, Key) not in store:
                error_response = {
                    "Error": {"Code": "NoSuchKey", "Message": "Not found"}
                }
                raise ClientError(error_response, "GetObject")
            body = MagicMock()
            body.read.return_value = store[(Bucket, Key)]
            return {"Body": body}

        mock_client.put_object = MagicMock(side_effect=_put_object)
        mock_client.get_object = MagicMock(side_effect=_get_object)
        monkeypatch.setattr(
            "overture_core.cloud.aws.object.boto3",
            MagicMock(client=MagicMock(return_value=mock_client)),
        )
        return store

    def test_reads_license_json_and_writes_mdx(self, s3_store):
        entries = [
            {
                "provider_name": "P",
                "resource_name": "R",
                "provider_url": "https://p.example",
                "license_type": "MIT",
                "license_url": "https://mit.example",
                "requires_attribution": False,
                "attribution": "",
                "themes": ["places"],
            }
        ]
        s3_store[("bucket", "out/license.json")] = json.dumps(entries).encode()

        AttributionArtifact().generate({}, "s3://bucket/out")

        assert ("bucket", "out/attribution.mdx") in s3_store
        body = s3_store[("bucket", "out/attribution.mdx")].decode()
        assert "### Places" in body
        assert "[R](https://p.example)" in body

    def test_skips_when_license_json_missing(self, s3_store):
        AttributionArtifact().generate({}, "s3://bucket/out")
        assert ("bucket", "out/attribution.mdx") not in s3_store


class TestArtifactRegistry:
    def test_license_registered(self):
        assert "license" in ARTIFACT_REGISTRY
        assert isinstance(ARTIFACT_REGISTRY["license"], LicenseArtifact)

    def test_attribution_registered(self):
        assert "attribution" in ARTIFACT_REGISTRY
        assert isinstance(ARTIFACT_REGISTRY["attribution"], AttributionArtifact)

    def test_attribution_runs_after_license(self):
        # Registry insertion order matters: attribution reads license.json,
        # so license must run first.
        names = list(ARTIFACT_REGISTRY)
        assert names.index("license") < names.index("attribution")

    def test_all_entries_are_artifacts(self):
        for name, artifact in ARTIFACT_REGISTRY.items():
            assert isinstance(artifact, MetadataArtifact)
            assert artifact.name == name
