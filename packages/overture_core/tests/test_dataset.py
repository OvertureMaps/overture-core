"""Tests for the Dataset class."""

import json

import pytest

from overture_core.dataset import Dataset

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture()
def single_resource_file(tmp_path):
    """Write a minimal single-resource dataset JSON file and return its path."""
    data = {
        "provider": {
            "label": "acme",
            "name": "Acme Corp",
            "url": {"primary": "https://acme.example.com"},
        },
        "resources": [
            {
                "label": "widgets",
                "name": "Acme Widgets",
                "collection": {
                    "license": {"type": "MIT"},
                    "coverage": {"global": True},
                    "data_location": {"primary": "s3://acme/widgets"},
                },
                "ingestion": {"format": "parquet"},
                "matching": {"strategy": "exact"},
            }
        ],
    }
    p = tmp_path / "acme.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


@pytest.fixture()
def multi_resource_file(tmp_path):
    """Write a two-resource dataset JSON file and return its path."""
    data = {
        "provider": {
            "label": "globex",
            "name": "Globex Inc",
            "url": {"primary": "https://globex.example.com"},
        },
        "resources": [
            {
                "label": "buildings",
                "name": "Globex Buildings",
                "collection": {},
                "ingestion": {},
                "matching": {},
            },
            {
                "label": "roads",
                "name": "Globex Roads",
                "collection": {"license": {"type": "ODbL"}},
                "ingestion": {"format": "geojson"},
                "matching": {},
            },
        ],
    }
    p = tmp_path / "globex.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


@pytest.fixture()
def empty_resources_file(tmp_path):
    """Write a dataset JSON file with no resources."""
    data = {
        "provider": {"label": "empty", "name": "Empty", "url": {}},
        "resources": [],
    }
    p = tmp_path / "empty.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


# ── from_file: single resource ──────────────────────────────────────────────


class TestFromFileSingleResource:
    def test_loads_by_name(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.dataset_id == "acme_widgets"
        assert ds.provider_label == "acme"
        assert ds.provider_name == "Acme Corp"
        assert ds.resource_label == "widgets"
        assert ds.resource_name == "Acme Widgets"

    def test_auto_selects_when_only_one(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file)
        assert ds.resource_label == "widgets"

    def test_pipeline_sections(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.collection["license"]["type"] == "MIT"
        assert ds.ingestion["format"] == "parquet"
        assert ds.matching["strategy"] == "exact"

    def test_convenience_properties(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.license_type == "MIT"
        assert ds.coverage == {"global": True}
        assert ds.data_location == {"primary": "s3://acme/widgets"}

    def test_repr_and_str(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert "acme_widgets" in repr(ds)
        assert str(ds) == "acme_widgets"

    def test_provider_url(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.provider_url == {"primary": "https://acme.example.com"}

    def test_provider_data_and_resource_data(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.provider_data["label"] == "acme"
        assert ds.resource_data["label"] == "widgets"

    def test_refresh_schedule_defaults_to_empty_dict(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file, resource="widgets")
        assert ds.refresh_schedule == {}


# ── from_file: multi resource ───────────────────────────────────────────────


class TestFromFileMultiResource:
    def test_loads_specific_resource(self, multi_resource_file):
        ds = Dataset.from_file(multi_resource_file, resource="roads")
        assert ds.dataset_id == "globex_roads"
        assert ds.resource_label == "roads"

    def test_requires_resource_name(self, multi_resource_file):
        with pytest.raises(ValueError, match="Multiple resources"):
            Dataset.from_file(multi_resource_file)

    def test_resource_not_found_raises(self, multi_resource_file):
        with pytest.raises(KeyError, match="nosuch"):
            Dataset.from_file(multi_resource_file, resource="nosuch")


# ── Error cases ──────────────────────────────────────────────────────────────


class TestErrors:
    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Dataset.from_file(tmp_path / "nonexistent.json")

    def test_empty_resources_no_name(self, empty_resources_file):
        with pytest.raises(ValueError, match="No resources"):
            Dataset.from_file(empty_resources_file)

    def test_empty_resources_with_name(self, empty_resources_file):
        with pytest.raises(KeyError, match="foo"):
            Dataset.from_file(empty_resources_file, resource="foo")

    def test_missing_pipeline_sections_return_empty(self, tmp_path):
        data = {
            "provider": {"label": "bare", "name": "Bare", "url": {}},
            "resources": [{"label": "r", "name": "R"}],
        }
        p = tmp_path / "bare.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        ds = Dataset.from_file(p)
        assert ds.collection == {}
        assert ds.ingestion == {}
        assert ds.matching == {}
        assert ds.license_type == ""


# ── download_url ─────────────────────────────────────────────────────────────


class TestDownloadUrl:
    def test_concatenates_url_and_endpoint(self, tmp_path):
        data = {
            "provider": {"label": "acme", "name": "Acme", "url": {}},
            "resources": [
                {
                    "label": "widgets",
                    "name": "Widgets",
                    "collection": {
                        "data_download": {
                            "type": "http",
                            "url": "https://example.com/",
                            "endpoint": "a/b/",
                        }
                    },
                }
            ],
        }
        p = tmp_path / "acme.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        ds = Dataset.from_file(p)
        assert ds.download_url == "https://example.com/a/b/"

    def test_url_without_endpoint(self, tmp_path):
        data = {
            "provider": {"label": "acme", "name": "Acme", "url": {}},
            "resources": [
                {
                    "label": "widgets",
                    "name": "Widgets",
                    "collection": {
                        "data_download": {
                            "type": "http",
                            "url": "https://example.com/a",
                        }
                    },
                }
            ],
        }
        p = tmp_path / "acme.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        ds = Dataset.from_file(p)
        assert ds.download_url == "https://example.com/a"

    def test_raises_when_missing(self, single_resource_file):
        ds = Dataset.from_file(single_resource_file)
        with pytest.raises(ValueError, match="No data_download"):
            _ = ds.download_url


# ── from_name / path_for ─────────────────────────────────────────────────────


class TestFromName:
    def test_delegates_to_from_file(self, single_resource_file):
        """from_name builds a path under the given datasets_dir and calls from_file."""
        ds = Dataset.from_name(
            "acme", "widgets", datasets_dir=single_resource_file.parent
        )
        assert ds.dataset_id == "acme_widgets"

    def test_missing_provider_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Dataset.from_name("nonexistent", "x", datasets_dir=tmp_path)

    def test_path_for_joins_datasets_dir_and_label(self, tmp_path):
        assert Dataset.path_for("acme", tmp_path) == tmp_path / "acme.json"

    def test_path_for_accepts_str_datasets_dir(self, tmp_path):
        assert Dataset.path_for("acme", str(tmp_path)) == tmp_path / "acme.json"


# ── all_from_file ────────────────────────────────────────────────────────────


class TestAllFromFile:
    def test_returns_all_resources(self, multi_resource_file):
        datasets = Dataset.all_from_file(multi_resource_file)
        assert len(datasets) == 2
        labels = {ds.resource_label for ds in datasets}
        assert labels == {"buildings", "roads"}

    def test_empty_resources(self, empty_resources_file):
        datasets = Dataset.all_from_file(empty_resources_file)
        assert datasets == []

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Dataset.all_from_file(tmp_path / "missing.json")
