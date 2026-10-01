"""Tests for license entries and their txt/md rendering."""

import pytest

from overture_core.dataset.attribution import (
    entries_from_file,
    license_entry,
    render_attribution_bullet,
    render_markdown,
    render_text,
)
from overture_core.dataset.schema import validate_file


@pytest.fixture()
def acme_entries(datasets_dir):
    return entries_from_file(validate_file(datasets_dir / "acme.json"))


class TestLicenseEntry:
    def test_flattens_full_resource(self, datasets_dir):
        entry = entries_from_file(
            validate_file(datasets_dir / "acme.json"), {"planet"}
        )[0]
        assert entry == {
            "provider_name": "Acme Maps",
            "provider_label": "acme",
            "provider_url": "https://acme.example/",
            "resource_name": "Acme Maps",
            "resource_label": "planet",
            "license_type": "ODbL-1.0",
            "license_url": "https://acme.example/license",
            "requires_attribution": True,
            "attribution": "© Acme Maps contributors",
            "coverage_description": "Global",
            "coverage_areas": ["GLOBAL"],
        }

    def test_tolerates_missing_sections(self):
        entry = license_entry(
            {"label": "p", "name": "P"},
            {"label": "r", "name": "R"},
        )
        assert entry["license_type"] == ""
        assert entry["provider_url"] == ""
        assert entry["requires_attribution"] is False
        assert entry["coverage_areas"] == []

    def test_null_license_is_empty(self, datasets_dir):
        (entry,) = entries_from_file(validate_file(datasets_dir / "nolic.json"))
        assert entry["license_type"] == ""
        assert entry["license_url"] == ""


class TestEntriesFromFile:
    def test_all_resources_by_default(self, acme_entries):
        assert [e["resource_label"] for e in acme_entries] == ["planet", "coastlines"]

    def test_subset_sorted_by_label(self, datasets_dir):
        ds = validate_file(datasets_dir / "acme.json")
        labels = [
            e["resource_label"] for e in entries_from_file(ds, {"planet", "coastlines"})
        ]
        assert labels == ["coastlines", "planet"]

    def test_unknown_resource_raises(self, datasets_dir):
        ds = validate_file(datasets_dir / "acme.json")
        with pytest.raises(KeyError, match="nope"):
            entries_from_file(ds, {"nope"})


class TestRenderBullet:
    def test_attribution_with_urls(self, acme_entries):
        assert render_attribution_bullet(acme_entries[0]) == (
            "- [© Acme Maps contributors](https://acme.example/). "
            "Available under [ODbL-1.0](https://acme.example/license)."
        )

    def test_falls_back_to_resource_name_without_attribution(self, datasets_dir):
        (entry,) = entries_from_file(validate_file(datasets_dir / "globex.json"))
        assert render_attribution_bullet(entry) == (
            "- [Globex Places](https://globex.example/). Available under CDLA-Permissive-2.0."
        )

    def test_no_urls_no_license(self, datasets_dir):
        (entry,) = entries_from_file(validate_file(datasets_dir / "nolic.json"))
        assert render_attribution_bullet(entry) == "- Stuff."


class TestRenderMarkdown:
    def test_empty(self):
        assert render_markdown([]) == ""

    def test_groups_by_provider_and_dedupes_identical_bullets(self, datasets_dir):
        entries = []
        for name in ("acme.json", "globex.json"):
            entries += entries_from_file(validate_file(datasets_dir / name))
        out = render_markdown(entries, title="Attribution")
        assert out == (
            "# Attribution\n"
            "\n"
            "## Acme Maps\n"
            "\n"
            "- [© Acme Maps contributors](https://acme.example/). "
            "Available under [ODbL-1.0](https://acme.example/license).\n"
            "\n"
            "## Globex\n"
            "\n"
            "- [Globex Places](https://globex.example/). Available under CDLA-Permissive-2.0.\n"
        )


class TestRenderText:
    def test_empty(self):
        assert render_text([]) == ""

    def test_one_block_per_resource(self, acme_entries):
        out = render_text(acme_entries, title="T")
        assert out.startswith("T\n=\n")
        assert out.count("© Acme Maps contributors\n") == 2
        assert "  Resource: Acme Coastlines\n" in out
        assert "  License:  ODbL-1.0 <https://acme.example/license>\n" in out
        assert "  Coverage: Global\n" in out

    def test_omits_empty_fields(self, datasets_dir):
        out = render_text(entries_from_file(validate_file(datasets_dir / "nolic.json")))
        assert "License:" not in out
        assert "Coverage:" in out
        assert "  Provider: No License Co\n" in out
