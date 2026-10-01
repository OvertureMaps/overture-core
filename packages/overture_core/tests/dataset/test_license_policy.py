"""Tests for the per-theme license policy."""

import json

import pytest

from overture_core.dataset.attribution import entries_from_file
from overture_core.dataset.license_policy import LicensePolicy, load_policy
from overture_core.dataset.schema import validate_file


@pytest.fixture()
def all_entries(datasets_dir):
    return [
        e
        for name in ("acme.json", "globex.json", "nolic.json")
        for e in entries_from_file(validate_file(datasets_dir / name))
    ]


class TestLoadPolicy:
    def test_loads(self, policy_path):
        policy = load_policy(policy_path)
        assert policy.themes == ["base", "places"]
        assert policy.allowed("base") == {"ODbL-1.0", "CC-BY-4.0"}
        assert policy.all_licenses == {
            "ODbL-1.0",
            "CC-BY-4.0",
            "CDLA-Permissive-2.0",
            "CC0-1.0",
        }

    def test_missing_file(self, tmp_path):
        with pytest.raises(ValueError, match="could not read"):
            load_policy(tmp_path / "nope.json")

    def test_top_level_must_be_object(self, tmp_path):
        p = tmp_path / "p.json"
        p.write_text("[]", encoding="utf-8")
        with pytest.raises(ValueError, match="top level must be a JSON object"):
            load_policy(p)

    @pytest.mark.parametrize(
        "doc, match",
        [
            pytest.param({"Base": ["ODbL-1.0"]}, "Base", id="theme-not-snake-case"),
            pytest.param({"base": []}, "base", id="theme-empty"),
            pytest.param({"base": ["ODbL 1.0"]}, "ODbL 1.0", id="license-has-space"),
            pytest.param(
                {"base": ["ODbL-1.0", "ODbL-1.0"]}, "duplicate", id="duplicate"
            ),
            pytest.param({"base": "ODbL-1.0"}, "base", id="value-not-list"),
        ],
    )
    def test_rejects_invalid(self, tmp_path, doc, match):
        p = tmp_path / "p.json"
        p.write_text(json.dumps(doc), encoding="utf-8")
        with pytest.raises(ValueError, match=match):
            load_policy(p)


class TestChecks:
    def test_unknown_theme(self, policy_path):
        with pytest.raises(KeyError, match="unknown theme 'nope'"):
            load_policy(policy_path).allowed("nope")

    def test_is_allowed(self, policy_path):
        policy = load_policy(policy_path)
        assert policy.is_allowed("base", "ODbL-1.0")
        assert not policy.is_allowed("places", "ODbL-1.0")
        assert not policy.is_allowed("base", None)
        assert not policy.is_allowed("base", "")

    def test_violations(self, policy_path, all_entries):
        policy = load_policy(policy_path)
        assert policy.violations("base", all_entries) == [
            ("globex_places", "CDLA-Permissive-2.0"),
            ("nolic_stuff", ""),
        ]
        assert policy.violations("places", all_entries) == [
            ("acme_planet", "ODbL-1.0"),
            ("acme_coastlines", "ODbL-1.0"),
            ("nolic_stuff", ""),
        ]

    def test_unlisted_licenses(self, all_entries):
        policy = LicensePolicy({"base": ["ODbL-1.0"]})
        # Unlicensed resources are not "unlisted": there's nothing to list.
        assert policy.unlisted_licenses(all_entries) == {
            "CDLA-Permissive-2.0": ["globex_places"]
        }
