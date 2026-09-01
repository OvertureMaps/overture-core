"""Validate the pydantic dataset config schema.

Unlike tf-data-platform's copy, this module has no baked-in default
directory to scan (see ``overture_core.dataset_schema``'s module docstring),
so there is no "validate every repository config file" test here -- that
belongs to whichever repository owns the actual `configs/datasets/*.json`
files and calls `validate_all()` with an explicit file list.
"""

import json

import pytest

from overture_core.dataset_schema import DataDownload, main, validate_all, validate_file


def _valid_doc() -> dict:
    """A minimal document exercising every optional section, kept valid."""
    return {
        "provider": {
            "label": "acme",
            "name": "Acme",
            "url": {"primary": "https://example.com/", "archive": ""},
        },
        "resources": [
            {
                "label": "widgets",
                "name": "Widgets",
                "collection": {
                    "data_location": {"primary": "", "archive": ""},
                    "data_download": {
                        "type": "http",
                        "url": "https://example.com/data.zip",
                        "endpoint": "",
                    },
                    "license": {
                        "url": {"primary": "", "archive": ""},
                        "type": "CC-BY-4.0",
                        "requires_attribution": True,
                        "text": "",
                        "attribution": "© Acme",
                    },
                    "coverage": {
                        "areas": [{"iso_3166_1": "US", "iso_3166_2": "US-NY"}],
                        "description": "",
                    },
                    "refresh_schedule": {
                        "frequency": "Monthly",
                        "source": "",
                        "type": "manual",
                        "month": [1, 6],
                    },
                    "known_issues": "",
                    "notes": "",
                    "api": {"url": "https://example.com/api"},
                    "extras": {"key": "value"},
                },
                "ingestion": {},
                "matching": {},
            }
        ],
    }


def _write(tmp_path, doc, name="acme.json"):
    path = tmp_path / name
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def _set(*path_and_value):
    """Mutator that sets doc[p0][p1]... = value."""
    *path, value = path_and_value

    def mutate(doc):
        target = doc
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value

    return mutate


def _del(*path):
    """Mutator that deletes doc[p0][p1]...[pN]."""

    def mutate(doc):
        target = doc
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]

    return mutate


def _col(*path_and_value):
    """Shortcut for _set inside resources[0].collection."""
    return _set("resources", 0, "collection", *path_and_value)


def _dup_resource(doc):
    doc["resources"].append(json.loads(json.dumps(doc["resources"][0])))


# ── negative fixtures ────────────────────────────────────────────────────────


def test_baseline_doc_is_valid(tmp_path):
    """Guards the fixture the invalid cases mutate from."""
    validate_file(_write(tmp_path, _valid_doc()))


INVALID_CASES = [
    # label / name rules
    pytest.param(
        _set("provider", "label", "Acme"),
        r"provider\.label",
        id="provider-label-uppercase",
    ),
    pytest.param(
        _set("provider", "label", "acme-corp"),
        r"provider\.label",
        id="provider-label-hyphen",
    ),
    pytest.param(
        _set("provider", "name", ""), r"provider\.name", id="provider-name-empty"
    ),
    pytest.param(
        _set("provider", "name", 123), r"provider\.name", id="provider-name-wrong-type"
    ),
    pytest.param(
        _set("provider", "url", "https://example.com/"),
        r"provider\.url",
        id="provider-url-wrong-type",
    ),
    pytest.param(
        _set("resources", 0, "label", "Widgets!"), "label", id="resource-label-pattern"
    ),
    pytest.param(_set("resources", 0, "name", ""), "name", id="resource-name-empty"),
    # top-level structure
    pytest.param(_set("resources", []), "resources", id="resources-empty"),
    pytest.param(_set("resources", {}), "resources", id="resources-wrong-type"),
    pytest.param(_set("surprise", 1), "surprise", id="unknown-key-top-level"),
    pytest.param(
        _set("resources", 0, "surprise", 1), "surprise", id="unknown-key-resource"
    ),
    pytest.param(_col("surprise", 1), "surprise", id="unknown-key-collection"),
    pytest.param(
        _dup_resource, "duplicate resource labels", id="duplicate-resource-labels"
    ),
    pytest.param(
        _set("resources", 0, "ingestion", ""), "ingestion", id="ingestion-wrong-type"
    ),
    # ISO patterns
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_1", "us"),
        "iso_3166_1",
        id="iso1-lowercase",
    ),
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_1", "USA"),
        "iso_3166_1",
        id="iso1-alpha3",
    ),
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_1", "global"),
        "iso_3166_1",
        id="iso1-global-lowercase",
    ),
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_2", "USNY"),
        "iso_3166_2",
        id="iso2-no-hyphen",
    ),
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_2", "us-ny"),
        "iso_3166_2",
        id="iso2-lowercase",
    ),
    pytest.param(
        _col("coverage", "areas", 0, "iso_3166_2", "US-ABCD"),
        "iso_3166_2",
        id="iso2-suffix-too-long",
    ),
    pytest.param(_col("coverage", "areas", []), "areas", id="coverage-areas-empty"),
    pytest.param(_col("coverage", "US"), "coverage", id="coverage-wrong-type"),
    # license rules
    pytest.param(
        _col("license", "attribution", ""),
        "attribution must be non-empty",
        id="attribution-missing-when-required",
    ),
    pytest.param(
        _col("license", "requires_attribution", "true"),
        "requires_attribution",
        id="requires-attribution-wrong-type",
    ),
    pytest.param(
        _del("resources", 0, "collection", "license", "type"),
        "type",
        id="license-type-missing",
    ),
    # refresh_schedule enums and month bounds
    pytest.param(
        _col("refresh_schedule", "frequency", "Quarterly"),
        "frequency",
        id="frequency-not-in-enum",
    ),
    pytest.param(
        _col("refresh_schedule", "frequency", 5), "frequency", id="frequency-wrong-type"
    ),
    pytest.param(
        _col("refresh_schedule", "type", "automatic"),
        "type",
        id="refresh-type-not-in-enum",
    ),
    pytest.param(
        _col("refresh_schedule", "month", [0]), "month", id="month-below-range"
    ),
    pytest.param(
        _col("refresh_schedule", "month", [13]), "month", id="month-above-range"
    ),
    pytest.param(
        _col("refresh_schedule", "month", ["1"]), "month", id="month-wrong-type"
    ),
    # data_download rules
    pytest.param(
        _col("data_download", ["https://example.com/data.zip"]),
        "data_download",
        id="data-download-legacy-list",
    ),
    pytest.param(
        _col("data_download", "https://example.com/data.zip"),
        "data_download",
        id="data-download-wrong-type",
    ),
    pytest.param(
        _col("data_download", "type", "ftp"), "type", id="data-download-bad-type"
    ),
    pytest.param(_col("data_download", "url", ""), "url", id="data-download-url-empty"),
    pytest.param(
        _del("resources", 0, "collection", "data_download", "url"),
        "url",
        id="data-download-url-missing",
    ),
    pytest.param(
        _col("data_download", "type", "s3"),
        "url scheme does not match type",
        id="data-download-scheme-mismatch",
    ),
    # api / extras
    pytest.param(_col("api", "url", ""), "url", id="api-url-empty"),
    pytest.param(_col("api", "https://example.com/api"), "api", id="api-wrong-type"),
    pytest.param(_col("extras", {"key": 1}), "extras", id="extras-non-string-value"),
    pytest.param(_col("notes", 1), "notes", id="notes-wrong-type"),
]


@pytest.mark.parametrize("mutate, match", INVALID_CASES)
def test_rejects_invalid_documents(tmp_path, mutate, match):
    doc = _valid_doc()
    mutate(doc)
    with pytest.raises(ValueError, match=match):
        validate_file(_write(tmp_path, doc))


def test_rejects_data_download_scheme_mismatch_direct():
    with pytest.raises(ValueError, match="url scheme does not match type"):
        DataDownload(type="s3", url="https://example.com/data.zip")


# ── file-level rules ─────────────────────────────────────────────────────────


def test_rejects_provider_label_stem_mismatch(tmp_path):
    with pytest.raises(ValueError, match="must match file stem"):
        validate_file(_write(tmp_path, _valid_doc(), name="acme_corp.json"))


def test_rejects_non_snake_case_filename(tmp_path):
    with pytest.raises(ValueError, match="must be snake_case"):
        validate_file(_write(tmp_path, _valid_doc(), name="Acme-Corp.json"))


def test_rejects_invalid_json(tmp_path):
    bad = tmp_path / "acme.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError):
        validate_file(bad)


def test_rejects_duplicate_provider_labels_across_files(tmp_path):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    first = _write(dir_a, _valid_doc())
    second = _write(dir_b, _valid_doc())
    errors = validate_all([first, second])
    assert len(errors) == 1
    assert errors[0][0] == second
    assert "already used by" in str(errors[0][1])


def test_validate_all_no_errors_for_single_valid_file(tmp_path):
    path = _write(tmp_path, _valid_doc())
    assert validate_all([path]) == []


# ── CLI entrypoint ───────────────────────────────────────────────────────────


class TestMain:
    def test_no_args_prints_usage_and_returns_2(self, capsys):
        assert main([]) == 2
        assert "usage" in capsys.readouterr().err

    def test_valid_files_return_0(self, tmp_path, capsys):
        path = _write(tmp_path, _valid_doc())
        assert main([str(path)]) == 0
        assert "1/1" in capsys.readouterr().out

    def test_invalid_file_returns_1_and_prints_failure(self, tmp_path, capsys):
        doc = _valid_doc()
        doc["surprise"] = 1
        path = _write(tmp_path, doc)
        assert main([str(path)]) == 1
        captured = capsys.readouterr()
        assert "FAIL" in captured.err
        assert "0/1" in captured.out
