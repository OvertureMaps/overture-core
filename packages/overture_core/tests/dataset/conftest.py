"""Shared fixtures for the dataset test suite."""

import json
from pathlib import Path

import pytest

ACME = {
    "provider": {
        "label": "acme",
        "name": "Acme Maps",
        "url": {"primary": "https://acme.example/", "archive": ""},
    },
    "resources": [
        {
            "label": "planet",
            "name": "Acme Maps",
            "collection": {
                "data_location": {"primary": "", "archive": ""},
                "data_download": {"type": "s3", "url": "s3://acme/planet/"},
                "license": {
                    "url": {"primary": "https://acme.example/license", "archive": ""},
                    "type": "ODbL-1.0",
                    "requires_attribution": True,
                    "text": "",
                    "attribution": "© Acme Maps contributors",
                },
                "coverage": {
                    "areas": [{"iso_3166_1": "GLOBAL"}],
                    "description": "Global",
                },
                "notes": "",
            },
            "ingestion": {},
            "matching": {},
        },
        {
            "label": "coastlines",
            "name": "Acme Coastlines",
            "collection": {
                "data_location": {"primary": "", "archive": ""},
                "license": {
                    "url": {"primary": "https://acme.example/license", "archive": ""},
                    "type": "ODbL-1.0",
                    "requires_attribution": True,
                    "text": "",
                    "attribution": "© Acme Maps contributors",
                },
                "coverage": {
                    "areas": [{"iso_3166_1": "GLOBAL"}],
                    "description": "Global",
                },
                "notes": "",
            },
            "ingestion": {},
            "matching": {},
        },
    ],
}

GLOBEX = {
    "provider": {
        "label": "globex",
        "name": "Globex",
        "url": {"primary": "https://globex.example/", "archive": ""},
    },
    "resources": [
        {
            "label": "places",
            "name": "Globex Places",
            "collection": {
                "data_location": {"primary": "", "archive": ""},
                "license": {
                    "url": {"primary": "", "archive": ""},
                    "type": "CDLA-Permissive-2.0",
                    "requires_attribution": False,
                    "text": "",
                    "attribution": "",
                },
                "coverage": {
                    "areas": [{"iso_3166_1": "US"}, {"iso_3166_1": "CA"}],
                    "description": "",
                },
                "notes": "",
            },
            "ingestion": {},
            "matching": {},
        }
    ],
}

# A resource with no license at all, and a provider with no URL.
UNLICENSED = {
    "provider": {
        "label": "nolic",
        "name": "No License Co",
        "url": {"primary": "", "archive": ""},
    },
    "resources": [
        {
            "label": "stuff",
            "name": "Stuff",
            "collection": {
                "data_location": {"primary": "", "archive": ""},
                "license": None,
                "coverage": {
                    "areas": [{"iso_3166_1": "GLOBAL"}],
                    "description": "Global",
                },
                "notes": "",
            },
            "ingestion": {},
            "matching": {},
        }
    ],
}

POLICY = {
    "base": ["ODbL-1.0", "CC-BY-4.0"],
    "places": ["CDLA-Permissive-2.0", "CC0-1.0"],
}


def write_json(directory: Path, name: str, doc) -> Path:
    path = directory / name
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture()
def datasets_dir(tmp_path) -> Path:
    d = tmp_path / "datasets"
    d.mkdir()
    write_json(d, "acme.json", ACME)
    write_json(d, "globex.json", GLOBEX)
    write_json(d, "nolic.json", UNLICENSED)
    return d


@pytest.fixture()
def policy_path(tmp_path) -> Path:
    return write_json(tmp_path, "license_policy.json", POLICY)
