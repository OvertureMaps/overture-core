"""Unit tests for the LatestRelease orchestrator.

Verifies orchestration only — that ``execute_job`` calls
``read_latest_release_from_stac`` with the right root_href and surfaces the
result. The helper itself is covered in ``test_catalog.py``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from overture_core.stac.latest_release_job import LatestRelease


def _run(params: dict, resolved: str = "2026-06-01.0") -> LatestRelease:
    with patch(
        "overture_core.stac.latest_release_job.read_latest_release_from_stac",
        return_value=resolved,
    ) as mock_read:
        job = LatestRelease()
        job.run(json.dumps(params))
        job._mock_read = mock_read
        return job


class TestLatestRelease:
    def test_defaults_to_prod_root_href(self):
        job = _run({})
        job._mock_read.assert_called_once_with("https://stac.overturemaps.org")

    def test_root_href_can_be_overridden(self):
        job = _run({"root_href": "https://stac.example.com"})
        job._mock_read.assert_called_once_with("https://stac.example.com")

    def test_result_stashed_on_instance(self):
        job = _run({}, resolved="2026-06-01.0")
        assert job.latest_release == "2026-06-01.0"

    def test_accepts_dict_params(self):
        with patch(
            "overture_core.stac.latest_release_job.read_latest_release_from_stac",
            return_value="2026-06-01.0",
        ):
            job = LatestRelease()
            job.run({"root_href": "https://stac.example.com"})
        assert job.latest_release == "2026-06-01.0"
