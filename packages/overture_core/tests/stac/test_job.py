"""Unit tests for the PublishStac orchestrator.

Verifies orchestration only — that ``execute_job`` calls the catalog
helpers in the right order with the right arguments. Catalog functions
themselves are covered in ``test_catalog.py``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from overture_core.stac.job import PublishStac


def _run(
    params: dict,
    *,
    existing_schemas: dict[str, str] | None = None,
    public_releases: list[str] | None = None,
    schema_from_rc: str = "v1.99.0",
    schema_from_released_rc: str = "v1.99.0",
):
    """Run PublishStac with all catalog helpers mocked; return the mocks."""
    with (
        patch("overture_core.stac.job.read_existing_stac_schemas") as mock_existing,
        patch("overture_core.stac.job.read_schema_version_from_rc_bundle") as mock_rc,
        patch("overture_core.stac.job.read_schema_from_released_rc") as mock_heal,
        patch("overture_core.stac.job.list_public_releases") as mock_list,
        patch("overture_core.stac.job.build_release_catalog") as mock_build,
        patch("overture_core.stac.job.mirror_directory_to_s3") as mock_mirror,
    ):
        mock_existing.return_value = dict(existing_schemas or {})
        mock_rc.return_value = schema_from_rc
        mock_heal.return_value = schema_from_released_rc
        mock_list.return_value = list(public_releases or [])
        mock_mirror.return_value = 0
        PublishStac().run(json.dumps(params))
        return {
            "existing": mock_existing,
            "rc": mock_rc,
            "heal": mock_heal,
            "list": mock_list,
            "build": mock_build,
            "mirror": mock_mirror,
        }


_BASE = {"scratch_bucket": "scratch-bkt", "extras_bucket": "extras-bkt"}


# ── Single-release mode ───────────────────────────────────────────────────────


class TestSingleReleaseMode:
    _SINGLE = {
        **_BASE,
        "release": "2026-05-20.0",
        "source_path": "release_candidate/release=2026-05-20.0/run=X",
    }

    def test_reads_schema_from_rc_bundle_only(self):
        mocks = _run(self._SINGLE, schema_from_rc="v1.18.0")
        mocks["rc"].assert_called_once_with(
            "scratch-bkt", "release_candidate/release=2026-05-20.0/run=X"
        )
        mocks["existing"].assert_not_called()
        mocks["list"].assert_not_called()

    def test_builds_catalog_only_for_that_release(self):
        mocks = _run(self._SINGLE, schema_from_rc="v1.18.0")
        assert mocks["build"].call_count == 1
        assert mocks["build"].call_args.kwargs["release"] == "2026-05-20.0"
        assert mocks["build"].call_args.kwargs["schema_version"] == "v1.18.0"

    def test_mirror_scoped_to_release_prefix(self):
        mocks = _run(self._SINGLE)
        args = mocks["mirror"].call_args.args
        assert args[1] == "extras-bkt"
        assert args[2] == "stac/2026-05-20.0/"
        # local root should be output/<release>, not output/
        assert args[0].name == "2026-05-20.0"

    def test_partial_single_release_params_raises_listing_missing_names(self):
        # release set, source_path + scratch_bucket omitted
        with pytest.raises(
            ValueError,
            match="single-release mode requires.*source_path.*scratch_bucket",
        ):
            _run({"extras_bucket": "extras-bkt", "release": "2026-05-20.0"})


# ── Walk mode ─────────────────────────────────────────────────────────────────


class TestWalkMode:
    def test_builds_catalog_for_each_discovered_release(self):
        mocks = _run(
            _BASE,
            existing_schemas={"2026-04-15.0": "v1.17.0", "2026-05-20.0": "v1.18.0"},
            public_releases=["2026-04-15.0", "2026-05-20.0"],
        )
        assert mocks["build"].call_count == 2
        releases_built = [c.kwargs["release"] for c in mocks["build"].call_args_list]
        assert releases_built == ["2026-04-15.0", "2026-05-20.0"]

    def test_uses_existing_stac_schema_for_known_releases(self):
        mocks = _run(
            _BASE,
            existing_schemas={"2026-05-20.0": "v1.18.0"},
            public_releases=["2026-05-20.0"],
        )
        assert mocks["build"].call_args.kwargs["schema_version"] == "v1.18.0"
        mocks["rc"].assert_not_called()
        mocks["heal"].assert_not_called()

    def test_self_heals_from_scratch_bucket_on_cache_miss(self):
        mocks = _run(
            _BASE,
            existing_schemas={},  # empty cache
            public_releases=["2026-05-20.0"],
            schema_from_released_rc="v1.17.0",
        )
        # self-heal reads from whichever scratch_bucket the caller passed in
        mocks["heal"].assert_called_once_with("scratch-bkt", "2026-05-20.0")
        assert mocks["build"].call_args.kwargs["schema_version"] == "v1.17.0"

    def test_self_heal_failure_aborts_walk(self):
        with (
            patch("overture_core.stac.job.read_existing_stac_schemas", return_value={}),
            patch(
                "overture_core.stac.job.list_public_releases",
                return_value=["2026-05-20.0"],
            ),
            patch(
                "overture_core.stac.job.read_schema_from_released_rc",
                side_effect=RuntimeError("no released marker"),
            ),
            patch("overture_core.stac.job.build_release_catalog") as mock_build,
            patch("overture_core.stac.job.mirror_directory_to_s3"),
        ):
            with pytest.raises(RuntimeError, match="no released marker"):
                PublishStac().run(json.dumps(_BASE))
        mock_build.assert_not_called()

    def test_mirrors_full_stac_prefix(self):
        mocks = _run(_BASE, public_releases=["2026-05-20.0"])
        args = mocks["mirror"].call_args.args
        assert args[1] == "extras-bkt"
        assert args[2] == "stac/"

    def test_default_release_bucket_is_public(self):
        mocks = _run(_BASE)
        mocks["list"].assert_called_once_with("overturemaps-us-west-2")

    def test_release_bucket_can_be_overridden(self):
        mocks = _run({**_BASE, "release_bucket": "my-test-bucket"})
        mocks["list"].assert_called_once_with("my-test-bucket")
