"""Unit tests for the pure-function STAC publishing operations."""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from overture_core.stac.catalog import (
    build_release_catalog,
    list_public_releases,
    mirror_directory_to_s3,
    read_existing_stac_schemas,
    read_latest_release_from_stac,
    read_schema_from_released_rc,
    read_schema_version_from_rc_bundle,
)


def _bytes_body(payload: dict | list) -> io.BytesIO:
    return io.BytesIO(json.dumps(payload).encode("utf-8"))


# ── read_schema_version_from_rc_bundle ────────────────────────────────────────


class TestReadSchemaVersion:
    def _stub_s3(self, body: dict) -> MagicMock:
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": _bytes_body(body)}
        return s3

    def test_returns_schema_from_first_theme_input(self):
        body = {
            "cdp_release_candidate_dag": {
                "inputs": [
                    {"buildings_promote": {"parameters": {"schema_version": "v1.18.0"}}}
                ]
            }
        }
        with patch(
            "overture_core.stac.catalog.boto3.client",
            return_value=self._stub_s3(body),
        ):
            assert (
                read_schema_version_from_rc_bundle("bucket", "path/to/bundle")
                == "v1.18.0"
            )

    def test_scans_multiple_inputs_until_schema_found(self):
        body = {
            "cdp_release_candidate_dag": {
                "inputs": [
                    {"noschema_input": {"parameters": {}}},
                    {"places_promote": {"parameters": {"schema_version": "v2.0.0"}}},
                ]
            }
        }
        with patch(
            "overture_core.stac.catalog.boto3.client",
            return_value=self._stub_s3(body),
        ):
            assert (
                read_schema_version_from_rc_bundle("bucket", "path/to/bundle")
                == "v2.0.0"
            )

    def test_missing_schema_raises_runtime_error(self):
        body = {"cdp_release_candidate_dag": {"inputs": []}}
        with patch(
            "overture_core.stac.catalog.boto3.client",
            return_value=self._stub_s3(body),
        ):
            with pytest.raises(RuntimeError, match="schema_version not found"):
                read_schema_version_from_rc_bundle("bucket", "path/to/bundle")

    def test_strips_trailing_slash_from_input_source_path(self):
        s3 = self._stub_s3(
            {
                "cdp_release_candidate_dag": {
                    "inputs": [{"x": {"parameters": {"schema_version": "v1.0.0"}}}]
                }
            }
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            read_schema_version_from_rc_bundle("bucket", "path/to/bundle/")

        s3.get_object.assert_called_once_with(
            Bucket="bucket", Key="path/to/bundle/metadata.json"
        )


# ── read_schema_from_released_rc ──────────────────────────────────────────────


class TestReadSchemaFromReleasedRc:
    def _stub_s3(
        self,
        *,
        runs: list[str],
        released_runs: set[str],
        metadata_body: dict | None = None,
    ) -> MagicMock:
        import botocore.exceptions

        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": f"release_candidate/release=X/{r}/"} for r in runs
                ]
            }
        ]
        s3.get_paginator.return_value = paginator

        def _head(Bucket: str, Key: str):
            for r in released_runs:
                if Key == f"release_candidate/release=X/{r}/released":
                    return {}
            err = botocore.exceptions.ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
            raise err

        s3.head_object.side_effect = _head

        if metadata_body is not None:
            s3.get_object.return_value = {"Body": _bytes_body(metadata_body)}

        return s3

    def _payload(self, schema: str) -> dict:
        return {
            "cdp_release_candidate_dag": {
                "inputs": [
                    {"theme_x_promote": {"parameters": {"schema_version": schema}}}
                ]
            }
        }

    def test_returns_schema_from_released_run(self):
        s3 = self._stub_s3(
            runs=["run=1", "run=2"],
            released_runs={"run=2"},
            metadata_body=self._payload("v1.18.0"),
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            assert read_schema_from_released_rc("bkt", "X") == "v1.18.0"

    def test_picks_latest_when_multiple_released(self):
        s3 = self._stub_s3(
            runs=["run=1", "run=2", "run=3"],
            released_runs={"run=1", "run=3"},
            metadata_body=self._payload("v2.0.0"),
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            read_schema_from_released_rc("bkt", "X")
        get_calls = [c for c in s3.get_object.call_args_list]
        assert (
            get_calls[0].kwargs["Key"]
            == "release_candidate/release=X/run=3/metadata.json"
        )

    def test_raises_when_no_runs_found(self):
        s3 = self._stub_s3(runs=[], released_runs=set())
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            with pytest.raises(RuntimeError, match="No RC runs found"):
                read_schema_from_released_rc("bkt", "X")

    def test_raises_when_no_run_has_released_marker(self):
        s3 = self._stub_s3(runs=["run=1", "run=2"], released_runs=set())
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            with pytest.raises(RuntimeError, match="'released' marker"):
                read_schema_from_released_rc("bkt", "X")

    def test_reraises_non_404_head_object_errors(self):
        import botocore.exceptions

        s3 = self._stub_s3(runs=["run=1"], released_runs=set())
        s3.head_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            with pytest.raises(botocore.exceptions.ClientError, match="Forbidden"):
                read_schema_from_released_rc("bkt", "X")


# ── list_public_releases ──────────────────────────────────────────────────────


class TestListPublicReleases:
    def _stub_s3(self, common_prefixes: list[str]) -> MagicMock:
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": p} for p in common_prefixes]}
        ]
        s3.get_paginator.return_value = paginator
        return s3

    def test_returns_release_names_sorted(self):
        s3 = self._stub_s3(
            [
                "release/2026-05-20.0/",
                "release/2026-04-15.0/",
                "release/2026-06-01.0/",
            ]
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            releases = list_public_releases("bkt")

        assert releases == ["2026-04-15.0", "2026-05-20.0", "2026-06-01.0"]

    def test_ignores_empty_prefixes(self):
        s3 = self._stub_s3(["release/", "release/2026-05-20.0/"])
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            releases = list_public_releases("bkt")

        assert releases == ["2026-05-20.0"]


# ── read_latest_release_from_stac ─────────────────────────────────────────────


class TestReadLatestReleaseFromStac:
    def _stub_catalog(self, child_ids: list[str]) -> MagicMock:
        catalog = MagicMock()
        catalog.get_children.return_value = [MagicMock(id=cid) for cid in child_ids]
        return catalog

    def test_returns_greatest_child_id(self):
        catalog = self._stub_catalog(["2026-05-20.0", "2026-04-15.0", "2026-06-01.0"])
        with patch(
            "overture_core.stac.catalog.pystac.Catalog.from_file",
            return_value=catalog,
        ) as mock_from_file:
            assert (
                read_latest_release_from_stac("https://stac.example.com")
                == "2026-06-01.0"
            )
        mock_from_file.assert_called_once_with("https://stac.example.com/catalog.json")

    def test_strips_trailing_slash_from_root_href(self):
        catalog = self._stub_catalog(["2026-05-20.0"])
        with patch(
            "overture_core.stac.catalog.pystac.Catalog.from_file",
            return_value=catalog,
        ) as mock_from_file:
            read_latest_release_from_stac("https://stac.example.com/")
        mock_from_file.assert_called_once_with("https://stac.example.com/catalog.json")

    def test_raises_when_catalog_has_no_children(self):
        catalog = self._stub_catalog([])
        with patch(
            "overture_core.stac.catalog.pystac.Catalog.from_file",
            return_value=catalog,
        ):
            with pytest.raises(RuntimeError, match="No release catalogs found"):
                read_latest_release_from_stac("https://stac.example.com")


# ── read_existing_stac_schemas ────────────────────────────────────────────────


class TestReadExistingStacSchemas:
    def _make_s3(
        self,
        common_prefixes: list[str],
        catalog_by_release: dict[str, dict | Exception],
    ) -> MagicMock:
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": p} for p in common_prefixes]}
        ]
        s3.get_paginator.return_value = paginator

        def _get_object(Bucket: str, Key: str):
            for release, payload in catalog_by_release.items():
                if Key == f"stac/{release}/catalog.json":
                    if isinstance(payload, Exception):
                        raise payload
                    return {"Body": _bytes_body(payload)}
            raise RuntimeError(f"Unexpected get_object key: {Key}")

        s3.get_object.side_effect = _get_object
        return s3

    def test_extracts_schema_from_each_release_catalog(self):
        s3 = self._make_s3(
            common_prefixes=["stac/2026-05-20.0/", "stac/2026-04-15.0/"],
            catalog_by_release={
                "2026-05-20.0": {"schema:version": "v1.18.0"},
                "2026-04-15.0": {"schema:version": "v1.17.0"},
            },
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            schemas = read_existing_stac_schemas("bkt")

        assert schemas == {
            "2026-05-20.0": "v1.18.0",
            "2026-04-15.0": "v1.17.0",
        }

    def test_skips_releases_with_missing_catalog(self):
        s3 = self._make_s3(
            common_prefixes=["stac/good/", "stac/no-catalog/"],
            catalog_by_release={
                "good": {"schema:version": "v1.0.0"},
                "no-catalog": RuntimeError("NoSuchKey"),
            },
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            schemas = read_existing_stac_schemas("bkt")

        assert schemas == {"good": "v1.0.0"}

    def test_skips_catalog_without_schema_field(self):
        s3 = self._make_s3(
            common_prefixes=["stac/schemaless/", "stac/good/"],
            catalog_by_release={
                "schemaless": {"other": "field"},
                "good": {"schema:version": "v1.0.0"},
            },
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            schemas = read_existing_stac_schemas("bkt")

        assert schemas == {"good": "v1.0.0"}

    def test_skips_empty_release_name(self):
        s3 = self._make_s3(
            common_prefixes=["stac/", "stac/good/"],
            catalog_by_release={"good": {"schema:version": "v1.0.0"}},
        )
        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            schemas = read_existing_stac_schemas("bkt")

        assert schemas == {"good": "v1.0.0"}


# ── build_release_catalog ─────────────────────────────────────────────────────


class TestBuildReleaseCatalog:
    def test_configures_overture_release_and_saves_absolute_catalog(self, tmp_path):
        with patch(
            "overture_core.stac.catalog.OvertureRelease"
        ) as mock_overture_release:
            catalog_obj = MagicMock()
            mock_overture_release.return_value = catalog_obj

            build_release_catalog(
                release="2026-05-20.0",
                schema_version="v1.18.0",
                output=tmp_path,
                root_href="https://stac.example.com",
                workers=8,
            )

        mock_overture_release.assert_called_once_with(
            release="2026-05-20.0",
            schema="v1.18.0",
            output=tmp_path,
            debug=False,
        )
        catalog_obj.build_release_catalog.assert_called_once_with(
            title="2026-05-20.0 Overture Release", max_workers=8
        )
        catalog_obj.release_catalog.normalize_hrefs.assert_called_once_with(
            "https://stac.example.com/2026-05-20.0/"
        )
        catalog_obj.release_catalog.save.assert_called_once()
        assert catalog_obj.release_catalog.save.call_args.kwargs["dest_href"] == str(
            tmp_path / "2026-05-20.0"
        )

    def test_accepts_none_schema(self, tmp_path):
        with patch("overture_core.stac.catalog.OvertureRelease") as mock_or:
            mock_or.return_value = MagicMock()
            build_release_catalog(
                release="2026-05-20.0",
                schema_version=None,
                output=tmp_path,
            )
        assert mock_or.call_args.kwargs["schema"] is None


# ── mirror_directory_to_s3 ────────────────────────────────────────────────────


class TestMirrorDirectoryToS3:
    def _make_local_tree(self, root: Path, files: list[str]) -> None:
        for rel in files:
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(f"content-of-{rel}")

    def _stub_s3(self, remote_keys: list[str]) -> MagicMock:
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": k} for k in remote_keys]}
        ]
        s3.get_paginator.return_value = paginator
        return s3

    def test_uploads_every_local_file(self, tmp_path):
        self._make_local_tree(tmp_path, ["a.json", "sub/b.json"])
        s3 = self._stub_s3(remote_keys=[])

        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            deleted = mirror_directory_to_s3(tmp_path, "bkt", "stac/")

        assert deleted == 0
        upload_calls = {(c.args[1], c.args[2]) for c in s3.upload_file.call_args_list}
        assert upload_calls == {("bkt", "stac/a.json"), ("bkt", "stac/sub/b.json")}
        s3.delete_objects.assert_not_called()

    def test_deletes_remote_orphans(self, tmp_path):
        self._make_local_tree(tmp_path, ["keep.json"])
        s3 = self._stub_s3(remote_keys=["stac/keep.json", "stac/gone.json"])

        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            deleted = mirror_directory_to_s3(tmp_path, "bkt", "stac/")

        assert deleted == 1
        s3.delete_objects.assert_called_once_with(
            Bucket="bkt",
            Delete={"Objects": [{"Key": "stac/gone.json"}]},
        )

    def test_batches_deletes_in_groups_of_1000(self, tmp_path):
        self._make_local_tree(tmp_path, ["keep.json"])
        orphans = [f"stac/orphan-{i:04d}.json" for i in range(2500)]
        s3 = self._stub_s3(remote_keys=["stac/keep.json", *orphans])

        with patch("overture_core.stac.catalog.boto3.client", return_value=s3):
            deleted = mirror_directory_to_s3(tmp_path, "bkt", "stac/")

        assert deleted == 2500
        assert s3.delete_objects.call_count == 3
        batch_sizes = [
            len(c.kwargs["Delete"]["Objects"]) for c in s3.delete_objects.call_args_list
        ]
        assert batch_sizes == [1000, 1000, 500]
