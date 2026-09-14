"""Unit tests for Azure Blob helpers."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from overture_core.cloud.azure import (
    ContentMd5ClearResult,
    blob_account_url,
    clear_content_md5,
)


def test_blob_account_url():
    assert blob_account_url("acct") == "https://acct.blob.core.windows.net"


def _blob(name, md5=None):
    settings = SimpleNamespace(
        content_md5=md5,
        content_type="application/octet-stream",
        content_encoding=None,
        content_language=None,
        content_disposition=None,
        cache_control=None,
    )
    return SimpleNamespace(name=name, content_settings=settings)


class TestClearContentMd5:
    def _run(self, blobs, set_headers_side_effect=None, prefix="/release/v1/"):
        container = MagicMock()
        container.list_blobs.return_value = blobs
        blob_client = container.get_blob_client.return_value
        if set_headers_side_effect:
            blob_client.set_http_headers.side_effect = set_headers_side_effect
        service_cls = MagicMock()
        service_cls.return_value.get_container_client.return_value = container
        with (
            patch("azure.storage.blob.BlobServiceClient", service_cls),
            patch("azure.storage.blob.ContentSettings") as content_settings,
        ):
            result = clear_content_md5("acct", "container", prefix, "sv=tok")
        return result, container, service_cls, content_settings

    def test_clears_only_blobs_with_md5(self):
        blobs = [
            _blob("release/v1/a.parquet", md5=b"x"),
            _blob("release/v1/b.parquet"),
            SimpleNamespace(name="release/v1/c", content_settings=None),
        ]
        result, container, service_cls, content_settings = self._run(blobs)
        assert result == ContentMd5ClearResult(
            scanned=3, had_md5=1, cleared=1, errors=[]
        )
        assert result.to_dict() == {
            "scanned": 3,
            "had_md5": 1,
            "cleared": 1,
            "errors": 0,
        }
        container.list_blobs.assert_called_once_with(name_starts_with="release/v1/")
        service_cls.assert_called_once_with(
            account_url="https://acct.blob.core.windows.net", credential="sv=tok"
        )
        assert content_settings.call_args.kwargs["content_md5"] is None
        assert (
            content_settings.call_args.kwargs["content_type"]
            == "application/octet-stream"
        )

    def test_empty_prefix_scans_whole_container(self):
        _, container, _, _ = self._run([], prefix="")
        container.list_blobs.assert_called_once_with(name_starts_with="")

    def test_raises_on_errors_with_result_attached(self):
        with pytest.raises(
            RuntimeError, match="1 blob\\(s\\); first failure: release/v1/a"
        ) as info:
            self._run(
                [_blob("release/v1/a", md5=b"x")],
                set_headers_side_effect=OSError("nope"),
            )
        assert info.value.result.cleared == 0
        assert info.value.result.errors == ["release/v1/a: nope"]
