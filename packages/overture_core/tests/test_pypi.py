"""Unit tests for overture_core.pypi: HTTP/PyPI downloaders."""

import sys
import types
from unittest.mock import MagicMock

import pytest

from overture_core.pypi import HttpDownloader, PyPiDownloader

_TOKEN = "SUPERSECRETAUTHTOKEN"
_INDEX_URL = f"https://aws:{_TOKEN}@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"


def _make_fake_sh(pip_impl):
    """Build a fake ``sh`` module exposing ``pip`` and ``ErrorReturnCode``."""
    fake_sh = types.ModuleType("sh")

    class ErrorReturnCode(Exception):
        def __init__(self, stderr: bytes = b""):
            super().__init__("pip failed")
            self.stderr = stderr

    fake_sh.pip = pip_impl
    fake_sh.ErrorReturnCode = ErrorReturnCode
    return fake_sh


def _make_downloader(tmp_path):
    client = MagicMock()
    client.get_url.return_value = _INDEX_URL
    return PyPiDownloader(client, str(tmp_path))


class TestHttpDownloader:
    def test_download_url_writes_file(self, tmp_path, monkeypatch):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_get = MagicMock(return_value=mock_response)
        monkeypatch.setattr("overture_core.pypi.requests.get", mock_get)

        downloader = HttpDownloader(str(tmp_path))
        result = downloader.download_url("https://example.com/pkg/file.whl")

        mock_get.assert_called_once_with(
            "https://example.com/pkg/file.whl", stream=True, timeout=300
        )
        mock_response.raise_for_status.assert_called_once()
        assert result == str(tmp_path / "file.whl")
        assert (tmp_path / "file.whl").read_bytes() == b"chunk1chunk2"

    def test_download_urls_skips_blank_entries(self, tmp_path, monkeypatch):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"data"]
        monkeypatch.setattr(
            "overture_core.pypi.requests.get", MagicMock(return_value=mock_response)
        )

        downloader = HttpDownloader(str(tmp_path))
        result = downloader.download_urls(["https://example.com/a.whl", "", "  "])

        assert result == [str(tmp_path / "a.whl")]


class TestPyPiDownloader:
    def test_all_packages_downloaded_in_single_pip_call(self, tmp_path, monkeypatch):
        """Downloading multiple packages should use a single pip call so pip
        resolves a consistent dependency tree and avoids duplicate/conflicting
        transitive dependency versions."""
        captured = {}

        def fake_pip(*args, **kwargs):
            captured["args"] = args

        monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

        packages = ["overture_transportation==1.0.0", "numba", "llvmlite", "rapidfuzz"]
        _make_downloader(tmp_path).download_packages(packages, python_version="3.11")

        for pkg in packages:
            assert pkg in captured["args"]

    def test_empty_packages_skips_pip(self, tmp_path, monkeypatch):
        called = {"pip": False}

        def fake_pip(*args, **kwargs):
            called["pip"] = True

        monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

        _make_downloader(tmp_path).download_packages([], python_version="3.11")
        assert called["pip"] is False

    def test_pip_flags_are_passed(self, tmp_path, monkeypatch):
        captured = {}

        def fake_pip(*args, **kwargs):
            captured["args"] = args

        monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

        _make_downloader(tmp_path).download_packages(["numba"], python_version="3.11")

        args = captured["args"]
        assert "download" in args
        assert "--python-version" in args
        assert "3.11" in args
        assert "--only-binary" in args
        assert ":all:" in args
        assert "numba" in args

    def test_token_passed_via_env_not_argv(self, tmp_path, monkeypatch):
        captured = {}

        def fake_pip(*args, **kwargs):
            captured["args"] = args
            captured["env"] = kwargs.get("_env")

        monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

        _make_downloader(tmp_path).download_packages(["mypkg"], "3.11")

        # Token must never appear in the command-line arguments.
        assert all(_TOKEN not in str(arg) for arg in captured["args"])
        assert "--index-url" not in captured["args"]
        # Token is delivered to pip via the environment instead.
        assert captured["env"]["PIP_INDEX_URL"] == _INDEX_URL

    def test_error_message_masks_token(self, tmp_path, monkeypatch, caplog):
        import logging

        fake_sh = _make_fake_sh(None)

        def fake_pip(*args, **kwargs):
            raise fake_sh.ErrorReturnCode(
                stderr=f"ERROR: failed fetching {_INDEX_URL}".encode()
            )

        fake_sh.pip = fake_pip
        monkeypatch.setitem(sys.modules, "sh", fake_sh)

        downloader = _make_downloader(tmp_path)
        with caplog.at_level(logging.ERROR), pytest.raises(fake_sh.ErrorReturnCode):
            downloader.download_packages(["mypkg"], "3.11")

        assert _TOKEN not in caplog.text
        assert "***@" in caplog.text
