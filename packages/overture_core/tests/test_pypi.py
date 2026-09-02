"""Unit tests for overture_core.pypi: HTTP/PyPI downloaders."""

import subprocess
from unittest.mock import MagicMock

import pytest

from overture_core.pypi import HttpDownloader, PyPiDownloader

_TOKEN = "SUPERSECRETAUTHTOKEN"
_INDEX_URL = f"https://aws:{_TOKEN}@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"


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
        mock_run = MagicMock()
        monkeypatch.setattr("overture_core.pypi.subprocess.run", mock_run)

        packages = ["overture_transportation==1.0.0", "numba", "llvmlite", "rapidfuzz"]
        _make_downloader(tmp_path).download_packages(packages, python_version="3.11")

        cmd = mock_run.call_args.args[0]
        for pkg in packages:
            assert pkg in cmd

    def test_empty_packages_skips_pip(self, tmp_path, monkeypatch):
        mock_run = MagicMock()
        monkeypatch.setattr("overture_core.pypi.subprocess.run", mock_run)

        _make_downloader(tmp_path).download_packages([], python_version="3.11")
        mock_run.assert_not_called()

    def test_pip_flags_are_passed(self, tmp_path, monkeypatch):
        mock_run = MagicMock()
        monkeypatch.setattr("overture_core.pypi.subprocess.run", mock_run)

        _make_downloader(tmp_path).download_packages(["numba"], python_version="3.11")

        cmd = mock_run.call_args.args[0]
        assert cmd[0] == "pip"
        assert "download" in cmd
        assert "--python-version" in cmd
        assert "3.11" in cmd
        assert "--only-binary" in cmd
        assert ":all:" in cmd
        assert "numba" in cmd

    def test_token_passed_via_env_not_argv(self, tmp_path, monkeypatch):
        mock_run = MagicMock()
        monkeypatch.setattr("overture_core.pypi.subprocess.run", mock_run)

        _make_downloader(tmp_path).download_packages(["mypkg"], "3.11")

        cmd = mock_run.call_args.args[0]
        env = mock_run.call_args.kwargs["env"]
        # Token must never appear in the command-line arguments.
        assert all(_TOKEN not in str(arg) for arg in cmd)
        assert "--index-url" not in cmd
        # Token is delivered to pip via the environment instead.
        assert env["PIP_INDEX_URL"] == _INDEX_URL

    def test_error_message_masks_token(self, tmp_path, monkeypatch, caplog):
        import logging

        error = subprocess.CalledProcessError(
            returncode=1,
            cmd=["pip", "download"],
            stderr=f"ERROR: failed fetching {_INDEX_URL}".encode(),
        )
        mock_run = MagicMock(side_effect=error)
        monkeypatch.setattr("overture_core.pypi.subprocess.run", mock_run)

        downloader = _make_downloader(tmp_path)
        with (
            caplog.at_level(logging.ERROR),
            pytest.raises(subprocess.CalledProcessError),
        ):
            downloader.download_packages(["mypkg"], "3.11")

        assert _TOKEN not in caplog.text
        assert "***@" in caplog.text
