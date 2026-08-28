"""Unit tests for version-string parsing helpers."""

from unittest.mock import MagicMock, patch

from overture_core.versioning import (
    get_last_modified_version,
    parse_http_last_modified_version,
    parse_iso_version,
)


class TestVersionParsers:
    """Verify version parsing functions produce nodash date strings."""

    def test_parse_iso_version(self):
        assert parse_iso_version("2026-03-03T00:15:08.890986Z") == "20260303"

    def test_parse_iso_version_strips_time(self):
        assert parse_iso_version("2024-11-30T23:59:59.000000Z") == "20241130"

    def test_parse_http_last_modified_version(self):
        assert (
            parse_http_last_modified_version("Sat, 01 Mar 2025 12:00:00 GMT")
            == "20250301"
        )

    def test_parse_http_last_modified_version_end_of_year(self):
        assert (
            parse_http_last_modified_version("Tue, 31 Dec 2024 23:59:59 GMT")
            == "20241231"
        )

    def test_get_last_modified_version(self):
        mock_response = MagicMock()
        mock_response.headers.get.return_value = "Sat, 01 Mar 2025 12:00:00 GMT"
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = get_last_modified_version("https://example.com/data.tsv")

        assert result == "20250301"

    def test_get_last_modified_version_missing_header_raises(self):
        mock_response = MagicMock()
        mock_response.headers.get.return_value = None
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            try:
                get_last_modified_version("https://example.com/data.tsv")
            except ValueError as exc:
                assert "Missing Last-Modified header" in str(exc)
            else:
                raise AssertionError("expected ValueError")

    def test_get_last_modified_version_falls_back_to_today(self):
        mock_response = MagicMock()
        mock_response.headers.get.return_value = None
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response):
            result = get_last_modified_version(
                "https://example.com/data.tsv", fallback_to_today=True
            )

        assert len(result) == 8
        assert result.isdigit()
