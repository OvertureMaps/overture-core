"""Unit tests for overture_core.urls: generic URL string utilities."""

from overture_core.urls import mask_url_credentials

_TOKEN = "SUPERSECRETAUTHTOKEN"
_INDEX_URL = f"https://aws:{_TOKEN}@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"


class TestMaskUrlCredentials:
    def test_redacts_password(self):
        assert mask_url_credentials(_INDEX_URL) == (
            "https://aws:***@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"
        )
        assert _TOKEN not in mask_url_credentials(_INDEX_URL)

    def test_handles_free_text(self):
        text = f"could not connect to {_INDEX_URL} (timeout)"
        masked = mask_url_credentials(text)
        assert _TOKEN not in masked
        assert "***@" in masked

    def test_noop_without_credentials(self):
        url = "https://pypi.org/simple/"
        assert mask_url_credentials(url) == url

    def test_masks_multiple_urls_in_the_same_text(self):
        text = f"tried {_INDEX_URL} then https://user:pw@other.example/x"
        masked = mask_url_credentials(text)
        assert _TOKEN not in masked
        assert "pw" not in masked
        assert masked.count("***@") == 2
