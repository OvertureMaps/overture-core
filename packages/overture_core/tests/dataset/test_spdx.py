"""Tests for pinned SPDX license text lookup."""

import io
import urllib.error

import pytest

from overture_core.dataset import spdx


def test_license_texts_dedupes_sorts_and_strips():
    seen = []

    def fetch(i):
        seen.append(i)
        return f"\n{i} text\n"

    assert spdx.license_texts(["B", "A", "B"], fetch) == {"A": "A text", "B": "B text"}
    assert seen == ["A", "B"]


def test_fetch_uses_pinned_release(monkeypatch):
    urls = []

    def fake_urlopen(url, timeout):
        urls.append(url)
        return io.BytesIO(b"body")

    monkeypatch.setattr(spdx.urllib.request, "urlopen", fake_urlopen)
    assert spdx.fetch_license_text("Apache-2.0") == "body"
    assert spdx.SPDX_LICENSE_LIST_VERSION in urls[0]
    assert urls[0].endswith("/text/Apache-2.0.txt")


def test_missing_id_names_it(monkeypatch):
    def fake_urlopen(url, timeout):
        raise urllib.error.HTTPError(url, 404, "nf", {}, None)

    monkeypatch.setattr(spdx.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(spdx.LicenseTextError, match="'Nope-1.0' not found"):
        spdx.fetch_license_text("Nope-1.0")


@pytest.mark.parametrize("bad", ["", "../x", "LicenseRef-a b", "a/b"])
def test_invalid_id_rejected(bad):
    with pytest.raises(spdx.LicenseTextError, match="not a valid SPDX"):
        spdx.fetch_license_text(bad)
