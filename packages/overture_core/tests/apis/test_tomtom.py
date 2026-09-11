"""Unit tests for the TomTom MCAPI client."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from overture_core.apis.tomtom import (
    McapiClient,
    McapiRelease,
    get_latest_orbis_release,
    get_latest_orbis_version,
)

PATCH_GET = "overture_core.apis.tomtom.requests.get"


def _response(content):
    resp = MagicMock()
    resp.json.return_value = {"content": content}
    return resp


def _released(version, release_id=7, due="2025-01-10"):
    return {
        "id": release_id,
        "version": version,
        "dueDate": due,
        "state": {"type": "Released"},
    }


def _mcapi(families=None, products=None, releases=None):
    """Return a requests.get side effect routing by URL path."""
    families = [{"id": 1}] if families is None else families
    products = [{"id": 2}] if products is None else products
    releases = [_released("2502.000")] if releases is None else releases

    def get(url, headers, params, timeout):
        assert headers == {"Authorization": "key"}
        assert timeout == 10
        if url.endswith("/families"):
            assert params == {"filter": "name eq 'Overture Transportation'"}
            return _response(families)
        if url.endswith("/families/1/products"):
            assert params == {"filter": "name eq 'WRL'"}
            return _response(products)
        if url.endswith("/products/2/releases"):
            assert params is None
            return _response(releases)
        raise AssertionError(f"unexpected url {url}")

    return get


class TestLatestRelease:
    def test_happy_path(self):
        with patch(PATCH_GET, side_effect=_mcapi()):
            release = McapiClient("key").latest_release()
        assert release == McapiRelease(release_id=7, version="2502.000", yyww="2502")

    def test_picks_newest_released_by_due_date(self):
        releases = [
            _released("2501.000", release_id=1, due="2025-01-03"),
            {
                "id": 9,
                "version": "2503.000",
                "dueDate": "2025-01-17",
                "state": {"type": "Planned"},
            },
            _released("2502.000", release_id=2, due="2025-01-10"),
        ]
        with patch(PATCH_GET, side_effect=_mcapi(releases=releases)):
            release = McapiClient("key").latest_release()
        assert release.release_id == 2

    def test_no_family(self):
        with patch(PATCH_GET, side_effect=_mcapi(families=[])):
            assert McapiClient("key").latest_release() is None

    def test_no_product(self):
        with patch(PATCH_GET, side_effect=_mcapi(products=[])):
            assert McapiClient("key").latest_release() is None

    def test_no_released_versions(self):
        with patch(PATCH_GET, side_effect=_mcapi(releases=[])):
            assert McapiClient("key").latest_release() is None

    def test_invalid_yyww(self):
        with patch(PATCH_GET, side_effect=_mcapi(releases=[_released("v25.2")])):
            assert McapiClient("key").latest_release() is None

    def test_missing_release_id(self):
        with patch(
            PATCH_GET,
            side_effect=_mcapi(releases=[_released("2502.000", release_id=None)]),
        ):
            assert McapiClient("key").latest_release() is None

    def test_http_error_propagates(self):
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("401")
        with patch(PATCH_GET, return_value=resp):
            with pytest.raises(requests.HTTPError):
                McapiClient("key").latest_release()

    def test_base_url_trailing_slash_normalized(self):
        with patch(PATCH_GET, side_effect=_mcapi()) as get:
            McapiClient("key", base_url="https://example.test/mcapi/").latest_release()
        assert get.call_args_list[0].args[0] == "https://example.test/mcapi/families"


class TestConvenienceWrappers:
    def test_release_and_version(self):
        with patch(PATCH_GET, side_effect=_mcapi()):
            assert get_latest_orbis_release("key").yyww == "2502"
            assert get_latest_orbis_version("key") == "2502"

    def test_request_exception_swallowed(self):
        with patch(PATCH_GET, side_effect=requests.ConnectionError("down")):
            assert get_latest_orbis_release("key") is None
            assert get_latest_orbis_version("key") is None
