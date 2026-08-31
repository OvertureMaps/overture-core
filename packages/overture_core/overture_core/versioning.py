"""Version-string parsing helpers for data sources without explicit version metadata."""

import ssl
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def parse_iso_version(date_str: str) -> str:
    """Parse an ISO 8601 date string into a nodash version string.

    Used for sources like LINZ which return published_at as ISO 8601
    e.g. "2026-03-03T00:15:08.890986Z".

    >>> parse_iso_version("2026-03-03T00:15:08.890986Z")
    '20260303'
    """
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    return dt.strftime("%Y%m%d")


def parse_http_last_modified_version(last_modified: str) -> str:
    """Parse an HTTP Last-Modified header value into a nodash version string.

    Used for sources without explicit version metadata (MEVN, Dados Abertos SP).
    HTTP dates follow RFC 2822 e.g. "Sat, 01 Mar 2025 12:00:00 GMT".

    >>> parse_http_last_modified_version("Sat, 01 Mar 2025 12:00:00 GMT")
    '20250301'
    """
    dt = parsedate_to_datetime(last_modified).astimezone(timezone.utc)
    return dt.strftime("%Y%m%d")


def get_last_modified_version(url: str, fallback_to_today: bool = False) -> str:
    """Fetch the Last-Modified header from a URL via HEAD request and return a nodash version.

    If the server does not return a Last-Modified header and fallback_to_today is True,
    today's date is used instead so the DAG run still succeeds. If False, a ValueError is raised.
    """
    req = urllib.request.Request(url, method="HEAD")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
        last_modified = resp.headers.get("Last-Modified")
    if not last_modified:
        if fallback_to_today:
            return datetime.now(timezone.utc).strftime("%Y%m%d")
        raise ValueError(f"Missing Last-Modified header for URL: {url}")
    return parse_http_last_modified_version(last_modified)
