"""Canonical license texts from the SPDX ``license-list-data`` repository.

Texts are fetched from a pinned release tag so output is reproducible and
nothing is hand-maintained. Bump ``SPDX_LICENSE_LIST_VERSION`` to adopt a new
release.
"""

from __future__ import annotations

import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Iterable

SPDX_LICENSE_LIST_VERSION = "v3.29.0"
SPDX_TEXT_URL = (
    "https://raw.githubusercontent.com/spdx/license-list-data/"
    f"{SPDX_LICENSE_LIST_VERSION}/text/{{id}}.txt"
)
SPDX_PAGE_URL = "https://spdx.org/licenses/{id}.html"
_SPDX_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+-]*")
_TIMEOUT_SECONDS = 30


class LicenseTextError(Exception):
    """A license text could not be obtained for an SPDX id."""


def _get(name: str) -> str:
    url = SPDX_TEXT_URL.format(id=urllib.parse.quote(name))
    with urllib.request.urlopen(url, timeout=_TIMEOUT_SECONDS) as response:
        return response.read().decode("utf-8")


def fetch_license_text(spdx_id: str) -> str:
    """Download the text of ``spdx_id`` from the pinned ``license-list-data`` release.

    Deprecated ids (e.g. ``GPL-2.0``) are published as ``deprecated_<id>.txt``.
    """
    if not _SPDX_ID.fullmatch(spdx_id):
        raise LicenseTextError(f"'{spdx_id}' is not a valid SPDX license id")
    try:
        try:
            return _get(spdx_id)
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
            return _get(f"deprecated_{spdx_id}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise LicenseTextError(
                f"license id '{spdx_id}' not found in SPDX license-list-data "
                f"{SPDX_LICENSE_LIST_VERSION}"
            ) from exc
        raise LicenseTextError(
            f"fetching license text for '{spdx_id}' failed: HTTP {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise LicenseTextError(
            f"fetching license text for '{spdx_id}' failed: {exc}"
        ) from exc


def license_texts(
    spdx_ids: Iterable[str],
    fetch: Callable[[str], str] = fetch_license_text,
) -> dict[str, str]:
    """Map each distinct id to its text, sorted by id. Raises on the first missing id."""
    return {i: fetch(i).strip("\n") for i in sorted(set(spdx_ids))}
