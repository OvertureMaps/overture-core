"""TomTom Map Content API (MCAPI) client for release discovery.

Walks ``families -> products -> releases`` to find the newest released
version of a product. Overture's transportation source is the ``WRL`` product
in the ``Overture Transportation`` family, which are the defaults here.
"""

import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

MCAPI_BASE_URL = "https://api.tomtom.com/mcapi"
ORBIS_FAMILY_NAME = "Overture Transportation"
ORBIS_PRODUCT_NAME = "WRL"
_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class McapiRelease:
    """A released MCAPI product version.

    ``version`` is TomTom's full version string; ``yyww`` is its leading
    ISO year-week, the token Overture uses to identify an Orbis release.
    """

    release_id: int
    version: str
    yyww: str


class McapiClient:
    """Thin authenticated wrapper over the MCAPI release endpoints."""

    def __init__(self, api_key: str, base_url: str = MCAPI_BASE_URL):
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": api_key}

    def _get(self, path: str, **params) -> list[dict]:
        response = requests.get(
            f"{self._base_url}{path}",
            headers=self._headers,
            params=params or None,
            timeout=_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json().get("content", [])

    def find_family_id(self, name: str) -> int | None:
        families = self._get("/families", filter=f"name eq '{name}'")
        return families[0]["id"] if families else None

    def find_product_id(self, family_id: int, name: str) -> int | None:
        products = self._get(
            f"/families/{family_id}/products", filter=f"name eq '{name}'"
        )
        return products[0]["id"] if products else None

    def list_released(self, product_id: int) -> list[dict]:
        """Return the product's releases in ``released`` state, newest ``dueDate`` first."""
        releases = [
            r
            for r in self._get(f"/products/{product_id}/releases")
            if r.get("state", {}).get("type", "").lower() == "released"
        ]
        return sorted(releases, key=lambda r: r.get("dueDate", ""), reverse=True)

    def latest_release(
        self,
        family_name: str = ORBIS_FAMILY_NAME,
        product_name: str = ORBIS_PRODUCT_NAME,
    ) -> McapiRelease | None:
        """Return the newest released version of *product_name*, or ``None``.

        Every "not found" and malformed-response case returns ``None`` with a
        logged reason, so a scheduled caller can fall back gracefully; only
        transport/HTTP errors (``requests.RequestException``) propagate.
        """
        family_id = self.find_family_id(family_name)
        if family_id is None:
            logger.warning("MCAPI: no family named %r", family_name)
            return None

        product_id = self.find_product_id(family_id, product_name)
        if product_id is None:
            logger.warning(
                "MCAPI: no product named %r in family %r", product_name, family_name
            )
            return None

        released = self.list_released(product_id)
        if not released:
            logger.warning("MCAPI: no released versions for %r", product_name)
            return None

        latest = released[0]
        version = str(latest.get("version", ""))
        yyww = version[:4]
        if len(yyww) != 4 or not yyww.isdigit():
            logger.warning("MCAPI: invalid YYWW in version %r", version)
            return None
        release_id = latest.get("id")
        try:
            release_id = int(release_id)
        except (TypeError, ValueError):
            logger.warning(
                "MCAPI: missing or non-numeric release id %r for version %r",
                release_id,
                version,
            )
            return None

        logger.info(
            "MCAPI: latest release id=%s version=%s yyww=%s", release_id, version, yyww
        )
        return McapiRelease(release_id=release_id, version=version, yyww=yyww)


def get_latest_orbis_release(
    api_key: str, base_url: str = MCAPI_BASE_URL
) -> McapiRelease | None:
    """Return the latest released Orbis WRL release, or ``None`` on lookup failure.

    Convenience over :meth:`McapiClient.latest_release` with Overture's
    defaults. Swallows ``requests.RequestException`` (logged) so callers polling
    on a schedule get ``None`` instead of a crash on a transient API outage.
    """
    try:
        return McapiClient(api_key, base_url).latest_release()
    except requests.RequestException as exc:
        logger.warning("MCAPI request failed: %s", exc)
        return None


def get_latest_orbis_version(
    api_key: str, base_url: str = MCAPI_BASE_URL
) -> str | None:
    """Return the latest released Orbis ``YYWW`` version string, or ``None``."""
    release = get_latest_orbis_release(api_key, base_url)
    return release.yyww if release else None
