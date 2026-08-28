"""``LatestRelease`` — query the STAC root catalog for the newest release.

Read-only, HTTPS-only. No AWS credentials, no S3 access. Works against any
STAC root that exposes ``catalog.json`` with per-release child catalogs.

The resolved release ID is logged and stashed on ``self.latest_release`` so
callers that instantiate the job directly (rather than via ``run()`` from a
container entrypoint) can consume the value programmatically.
"""

from __future__ import annotations

from overture_serverless.job import ServerlessPythonJob

from overture_core.stac.catalog import (
    PROD_ROOT_HREF,
    read_latest_release_from_stac,
)


class LatestRelease(ServerlessPythonJob):
    latest_release: str | None = None

    def execute_job(self) -> None:
        root_href = self.get_param(
            "root_href", default=PROD_ROOT_HREF, is_required=False
        )
        self.latest_release = read_latest_release_from_stac(root_href)
        self.log(f"Latest release at {root_href}: {self.latest_release}")
