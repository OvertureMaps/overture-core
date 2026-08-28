"""``PublishStac`` — composes ``catalog.py`` helpers into a serverless job.

Two modes, selected by presence of the ``release`` parameter:

- **single-release** (``release`` + ``source_path`` + ``scratch_bucket``
  supplied): publish exactly that release. Schema resolved from the RC bundle
  at ``source_path``. Mirror scoped to ``stac/{release}/`` only — no other
  releases are touched.

- **walk** (``scratch_bucket`` supplied, no ``release``): rebuild the catalog
  for every release currently present in the public release bucket. Schema
  per release comes from the existing STAC (``schema:version`` in each
  release's ``catalog.json``); on cache miss the job **self-heals** by finding
  the RC bundle with the ``released`` marker in ``scratch_bucket`` and
  reading its ``metadata.json``. Self-heal is LOUD — it raises if it can't
  recover the schema for a release, aborting the walk. Mirror covers the
  whole ``stac/`` prefix with delete-orphans, so releases removed by bucket
  lifecycle policy get their STAC items removed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from overture_serverless.job import ServerlessPythonJob

from overture_core.stac.catalog import (
    PROD_ROOT_HREF,
    PUBLIC_RELEASE_BUCKET,
    STAC_S3_PREFIX,
    build_release_catalog,
    list_public_releases,
    mirror_directory_to_s3,
    read_existing_stac_schemas,
    read_schema_from_released_rc,
    read_schema_version_from_rc_bundle,
)


class PublishStac(ServerlessPythonJob):
    def execute_job(self) -> None:
        extras_bucket = self.get_param("extras_bucket")
        root_href = self.get_param(
            "root_href", default=PROD_ROOT_HREF, is_required=False
        )
        workers = int(self.get_param("workers", default=4, is_required=False))

        release = self.get_param("release", is_required=False)

        if release:
            # single-release mode requires the coupled trio.
            source_path = self.get_param("source_path", is_required=False)
            scratch_bucket = self.get_param("scratch_bucket", is_required=False)
            missing = [
                name
                for name, value in [
                    ("source_path", source_path),
                    ("scratch_bucket", scratch_bucket),
                ]
                if not value
            ]
            if missing:
                raise ValueError(
                    f"single-release mode requires release + source_path + scratch_bucket; "
                    f"missing: {missing}"
                )
            self._publish_single(
                release=release,
                source_path=source_path,
                scratch_bucket=scratch_bucket,
                extras_bucket=extras_bucket,
                root_href=root_href,
                workers=workers,
            )
        else:
            scratch_bucket = self.get_param("scratch_bucket")
            release_bucket = self.get_param(
                "release_bucket", default=PUBLIC_RELEASE_BUCKET, is_required=False
            )
            self._publish_walk(
                scratch_bucket=scratch_bucket,
                extras_bucket=extras_bucket,
                release_bucket=release_bucket,
                root_href=root_href,
                workers=workers,
            )

    def _publish_single(
        self,
        release: str,
        source_path: str,
        scratch_bucket: str,
        extras_bucket: str,
        root_href: str,
        workers: int,
    ) -> None:
        schema = read_schema_version_from_rc_bundle(scratch_bucket, source_path)
        self.log(f"Publishing single release {release} with schema {schema}")

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "stac"
            output.mkdir(parents=True, exist_ok=True)
            build_release_catalog(
                release=release,
                schema_version=schema,
                output=output,
                root_href=root_href,
                workers=workers,
            )
            release_prefix = f"{STAC_S3_PREFIX}{release}/"
            deleted = mirror_directory_to_s3(
                output / release, extras_bucket, release_prefix
            )
            self.log(
                f"Mirrored {release} to s3://{extras_bucket}/{release_prefix}; "
                f"deleted {deleted} orphans within that release"
            )

    def _publish_walk(
        self,
        scratch_bucket: str,
        extras_bucket: str,
        release_bucket: str,
        root_href: str,
        workers: int,
    ) -> None:
        known_schemas = read_existing_stac_schemas(extras_bucket, STAC_S3_PREFIX)
        self.log(f"Cached {len(known_schemas)} schemas from existing STAC")

        releases = list_public_releases(release_bucket)
        self.log(
            f"Discovered {len(releases)} releases in s3://{release_bucket}/release/"
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "stac"
            output.mkdir(parents=True, exist_ok=True)
            for release in releases:
                schema = known_schemas.get(release)
                if schema is None:
                    self.log(
                        f"No cached schema for {release}; self-healing from RC bundle in "
                        f"s3://{scratch_bucket}"
                    )
                    schema = read_schema_from_released_rc(scratch_bucket, release)
                    self.log(f"Self-healed {release} -> {schema}")
                build_release_catalog(
                    release=release,
                    schema_version=schema,
                    output=output,
                    root_href=root_href,
                    workers=workers,
                )

            deleted = mirror_directory_to_s3(output, extras_bucket, STAC_S3_PREFIX)
            self.log(
                f"Mirrored {len(releases)} releases to s3://{extras_bucket}/{STAC_S3_PREFIX}; "
                f"deleted {deleted} orphans"
            )
