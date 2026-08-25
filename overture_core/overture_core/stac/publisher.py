"""STAC publishing operations. Composed by ``job.py``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
import pystac
from botocore import UNSIGNED
from botocore.config import Config
from overture_stac.overture_stac import OvertureRelease

PROD_ROOT_HREF = "https://stac.overturemaps.org"
STAC_S3_PREFIX = "stac/"
PUBLIC_RELEASE_BUCKET = "overturemaps-us-west-2"


def read_schema_version_from_rc_bundle(
    scratch_bucket: str, input_source_path: str
) -> str:
    """Read ``schema_version`` from an RC bundle's ``metadata.json``."""
    key = f"{input_source_path.rstrip('/')}/metadata.json"
    obj = boto3.client("s3").get_object(Bucket=scratch_bucket, Key=key)
    data: dict[str, Any] = json.loads(obj["Body"].read())
    for inp in data.get("cdp_release_candidate_dag", {}).get("inputs", []):
        for section_val in inp.values():
            if isinstance(section_val, dict):
                schema_version = section_val.get("parameters", {}).get("schema_version")
                if schema_version:
                    return schema_version
    raise RuntimeError(f"schema_version not found in s3://{scratch_bucket}/{key}")


def read_schema_from_released_rc(scratch_bucket: str, release: str) -> str:
    """Find the RC bundle marked ``released`` for *release* and read its schema.

    Walks ``release_candidate/release={release}/run=*/`` in *scratch_bucket*,
    picks the run(s) carrying a ``released`` marker (the one that became a real
    release), and returns its schema_version. Raises on any failure — used as a
    LOUD self-heal fallback from walk mode.
    """
    import botocore.exceptions

    s3 = boto3.client("s3")
    prefix = f"release_candidate/release={release}/"

    runs: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=scratch_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            run = cp["Prefix"][len(prefix) :].rstrip("/")
            if run.startswith("run="):
                runs.append(run)

    if not runs:
        raise RuntimeError(f"No RC runs found under s3://{scratch_bucket}/{prefix}")

    released_runs: list[str] = []
    for run in runs:
        try:
            s3.head_object(Bucket=scratch_bucket, Key=f"{prefix}{run}/released")
            released_runs.append(run)
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                continue
            raise

    if not released_runs:
        raise RuntimeError(
            f"No RC run under s3://{scratch_bucket}/{prefix} has a 'released' marker"
        )

    chosen_run = sorted(released_runs)[-1]
    return read_schema_version_from_rc_bundle(scratch_bucket, f"{prefix}{chosen_run}")


def list_public_releases(
    release_bucket: str = PUBLIC_RELEASE_BUCKET, prefix: str = "release/"
) -> list[str]:
    """List release versions currently present under ``s3://{release_bucket}/{prefix}``."""
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    releases: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=release_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"][len(prefix) :].rstrip("/")
            if name:
                releases.append(name)
    return sorted(releases)


def read_existing_stac_schemas(
    extras_bucket: str, prefix: str = STAC_S3_PREFIX
) -> dict[str, str]:
    """Return ``{release: schema_version}`` extracted from any existing per-release
    ``catalog.json`` at ``s3://{extras_bucket}/{prefix}<release>/catalog.json``.

    Missing / unreadable / schema-less catalogs are silently skipped — this
    map is a best-effort cache, not authoritative.
    """
    s3 = boto3.client("s3")
    schemas: dict[str, str] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=extras_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            release = cp["Prefix"][len(prefix) :].rstrip("/")
            if not release:
                continue
            try:
                obj = s3.get_object(
                    Bucket=extras_bucket, Key=f"{prefix}{release}/catalog.json"
                )
                data = json.loads(obj["Body"].read())
                schema = data.get("schema:version")
                if schema:
                    schemas[release] = schema
            except Exception:
                continue
    return schemas


def build_release_catalog(
    release: str,
    schema_version: str | None,
    output: Path,
    root_href: str = PROD_ROOT_HREF,
    workers: int = 4,
) -> None:
    """Build one release's STAC catalog into *output*.

    TODO: move upstream into ``overture-stac`` — this is the exact call
    sequence in that library's CLI; belongs as a public API there.
    """
    catalog = OvertureRelease(
        release=release,
        schema=schema_version,
        output=output,
        debug=False,
    )
    catalog.build_release_catalog(
        title=f"{release} Overture Release", max_workers=workers
    )
    catalog.release_catalog.normalize_hrefs(f"{root_href}/{release}/")
    catalog.release_catalog.save(
        catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
        dest_href=str(output / release),
    )


def mirror_directory_to_s3(local_root: Path, bucket: str, prefix: str) -> int:
    """Mirror *local_root* to ``s3://{bucket}/{prefix}``; return orphan-delete count."""
    s3 = boto3.client("s3")

    local_keys: set[str] = set()
    for path in local_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix}{rel}"
        local_keys.add(key)
        s3.upload_file(str(path), bucket, key)

    remote_keys: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            remote_keys.add(obj["Key"])

    orphans = list(remote_keys - local_keys)
    for i in range(0, len(orphans), 1000):
        batch = orphans[i : i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch]},
        )
    return len(orphans)
