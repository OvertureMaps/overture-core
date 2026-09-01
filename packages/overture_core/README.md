# overture_core

[![PyPI](https://img.shields.io/pypi/v/overture-core.svg)](https://pypi.org/project/overture-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-core.svg)](https://pypi.org/project/overture-core/)

Shared, framework-agnostic business logic — portable job classes built on [`overture-serverless`](../overture_serverless).

`overture-stac` (and its `pyarrow>=16` floor via `stac-geoparquet`) is an optional extra. Install `overture-core[stac]` (quote it in shells like zsh that glob brackets: `pip install 'overture-core[stac]'`) if you need `stac.job.PublishStac` or `stac.catalog.build_release_catalog`. `stac.job.LatestRelease` only reads the STAC root catalog over HTTPS via `pystac`, so it works without the extra. Plain `overture-core` covers the `cloud`/`iceberg`/`versioning`/`uuids`/`uuids_sql` modules too.

## Modules

Framework-agnostic helpers, each importable on its own without pulling in the job classes below.

| Module | What it's for |
| --- | --- |
| `uuids` | Pure Python `generate_uuid3`/`generate_uuid4`/`generate_uuid5` (no Spark dependency, wrap in `@F.udf`/`@udf` yourself). `generate_uuid6`/`7`/`8` wrap the RFC 9562 stdlib generators added in Python 3.14, raising `NotImplementedError` on older interpreters. |
| `uuids_sql` | `generate_uuid3_sql`/`generate_uuid4_sql`/`generate_uuid5_sql` build the equivalent SQL expression for Spark or Trino (`engine="spark"`/`"trino"`), so a UUID column can be computed server-side instead of through a Python UDF. |
| `versioning` | Parse a source's version from an ISO 8601 date or an HTTP `Last-Modified` header into a nodash (`YYYYMMDD`) string, for sources with no explicit version metadata. |
| `iceberg` | `Platform`/`CatalogKind` enums, `CatalogSpec`/`CatalogBinding` dataclasses, and the Iceberg + Sedona Spark SQL extensions constant shared by every platform's catalog config. |
| `cloud.cloud` | `CloudProvider` enum and a `Partition` dataclass for building Hive-style (`key=value`) or plain partition path segments. |
| `cloud.aws.core` | Account ID, region, and role ARN/assume-role helpers built on boto3. |
| `cloud.aws.object` | S3 object/prefix helpers built on boto3: URI parsing, existence checks, read/write/copy/delete, `list_common_prefixes`. |
| `cloud.aws.codeartifact` | Mint a short-lived CodeArtifact authorization token. |
| `stac.catalog` | STAC catalog reads/writes backing the jobs below: RC bundle schema lookups, `read_latest_release_from_stac`, `build_release_catalog`. |

## Jobs

- **`stac.job.PublishStac`** — publishes the Overture STAC catalog. Two modes selected by the presence of the `release` param:
  - *single-release* (`release` + `source_path` + `scratch_bucket`): publish just that release, mirror scoped to `stac/{release}/`. Schema comes from the RC bundle's `metadata.json`.
  - *walk* (no `release`, requires `scratch_bucket`): rebuild the catalog for every release currently in the public bucket, mirror the whole `stac/` prefix with delete-orphans. Schema per release comes from the existing STAC (`schema:version` in each `catalog.json`); on cache miss the job self-heals by reading schema from the released RC bundle in `scratch_bucket` and raises if it can't recover.

## Future work

- **Bundle-owned schema accessor** — when `ReleaseCandidateBundle` becomes publishable, move `read_schema_version_from_rc_bundle` onto it as `resolved_schema_version()`; single-release mode's schema read shrinks to one line.
- **Upstream `build_release_catalog`** — file a PR against [OvertureMaps/stac](https://github.com/OvertureMaps/stac) exposing this as a public API (their CLI already does the exact call sequence). When it lands, our wrapper collapses.
