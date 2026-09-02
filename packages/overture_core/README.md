# overture_core

[![PyPI](https://img.shields.io/pypi/v/overture-core.svg)](https://pypi.org/project/overture-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-core.svg)](https://pypi.org/project/overture-core/)

Shared, framework-agnostic business logic — portable job classes built on [`overture-serverless`](../overture_serverless).

`overture-stac` (and its `pyarrow>=16` floor via `stac-geoparquet`) is an optional extra. Install `overture-core[stac]` (quote it in shells like zsh that glob brackets: `pip install 'overture-core[stac]'`) if you need `stac.job.PublishStac` or `stac.catalog.build_release_catalog`. `stac.latest_release_job.LatestRelease` only reads the STAC root catalog over HTTPS via `pystac`, so it works without the extra. Plain `overture-core` covers the `cloud`/`iceberg`/`versioning`/`uuids`/`uuids_sql` modules too.

## Modules

Framework-agnostic helpers, each importable on its own without pulling in the job classes below.

| Module | What it's for |
| --- | --- |
| `uuids` | Pure Python UUID v3/v4/v5 generators, no Spark dependency; wrap them in a UDF yourself. Also includes the newer RFC 9562 v6/v7/v8 generators, unavailable before Python 3.14. |
| `uuids_sql` | SQL-string equivalents of the `uuids` generators for Spark or Trino, so a UUID column can be computed server-side instead of round-tripping through a Python UDF. |
| `versioning` | Parse a source's version from an ISO 8601 date or an HTTP header into a consistent dated string, for sources with no explicit version metadata. |
| `iceberg` | Shared enums, dataclasses, and Spark SQL extensions for Iceberg + Sedona catalog configuration across platforms. |
| `cloud.cloud` | Provider-agnostic helpers for building partition path segments. |
| `cloud.aws` | boto3-backed AWS integrations: account/region/role identity, S3 object I/O, and CodeArtifact auth/package access. The home for any AWS-specific helper. |
| `cloud.databricks` | Databricks integrations, taking a caller-supplied SDK client rather than constructing one, so this module never pulls in `databricks-sdk` at runtime. The home for any Databricks-specific helper. |
| `pypi` | Provider-agnostic PyPI package download/publish helpers, usable against any index (CodeArtifact, public PyPI, etc.). |
| `urls` | Generic URL string utilities not tied to any specific service or cloud provider. |
| `stac.catalog` | STAC catalog reads/writes backing the jobs below. |
| `data` | `DataLocation`/`DatasyncSpec` dataclasses describing a data location and its DataSync configuration. |
| `docs` | `update_docs_for_release()` opens a pull request against a docs repo for a new release, via a GitHub App. |
| `artifacts` | `MetadataArtifact`/`LicenseArtifact`/`AttributionArtifact` classes plus tree-search and S3 JSON/Markdown I/O helpers for release artifacts. |
| `dataset.dataset` | `Dataset` class parsing a provider/resource JSON config into collection/ingestion/matching sections. |
| `dataset.schema` | Pydantic schema validating dataset provider/resource JSON configs; also runnable as a script for CI validation. |

## Jobs

- **`stac.job.PublishStac`**: publishes the Overture STAC catalog. Two modes selected by the presence of the `release` param:
  - *single-release* (`release` + `source_path` + `scratch_bucket`): publish just that release, mirror scoped to `stac/{release}/`. Schema comes from the RC bundle's `metadata.json`.
  - *walk* (no `release`, requires `scratch_bucket`): rebuild the catalog for every release currently in the public bucket, mirror the whole `stac/` prefix with delete-orphans. Schema per release comes from the existing STAC (`schema:version` in each `catalog.json`); on cache miss the job self-heals by reading schema from the released RC bundle in `scratch_bucket` and raises if it can't recover.
- **`stac.latest_release_job.LatestRelease`**: reads the STAC root catalog (`root_href`, defaults to the prod root) and resolves the newest release ID. Read-only, HTTPS-only, no AWS credentials or S3 access needed. Stashes the result on `self.latest_release` for a caller that instantiates the job directly instead of going through `run()`.

## Future work

- **Bundle-owned schema accessor** — when `ReleaseCandidateBundle` becomes publishable, move `read_schema_version_from_rc_bundle` onto it as `resolved_schema_version()`; single-release mode's schema read shrinks to one line.
- **Upstream `build_release_catalog`** — file a PR against [OvertureMaps/stac](https://github.com/OvertureMaps/stac) exposing this as a public API (their CLI already does the exact call sequence). When it lands, our wrapper collapses.
