# overture_core

[![PyPI](https://img.shields.io/pypi/v/overture-core.svg)](https://pypi.org/project/overture-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-core.svg)](https://pypi.org/project/overture-core/)

Shared, framework-agnostic business logic — portable job classes built on [`overture-serverless`](../overture_serverless).

`overture-stac` (and its `pyarrow>=16` floor via `stac-geoparquet`) is an optional extra. Install `overture-core[stac]` (quote it in shells like zsh that glob brackets: `pip install 'overture-core[stac]'`) if you need `stac.job.PublishStac` or `stac.catalog.build_release_catalog`. `stac.latest_release_job.LatestRelease` only reads the STAC root catalog over HTTPS via `pystac`, so it works without the extra. Plain `overture-core` covers the `cloud`/`iceberg`/`versioning`/`uuids`/`uuids_sql` modules too.

## Modules

Framework-agnostic helpers, each importable on its own without pulling in the job classes below.

<!--
When adding or editing a row: describe the module's scope/boundary (what kind of
helper belongs here), not an inventory of its current classes/functions. This
table should stay accurate as the module grows without needing an edit every
time something is added to it. Put new code in the module whose scope already
covers it; only add a new row/module when nothing existing fits.
-->

| Module | Scope |
| --- | --- |
| `uuids` | Pure Python, Spark-free UUID generation for any UUID version, current or future RFC. |
| `uuids_sql` | SQL-string equivalents of `uuids`, for computing UUID columns server-side in Spark or Trino instead of round-tripping through a Python UDF. |
| `versioning` | Deriving a consistent dated version string for sources that don't publish explicit version metadata. |
| `iceberg` | Shared Iceberg + Sedona catalog configuration, reusable across platforms/engines. |
| `cloud.cloud` | Provider-agnostic cloud helpers that don't belong to one specific vendor. |
| `cloud.aws` | The home for any AWS-specific helper, built on boto3. |
| `cloud.databricks` | The home for any Databricks-specific helper. Prefers accepting a caller-supplied SDK client over constructing one, keeping `databricks-sdk` out of this package's runtime dependencies. |
| `pypi` | Provider-agnostic PyPI package download/publish helpers, usable against any index. |
| `urls` | Generic URL string utilities not tied to any specific service or cloud provider. |
| `stac.catalog` | STAC catalog reads/writes backing the jobs below. |
| `data` | Describing a data location and its sync configuration, independent of the mechanism used to move it. |
| `docs` | Automating docs-repo updates for a release, via a GitHub App. |
| `artifacts` | Release artifact types (metadata, license, attribution) and the tree-search/S3 I/O to read and write them. |
| `dataset.dataset` | Parsing a provider/resource JSON config into its collection/ingestion/matching sections. |
| `dataset.schema` | Validating provider/resource JSON configs, including as a standalone CI check. |

## Jobs

- **`stac.job.PublishStac`**: publishes the Overture STAC catalog. Two modes selected by the presence of the `release` param:
  - *single-release* (`release` + `source_path` + `scratch_bucket`): publish just that release, mirror scoped to `stac/{release}/`. Schema comes from the RC bundle's `metadata.json`.
  - *walk* (no `release`, requires `scratch_bucket`): rebuild the catalog for every release currently in the public bucket, mirror the whole `stac/` prefix with delete-orphans. Schema per release comes from the existing STAC (`schema:version` in each `catalog.json`); on cache miss the job self-heals by reading schema from the released RC bundle in `scratch_bucket` and raises if it can't recover.
- **`stac.latest_release_job.LatestRelease`**: reads the STAC root catalog (`root_href`, defaults to the prod root) and resolves the newest release ID. Read-only, HTTPS-only, no AWS credentials or S3 access needed. Stashes the result on `self.latest_release` for a caller that instantiates the job directly instead of going through `run()`.

## Future work

- **Bundle-owned schema accessor** — when `ReleaseCandidateBundle` becomes publishable, move `read_schema_version_from_rc_bundle` onto it as `resolved_schema_version()`; single-release mode's schema read shrinks to one line.
- **Upstream `build_release_catalog`** — file a PR against [OvertureMaps/stac](https://github.com/OvertureMaps/stac) exposing this as a public API (their CLI already does the exact call sequence). When it lands, our wrapper collapses.
