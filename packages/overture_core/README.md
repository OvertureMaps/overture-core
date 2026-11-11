# overture_core

[![PyPI](https://img.shields.io/pypi/v/overture-core?color=4051CC&labelColor=001A39)](https://pypi.org/project/overture-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-core?color=4051CC&labelColor=001A39)](https://pypi.org/project/overture-core/)
[![uv](https://img.shields.io/badge/managed%20with-uv-0EC1BD?labelColor=001A39)](https://docs.astral.sh/uv/)
[![License: MIT](https://img.shields.io/badge/license-MIT-05A5AF?labelColor=001A39)](LICENSE)

Shared, framework-agnostic business logic for the Overture data platform: helpers and portable job classes built on [`overture-serverless`](../overture_serverless).

## Install

```sh
uv add overture-core
uv add 'overture-core[stac]'  # with an extra
```

Heavier dependencies are optional extras, so consumers on constrained runtimes (for example MWAA's pinned `pyarrow`) stay installable. Quote the extra in shells like zsh that glob brackets. For local development, run `uv sync --extra dev`, then `uv run pytest`.

| Extra | Needed for |
| --- | --- |
| `stac` | `stac.job.PublishStac`, `stac.catalog.build_release_catalog` (`stac.latest_release_job.LatestRelease` works without it) |
| `azure` | `cloud.azure` |
| `maproulette` | `apis.maproulette.MapRouletteClient` |
| `compression` | `deflate64.enable_zipfile_deflate64` |
| `test-area` | `test_area` with a WKT area (a bbox needs no extra) |
| `sql-spark`, `sql-trino` | The SQL-engine test lanes only |

`cloud.aws.object.read_parquet_prefix` needs a `pyarrow` of your choosing on the path. Optional provider SDKs stay out of the base dependencies: add an extra and import the SDK lazily inside the helper.

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
| `spark_sql` | Spark SQL table-creation strings from caller-owned column definitions, rendered with SQLGlot without importing Spark or Arrow. |
| `strings_sql` | SQL expressions that make string values satisfy the Overture schema's string constraints (e.g. `StrippedString`), computed in-query instead of through a Python UDF. |
| `versioning` | Deriving a consistent dated version string for sources that don't publish explicit version metadata. |
| `iceberg` | Shared Iceberg + Sedona catalog configuration, reusable across platforms/engines. |
| [`cloud`](overture_core/cloud/README.md) | Cloud helpers: provider-agnostic code plus one subpackage per vendor (`aws`, `azure`, `databricks`). |
| `pypi` | Provider-agnostic PyPI package download/publish helpers, usable against any index. |
| `apis` | Clients for third-party HTTP APIs, one module per vendor (`apis.tomtom`, `apis.maproulette`, and `apis.github`, which mints GitHub App installation tokens from a PEM in Secrets Manager; see the [reference doc](https://github.com/OvertureMaps/omf-github-terraform/blob/main/docs/github-app-tokens.md)). Anything that's not a cloud platform but still an external service belongs here. |
| `urls` | Generic URL string utilities not tied to any specific service or cloud provider. |
| `file_io` | Stdlib-only file readers (CSV, TSV, JSON). Currently reads data files shipped inside a Python package, including from a zipped wheel. No Spark or pandas, so usable in UDF setup on workers. |
| `deflate64` | Letting the stdlib `zipfile` read Deflate64 (method 9) archives, such as Windows-built vendor zips, which it otherwise rejects. Patches `zipfile` process-wide. |
| `bbox` | Validating and safely rendering an optional bounding-box string param ("min_lon,min_lat,max_lon,max_lat", `''` = full planet), shared by any entry point that accepts one. |
| `test_area` | Validating an optional test-area string param (a bbox or a `POLYGON` / `MULTIPOLYGON` WKT, `''` = full planet) that scopes a dev or test run to a region, and deriving its envelope and polygon WKT, shared by any entry point that accepts one. |
| `stac` | STAC catalog reads/writes and the jobs that publish them (see [Jobs](#jobs)). |
| `data` | Describing a data location and its sync configuration, independent of the mechanism used to move it. |
| `docs` | Automating docs-repo updates for a release, via a GitHub App. |
| `artifacts` | Release artifact types (metadata, license, attribution) and the tree-search/S3 I/O to read and write them. |
| [`dataset`](overture_core/dataset/README.md) | Parsing, validating and rendering provider/resource dataset configs and the license policy, plus the `overture-datasets` CLI. |
| `bundle` | The pipeline data bundle model: an S3 directory of `data/` plus `metadata.json` and a `success` marker under Hive-style partitions, with its path layout and components, lifecycle and status, partition resolution and retention/cleanup; `bundle.registry` maps an S3 path to its bundle type. Storage goes through a pluggable `BundleStore` (S3 by default; an in-memory store for tests and offline tools). Orchestrator concerns (default buckets, namespacing, the metadata writer, catalogue table drops) are passed in by the caller. |

## Jobs

- **`stac.job.PublishStac`**: publishes the Overture STAC catalog. Two modes selected by the presence of the `release` param:
  - *single-release* (`release` + `source_path` + `scratch_bucket`): publish just that release, mirror scoped to `stac/{release}/`. Schema comes from the RC bundle's `metadata.json`.
  - *walk* (no `release`, requires `scratch_bucket`): rebuild the catalog for every release currently in the public bucket, mirror the whole `stac/` prefix with delete-orphans. Schema per release comes from the existing STAC (`schema:version` in each `catalog.json`); on cache miss the job self-heals by reading schema from the released RC bundle in `scratch_bucket` and raises if it can't recover.
- **`stac.latest_release_job.LatestRelease`**: reads the STAC root catalog (`root_href`, defaults to the prod root) and resolves the newest release ID. Read-only, HTTPS-only, no AWS credentials or S3 access needed. Stashes the result on `self.latest_release` for a caller that instantiates the job directly instead of going through `run()`.

## Future work

- **Bundle-owned schema accessor** — move `read_schema_version_from_rc_bundle` onto `bundle.bundle.ReleaseCandidateBundle` as `resolved_schema_version()`; single-release mode's schema read shrinks to one line.
- **Upstream `build_release_catalog`** — file a PR against [OvertureMaps/stac](https://github.com/OvertureMaps/stac) exposing this as a public API (their CLI already does the exact call sequence). When it lands, our wrapper collapses.
