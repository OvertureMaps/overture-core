# overture_core

[![PyPI](https://img.shields.io/pypi/v/overture-core.svg)](https://pypi.org/project/overture-core/)
[![Python versions](https://img.shields.io/pypi/pyversions/overture-core.svg)](https://pypi.org/project/overture-core/)

Shared, framework-agnostic business logic — portable job classes built on [`overture-serverless`](../overture_serverless).

## Jobs

- **`stac.job.PublishStac`** — publishes the Overture STAC catalog. Two modes selected by the presence of the `release` param:
  - *single-release* (`release` + `source_path` + `scratch_bucket`): publish just that release, mirror scoped to `stac/{release}/`. Schema comes from the RC bundle's `metadata.json`.
  - *walk* (no `release`, requires `scratch_bucket`): rebuild the catalog for every release currently in the public bucket, mirror the whole `stac/` prefix with delete-orphans. Schema per release comes from the existing STAC (`schema:version` in each `catalog.json`); on cache miss the job self-heals by reading schema from the released RC bundle in `scratch_bucket` and raises if it can't recover.

## Future work

- **Bundle-owned schema accessor** — when `ReleaseCandidateBundle` becomes publishable, move `read_schema_version_from_rc_bundle` onto it as `resolved_schema_version()`; single-release mode's schema read shrinks to one line.
- **Upstream `build_release_catalog`** — file a PR against [OvertureMaps/stac](https://github.com/OvertureMaps/stac) exposing this as a public API (their CLI already does the exact call sequence). When it lands, our wrapper collapses.
