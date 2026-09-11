# Addresses Theme: Pipeline Mechanics (cairn vocabulary)

Terse companion to `pipeline-trace-addresses.md`. Mechanical facts only, tagged
with cairn operation vocabulary: drop, merge, split, revision, id change/rebind,
join, new column, column removed, aggregation, black box, write.

## Architecture finding

- Two parallel, disconnected implementations exist for this theme.
- **Legacy `omf` pipeline** (`omf/omf/addr/...`) — what actually ships the
  released `addresses` theme today. Sections 1-4 below.
- **New `overture_addresses` pipeline** (`overture_addresses/overture_addresses/...`)
  — collect + ingest only, half of a planned collect → ingest → match
  pipeline. Section 5 below. Kept in its own section; not merged with the
  legacy pipeline's facts anywhere below.

---

## 1. Release Publish (`airflow/dags/release_publish_dag.py`)

- black box: DataSync of `data/`, `changelog/`, `bridgefiles/`, `registry/`
- write: managed release bucket (AWS), Azure storage account, archive bucket
- write: PMTiles via boto3 multipart S3 copy (bypasses DataSync)
- black box: `PublishStac` (`overture_core.stac.job`) on Fargate
- black box: STAC CloudFront invalidation
- black box: 4 Glue crawlers started (`Overture`, `Overture Changelogs`,
  `Overture bridge files`, `Overture registry`)
- revision: `metadata.json` on the RC bundle gets `released` + `released_at`
  fields set (`ReleaseCandidateBundle.tag_as_released_from_uri`)
- no per-record transform of theme data

## 2. Theme Promote (`airflow/dags/theme_promote_dag.py`, `theme_promote.py`)

- (plumbing) `setup`: `validate_bucket_accessibility` x2 + `validate_input_data_path` — existence/permission checks, no data read (`theme_promote.py:175-191`)
- (plumbing) `discover_input_types`: lists `type=` partitions under the input bundle via S3 `list_objects_v2` (`theme_promote.py:89-103`)
- join: `ComputeInternalChangelogJob` full-outer-joins old vs new theme data on `(id, type)` (`compute_internal_changelog.py:83-176`)
- new column: per-column content hash (`sha2`, geometry via `ST_AsText`) on each side (`compute_internal_changelog.py:83-176`)
- new column: `change_type` = ADDED/REMOVED/DATA_CHANGED/UNCHANGED (`compute_internal_changelog.py:83-176`)
- new column: `version` = `old.version + 1` if hashes differ else `coalesce(old.version, 1)` (`compute_internal_changelog.py:83-176`)
- write: `changelog_df` keyed by `id`; no rows dropped here (`compute_internal_changelog.py:83-176`)
- aggregation: `ComputeChurnJob` groups the changelog by `type`, computes added/removed/data_changed/unchanged counts and percentages (`compute_churn.py:133-142`)
- write: churn stats to CSV/Markdown/Parquet, plus a telemetry write (`compute_churn.py:55-124`)
- drop (job-level, not record-level): `validate_churn_thresholds` reads `changelog_stats.csv`, compares each type's percentages against `THEME_THRESHOLDS` (addresses has its own wider `address` threshold set), fails the whole run if exceeded (`changelog.py:131-263`)
- aggregation: `ValidateDataJob` runs `compare_schemas` against the `overture-schema` registry entry for the type (ignoring `bbox`/`version`), then `evaluate_checks` (declared per-field checks from that registry), groups violations by `field:check` (`validate_data.py:141-246`)
- write: samples of violating rows to `output_path` when errors exist (`validate_data.py:219-230`)
- drop (job-level): `ValidateDataJob` fails the run if `error_rows > 0` (`validate_data.py:241-245`)
- join: `ProcessDataJob` left-joins the theme data to `changelog_df.select("id","version","bbox")` on `id` (`process_data.py:~133`)
- drop: rows whose `bbox` doesn't intersect the run's `bbox` param, when one is passed (`process_data.py` `_filter_by_bbox`)
- drop: rows with `id` in the hardcoded `overture_cdp.block_list.BLOCKED_IDS` (`process_data.py:126-131`)
- write: spatially repartitioned (KDB-tree over bbox centroids) GeoParquet with Hilbert-ordered row groups (`process_data.py` `write_spatial_parquet`)
- column removed: `ComputePublicChangelogJob` drops `version` before writing the public changelog (`compute_public_changelog.py:30`)
- write: public changelog spatially repartitioned, written partitioned by `change_type` (`compute_public_changelog.py:23-35`)
- aggregation, write: `generate_metrics` runs a Spark metrics-suite job for the theme against the latest published release baseline (`metrics.py:101+`) — real Spark logic, not traced deeper in this pass
- write: bridge file generation triggers `bridge_file_create_dag`, config-driven per type (`THEME_BRIDGE_FILE_TYPES`)
- black box: `pmtiles_task` submits an AWS Batch job running the public `ghcr.io/overturemaps/overture-tiles` Docker image; tiling logic itself is not in this repo (`pmtiles.py:25-90`)
- (plumbing) `validate_final_output`: checks S3 for expected output directories (existence check only, no data transform) (`validation.py:151-181`)
- (plumbing) `cleanup_hidden_files`: S3 cleanup of hidden files
- no id reassignment

## 3. Theme Addresses Stage (`airflow/dags/theme_addresses_stage_dag.py`)

### 3a. Stage 1 — `OvertureAddresses` (`omf/omf/addr/scripts/glue.py:24`, task `stage_1_no_ids`, `theme_addresses_stage_dag.py:91-105`)

- input: `storage_root` = collect output, pattern
  `s3://.+/collection/addresses/run=[^<>]+/`
- black box: `Planet` = union of ~38 per-country `AddressDataset`s; each
  `to_df` unioned via `union_all` (row concat, no dedup key)
- per source (`BasicAddressDataset.to_df`, `omf/omf/addr/base.py:50`):
  - new column set: output projected to `id, version, street, number, unit,
    postcode, postal_city, address_levels, country, geometry, sources`
    (source-specific column names aliased into this fixed schema)
  - revision: whitespace/control chars stripped from `street`, `number`,
    `unit`, `postcode`, `postal_city`
  - id change, mint: `id` = `expr("uuid()")` (`omf/omf/addr/overture_id.py`)
    — fresh random UUID per row, non-deterministic across runs
- revision: `OpenAddressDataset` nulls disallowed placeholder tokens
  (`"unknown"`, `"n/a"`, `"s/n"`, etc.) per field via `none_if_matches_any`
  (`omf/omf/addr/sources/open_addresses.py`)
- drop: countries `CA, CL, EE, FI, PT, SK` drop rows with no
  street/number/unit at all (`filter_empty_street_number_unit`)
- drop: Italy, Mexico — bbox filter removes stray geocoding-error rows
- drop: Australia — rows where `unit` contains `"carspace"`
- drop: Mexico — rows matching `"calle ninguno"` / `"sn"` placeholder
- revision: New Zealand — out-of-range longitudes wrapped
- revision: France — embedded newlines stripped from `number`
- US only (`omf/omf/addr/sources/us.py`, class `US`):
  - join, drop: NAD `left_anti` join on
    `ST_Contains(nyc_borough_boundaries.geometry, nad.geometry)` — drops NAD
    rows inside NYC boroughs
  - join, drop: NAD (post above) `left_anti` join on
    `ST_Contains(us_county_boundaries.geometry, nad.geometry)` — drops NAD
    rows inside ~150 TIGER counties/states (CA, MN, MS, WI, GA, CO, FL, MA, OR)
  - drop: the 2 Mississippi statewide OA files `left_anti` clipped by the
    same county boundaries (per-county MS OA wins, no double-count)
  - merge: clipped NAD + ~150 per-county/per-state OA datasets unioned into
    one US dataframe (row concat, no grouping key)
- new column: `geometry` → `ST_AsBinary(col("geometry"))` before write
- write: `{run}/stage_1/theme=addresses` (Parquet), `id` = random UUID,
  no cross-release stability yet

### 3b. Stage 2 — `AddressIDAssignment` (`omf/omf/addr/scripts/glue.py:42`, task `stage_2_id_assignment`, `theme_addresses_stage_dag.py:107-123`)

Raw-SQL block 1 — exact-duplicate collapse (`self.spark.sql(...)`):
- black box: raw SQL query, full text in source doc §3b
- drop: `WHERE geometry IS NOT NULL` — null-geometry rows removed (applied
  both in the `GROUP BY` subquery and outer query)
- black box, merge: `GROUP BY address_levels, country, postcode, street,
  number, unit, postal_city, geometry`, keeps row with `MIN(id)` — collapses
  byte-identical rows to one; tie-break = lexicographically smallest UUID;
  no record of which duplicate "won" beyond its `sources` array
- applied to both `candidate` and `baseline` (previous release) sets
  independently before the join below

Raw-SQL block 2 — baseline join for ID stability (`self.spark.sql(...)`):
- new column: `h3` = H3 level-10 cell index computed on both sides for the
  join key
- black box, join: `LEFT JOIN candidate_unique c ON baseline_unique b` where
  `c.h3 = b.h3` AND null-safe equality (`<=>`) on `address_levels, country,
  postcode, street, number, unit, postal_city` AND `ST_X(...) = ST_X(...)
  AND ST_Y(...) = ST_Y(...)` — exact-field match only, no fuzzy matching
- black box, id change, rebind: `result_id = COALESCE(b.id, c.id)` — matched
  candidate rows take the baseline's `id` (rebind); unmatched rows keep the
  stage-1 UUID
- new column: `result_id` (becomes the row's `id` downstream)
- write: `{run}/data/theme=addresses/type=address/` — feeds
  `ThemeAssembleBundle` (`ROOT_PATH = "theme_stage"`) → read by Theme Promote (§2)

## 4. Source Addresses Collect (`airflow/dags/source_addresses_collect_dag.py`, task group `data_import`, lines 90-179)

- black box: one ECS Fargate task per source runs `address_ingestion` CLI
  (`omf/omf/scripts/address_ingestion.py:main`) with `--sources <name>`
- black box: `SimpleRawSourceRepo.ingest_source(source)` → `source.ingest()`
  — raw byte fetch from external provider, no schema transform
- black box: OA — downloads `v2.openaddresses.io/batch-prod/collection-global.zip`
  (requester-pays), extracts per-country/county GeoJSON members
- black box: NAD — downloads ZIP from `data.transportation.gov`, shells out
  to `7z`, selects largest `NAD_r*.txt` file
- black box: other sources (`be, br, cz, isl, nl, no, si`) — per-source
  `RawSource`/`HttpRawSource` fetch-and-store, not individually traced
- write: `s3://<bucket>/.../collection/addresses/run=<ts>/` per source —
  this is the `storage_root` Stage 1 reads
- no theme-schema transform in this DAG

---

## 5. Parallel new pipeline: collect → ingest (`overture_addresses`)

Not wired to any release. Kept fully separate from §1-4 above.

### 5a. Dataset Addresses Collect (`airflow/dags/dataset_addresses_collect_dag.py`, `overture_addresses/overture_addresses/collect/job.py`, `class CollectionJob`)

- black box: serverless Python (no Spark, no ECS)
- black box: strategy = `s3_copy` (OA, server-side S3→S3 copy, 16 parallel
  workers) | `http_download` (stream URL → S3) | `http_signed_api` (resolve
  short-lived signed URL, GURS, then stream)
- black box: per-resource version detection (OA batch job ID, HTTP
  `Last-Modified`, Socrata `rowsUpdatedAt`, signed-API TTL, or run date)
- write: `datasets/provider=<p>/resource=<r>/version=<v>/run=<run>/data/`
- drop (scheduling, not data): `SourceRawBundle.needs_processing` /
  `needs_processing_batch` skip resources whose current upstream version was
  already collected, unless `force_collect=True`
- no row-level transform — byte-for-byte replication; archive extraction
  deferred to ingest

### 5b. Dataset Addresses Ingest (`airflow/dags/dataset_addresses_ingest_dag.py`, `overture_addresses/overture_addresses/ingest/job.py`, `class BatchIngestJob`)

- black box: resolvers `resolve_oa_per_source_params`, `resolve_non_oa_params`
  (`airflow/dags/addresses/utils.py`) diff S3 listings, decide which raw
  bundles need (re)ingestion
- black box: one Spark session, loops per resolved resource, dispatches to
  provider normalizer: `OANormalizer`, `NadNormalizer`,
  `StadfangaskraNormalizer`, `PdokNormalizer`, `GursNormalizer`,
  `CuzkNormalizer` (`_NORMALIZERS` dict)
- new column: `id` = `F.lit(None).cast("string")` — every normalizer sets
  `id` to null at this stage (`ADDRESS_SCHEMA`,
  `overture_addresses/overture_addresses/ingest/schema.py`)
- new column: `provider`, `resource`, `version` provenance fields added to
  `sources` struct (null in legacy pipeline)
- revision: disallowed-token nulling, whitespace stripping (ported from
  legacy `AddressNormalizer` base)
- new column: `address_levels` array constructed (same construction as
  legacy Stage 1)

`OANormalizer` (`ingest/providers/oa.py`):
- drop: `geometry IS NOT NULL` filter
- drop: rows where both `street` and `number` are null
  (`street.isNotNull() | number.isNotNull()` required)
- revision: disallowed-token nulling
- revision: out-of-range longitude wrap (NZ Chatham Islands)
- drop: `unit_excludes` JSON-config editorial filters per source, e.g. AU
  `"carspace"` — `~lower(coalesce(unit,"")).contains(token)` filter per token
- join, drop (MS only): `_apply_us_county_clip` — `left_anti` against TIGER
  counties so per-county OA takes precedence

`NadNormalizer` (`ingest/providers/nad.py`):
- black box: extracts `NAD_r*.txt` from `source.zip`, maps columns
- join, drop: `left_anti` join on
  `ST_Contains(bnd.geometry, nad._geom)` against NYC borough boundaries
- join, drop: `left_anti` join against TIGER counties/states, filter lists
  from `airflow/dags/addresses/us_clip_regions.py`
  (`COUNTY_NS_FILTERS`, `STATE_FP_FILTERS`, `CLIP_BY_COUNTY_OA_SOURCES`),
  ported from legacy `omf/omf/addr/sources/us.py`

- write: `s3://<feeds bucket>/.../type=address/` per resource — not read by
  any other DAG in this repo; terminates pending the match DAG

### Supporting files in `airflow/dags/addresses/` — no row-level data ops

- `collect.py`: `fetch_all_oa_jobs` — HTTP call to OA batch API, resolves job
  IDs. No transform.
- `loaders.py`: builds config objects from `configs/datasets/` JSON, dispatch
  by provider label. No rows touched.
- `resource_configs.py`: config dataclasses, fetch-strategy metadata only.
- `us_clip_regions.py`: pure data — filter lists (see `NadNormalizer` above).
- `utils.py`: scheduling/idempotency resolvers + `finalize_batch_bundles`
  (bundle metadata/success markers). No row-level transform.

### 5c. Match — Planned, not built

- `theme_addresses_match_dag`, listed "Planned" in
  `airflow/dags/addresses/README.md`, description "Deduplication and GERS ID
  assignment." Does not exist in this repository.

---

## Summary table

| Stage | Operations | ID impact |
|---|---|---|
| 1. Release Publish | write, black box, revision (metadata.json marker) | none |
| 2. Theme Promote | join, new column, drop, aggregation, write, column removed, black box (pmtiles only) | none |
| 3a. Stage 1 `OvertureAddresses` | id change (mint), new column, revision, drop, join, merge, write | mint |
| 3b. Stage 2 `AddressIDAssignment` | black box, drop, merge, join, id change/rebind, new column, write | merge + rebind |
| 4. Source Addresses Collect (ECS) | black box, write | none |
| 5a. Dataset Addresses Collect | black box, write, drop (scheduling only) | none |
| 5b. Dataset Addresses Ingest | black box, drop, revision, join, new column, write | none (id nulled, mint deferred) |
| 5c. Match (Planned) | not built — no observable operations | mint (planned) |
