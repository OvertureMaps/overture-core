# Buildings Pipeline Mechanics (cairn operation trace)

Terse companion to `pipeline-trace-buildings.md`. Mechanical facts only, tagged
with cairn operation vocabulary. Same stage order/headings as the source doc
(forward, source → release).

---

## 0. Shared stages

### 0.1 `release_publish_dag.py`
- write: DataSync copy of `data/`, `changelog/`, `bridgefiles/`, `registry/` to AWS release bucket, Azure blob container, archive bucket (`release_publish_dag.py:75-211`)
- write: boto3 multipart copy of PMTiles to "extras" bucket (`:327-332`)
- black box: `PublishStac` (`overture_core.stac.job`) via Fargate task group (`:334-351`)
- (no row-level transform) CloudFront invalidation, 4 Glue crawler runs, tag bundle as `released` (`:355-380`)

### 0.2 `theme_promote_dag.py` + `theme_promote.py`
- (plumbing) `setup`: `validate_bucket_accessibility` x2 + `validate_input_data_path` — existence/permission checks, no data read (`theme_promote.py:175-191`)
- (plumbing) `discover_input_types`: lists `type=` partitions under the input bundle via S3 `list_objects_v2` (`theme_promote.py:89-103`)
- join: `ComputeInternalChangelogJob` full-outer-joins old vs new theme data on `(id, type)` (`compute_internal_changelog.py:83-176`)
- new column: per-column content hash (`sha2`, geometry via `ST_AsText`) on each side (`compute_internal_changelog.py:83-176`)
- new column: `change_type` = ADDED/REMOVED/DATA_CHANGED/UNCHANGED (`compute_internal_changelog.py:83-176`)
- new column: `version` = `old.version + 1` if hashes differ else `coalesce(old.version, 1)` (`compute_internal_changelog.py:83-176`)
- write: `changelog_df` keyed by `id`; no rows dropped here (`compute_internal_changelog.py:83-176`)
- aggregation: `ComputeChurnJob` groups the changelog by `type`, computes added/removed/data_changed/unchanged counts and percentages (`compute_churn.py:133-142`)
- write: churn stats to CSV/Markdown/Parquet, plus a telemetry write (`compute_churn.py:55-124`)
- drop (job-level, not record-level): `validate_churn_thresholds` reads `changelog_stats.csv`, compares each type's percentages against `THEME_THRESHOLDS`, fails the whole run if any threshold is exceeded (`changelog.py:131-263`)
- aggregation: `ValidateDataJob` runs `compare_schemas` against the `overture-schema` registry entry for the type, then `evaluate_checks` (declared per-field checks from that registry), groups violations by `field:check` (`validate_data.py:141-246`)
- write: samples of violating rows to `output_path` when errors exist (`validate_data.py:219-230`)
- drop (job-level): `ValidateDataJob` fails the run if `error_rows > 0` (`validate_data.py:241-245`)
- join: `ProcessDataJob` left-joins the theme data to `changelog_df.select("id","version","bbox")` on `id` (`process_data.py:~133`)
- drop: rows whose `bbox` doesn't intersect the run's `bbox` param, when one is passed (`process_data.py` `_filter_by_bbox`)
- drop: rows with `id` in the hardcoded `overture_cdp.block_list.BLOCKED_IDS` (`process_data.py:126-131`)
- write: spatially repartitioned (KDB-tree over bbox centroids) GeoParquet with Hilbert-ordered row groups (`process_data.py` `write_spatial_parquet`)
- column removed: `ComputePublicChangelogJob` drops `version` before writing the public changelog (`compute_public_changelog.py:30`)
- write: public changelog spatially repartitioned, written partitioned by `change_type` (`compute_public_changelog.py:23-35`)
- aggregation, write: `generate_metrics` runs a Spark metrics-suite job for the theme against the latest published release baseline (`metrics.py:101+`) — real Spark logic, not traced deeper in this pass
- (see this theme's own bridge-file section) bridge file generation triggers `bridge_file_create_dag`, reading either corpus or a direct path depending on the theme
- black box: `pmtiles_task` submits an AWS Batch job running the public `ghcr.io/overturemaps/overture-tiles` Docker image; tiling logic itself is not in this repo (`pmtiles.py:25-90`)
- (plumbing) `validate_final_output`: checks S3 for expected output directories (existence check only, no data transform) (`validation.py:151-181`)
- (plumbing) `cleanup_hidden_files`: S3 cleanup of hidden files

---

## 1. Assemble / conflation stage — `theme_buildings_assemble_dag.py`

### 1.1 Setup: corpus fetch
- black box: `fetch_corpus` task triggers `corpus_data_export_dag` (`overture_corpus` Scala/Iceberg job) for `ThemeName=buildings, TableName=building, BranchName=main` (`theme_buildings_assemble_dag.py:244-258`)
- write: combined parquet export to `corpus/theme=buildings/type=building`
- black box: `TileWater` reads `theme=base/type=water` from latest full release, `ST_SubDivide` chops polygons to ≤100 vertices (`tile_water.py:48-63`)

### 1.2 Spatial merge — `BuildingSpatialMerge` (`theme_buildings_assemble_dag.py:266-286`, `building_spatial_merge.py`)
- drop: filter `source = '<source_name>'` per iteration over `SOURCE_NAMES` priority list (`theme_buildings_assemble_dag.py:34-43`)
- drop: filter `corpus_state IS NULL OR corpus_state = 'active'` (`building_spatial_merge.py:26-50`)
- merge: per-source dedup, group key `id`, keep `rank == 1` from `row_number() OVER (PARTITION BY id ORDER BY sources[0].record_id ASC)` (`building_spatial_merge.py:26-50`)
- drop: candidate row dropped when `ST_Intersects(i.geom, v_all.geom)` AND `(v_all.deleted_at IS NULL OR i.sources[0].update_time IS NULL OR i.sources[0].update_time <= v_all.deleted_at)` — i.e. lower-priority building intersecting an already-placed one, unless the placed one is deleted and candidate is newer (`building_spatial_merge.py:72-104`)
- write: suppressed rows written as `filter_type=conflate` disposition records to `s3_output_filter_path` (not the violation-store table)
- id impact: none — ids already assigned by corpus/matcher (§2.4)

### 1.3 Tag merge — `BuildingTagMerge` (`theme_buildings_assemble_dag.py:293-313`, `building_tag_merge.py`)
- black box/aggregation: "outlier" computed as relative distance from min/max of other 3 height candidates > 50% (`building_tag_merge.py:117-160`)
- revision: `height` = `CASE WHEN input.height IS NOT NULL THEN input.height WHEN esri.height IS NOT NULL AND NOT esri_is_outlier THEN esri.height WHEN lidar.height IS NOT NULL AND NOT lidar_is_outlier THEN lidar.height WHEN ms.height IS NOT NULL AND NOT ms_is_outlier THEN ms.height ELSE NULL END` (`building_tag_merge.py:162-175`)
- revision: `sources` array gets contributing source's `sources` struct appended when a non-input height wins (`building_tag_merge.py:227-236`)

### 1.4 Post-merge filters — `BuildingIntersect` (×2) and `BuildingPostMergeFilter` (`theme_buildings_assemble_dag.py:320-429`)
- aggregation: `BuildingIntersect` vs current release `theme=transportation/type=segment` flags intersection → violation `building_transportation_intersection` (`building_intersect.py`)
- aggregation: `BuildingIntersect` vs `TileWater` output flags intersection → violation `building_water_intersection`; `severity = 1 WHEN building.source IN ('zenodo','google','google_high','google_low','microsoft') ELSE -1` (`building_intersect.py:50-57`)
- black box: `BuildingPostMergeFilter` raw Sedona SQL computes `building_invalid_area` (spherical area null or ≤ 0) (`building_post_merge_filter.py:68-140`)
- split: exploded point-triplet `ATAN2` calc over polygon vertices feeds `building_too_many_small_angles` (>5 corners with interior angle <30° or >330°) (`_find_buildings_with_small_angles`, `building_post_merge_filter.py:68-140`)
- write: `MERGE INTO` Iceberg entity-violations table, keyed on `(id, violation_name, version, counterpart, dataset)`, upsert-only (never deletes) — for `building_transportation_intersection`, `building_water_intersection`, `building_invalid_area`, `building_too_many_small_angles` (`entity_violations_update.py:29-63`, `theme_buildings_assemble_dag.py:382-399`)
- merge: `BuildingFilter` dedup, group key `id`, keep lowest `record_id` (`building_filter.py:16-29`)
- drop: anti-join vs Iceberg violations table for the 4 post-merge violation names where `severity > 0`, matched on `(id, dataset, version)` (`building_filter.py:45-62`)
- drop: `WHERE deleted_at IS NULL` (`building_filter.py:64-67`)

### 1.5 Stage — `BuildingPartsStage` and `BuildingStage` (`theme_buildings_assemble_dag.py:435-483`)
- drop: filter `sources[0].dataset = 'OpenStreetMap'` before join (`building_parts_stage.py:25`)
- join: filtered OSM buildings ⨝ `building_parts_bundle` (OSM building-parts feed), on part's OSM `building_id` tag = building's own OSM `record_id` split on `@` — inner join (`building_parts_stage.py:26-56`)
- drop: non-Polygon/MultiPolygon part geometries (`building_parts_stage.py:38-45`)
- drop: parts whose parent building was filtered out of release earlier (no match in inner join)
- join: filtered buildings ⨝ distinct set of `building_id`s that received a part (`building_stage.py:39-72`)
- new column: `has_parts` set from that join (`building_stage.py:39-72`)
- revision: `names.primary` set from `names.common`/`names.rules` when no primary name already set (`_promote_common_to_primary_name`, `building_stage.py:88-115`, `building_parts_stage.py:91-118`)
- write: `BuildingStage` output → `data/theme=buildings/type=building`; `BuildingPartsStage` output → `data/theme=buildings/type=building_part` (`finalize_bundle`, `theme_buildings_assemble_dag.py:481`)

---

## 2. Ingest stage — `theme_buildings_ingest_dag.py`

### 2.1 Feed ingest — `BuildingIngest` (`theme_buildings_ingest_dag.py:277-305`, `building_ingest.py`)
- black box: `building_sources.initialize_schema(...)` — per-source `if source == ...` schema translator (`building_sources.py:17-332`)
- revision: Esri `record_id` = substring of `FeatureUID` (`:53`)
- new column: Esri `license` hardcoded to `"Creative Commons by Attribution (CC BY 4.0) with OpenStreetMap waivers"` (`:50-52`)
- id change: Google `record_id` = `full_plus_code` tag
- merge: Google dedup, group key `record_id`, keep first row via `row_number() OVER (PARTITION BY record_id ORDER BY monotonically_increasing_id())` (`:151-161`)
- split: one raw `open_buildings` feed fissioned into `google_high`/`google_low` sources by filter `tags['high_precision'] = 'true'` vs not (`theme_buildings_ingest_dag.py:199-212`)
- revision: Microsoft `update_time` falls back to sentinel `"2000-01-01T00:00:00.000Z"` when `imagerycapturedate` missing or later than snapshot version (`:206-223`)
- id change: Vancouver `record_id` = substring of `esri_id`
- new column: Vancouver `roof:height` = `bldgheight - eaveheight`, computed at ingest not read from source (`:317-320`)
- drop: bbox filter applied in `execute_job`
- id change (mint): `id` = `uuid()` random value minted per record (`building_ingest.py:41`) — pre-corpus placeholder, replaced/kept by matcher (§2.4)
- new column: `height`, `subtype`, `class`, `roof_*` etc. derived via `building_common.py` helpers (`building_ingest.py:42-59`)
- write: GeoParquet per source
- note: no `osm`/`usgs` branch in `initialize_schema` — those sources bypass this stage entirely

### 2.2 Pre-match filter — `BuildingPreMatchFilter` (`theme_buildings_ingest_dag.py:307-328`, `building_pre_match_filter.py:65-165`)
- aggregation: writes (not yet drops) 5 violation types to `violations/` path
- `building_tiny`: `area < MIN_ML_BUILDING_AREA_SQ_METERS`, filter `~sources[0].dataset.isin(NON_ML_SOURCES)` (ML sources only)
- `building_large`: `area > MAX_ML_BUILDING_AREA_SQ_METERS`, ML sources only
- `building_huge`: `area > MAX_BUILDING_AREA_SQ_METERS`, all sources
- `building_invalid_geometry`: `NOT ST_ISVALID` or empty after `ST_MakeValid`
- aggregation: `building_duplicate_record_id` via `count() OVER (PARTITION BY record_id)` > 1 in batch
- limits sourced from `src/public/building/limits.py`

### 2.3 Violation update
- write: `MERGE INTO` Iceberg entity-violations table (same mechanism as §1.4) for the 5 pre-match violation types (`EntityViolationsUpdate`)

### 2.4 Match against corpus — `BuildingMatcher` (`theme_buildings_ingest_dag.py:387-413`, `building_matcher.py`)
- black box: per-run corpus snapshot export (`fetch_corpus`, `theme_buildings_ingest_dag.py:491-503`, `overture_corpus` Scala job)
- drop: anti-join feed vs Iceberg entity-violations table filtered to `severity > 0` pre-match violations, matched on `(id, dataset, update_time)` (`building_matcher.py:82-101`)
- join: `v_corpus ⨝ v_feed WHERE ST_Intersects(v_corpus.geom, v_feed.geom)` (`building_matcher.py:110-119`)
- aggregation: `iou = ST_Area(ST_Intersection(v_feed.geom, v_corpus.geom)) / ST_Area(ST_Union(v_feed.geom, v_corpus.geom))` per pair (`building_matcher.py:110-119`)
- drop: filter `iou > 0.5` (`building_matcher.py:142-151`)
- merge: group key `id` (feed), keep top row from `row_number() OVER (PARTITION BY id ORDER BY iou DESC)` (`building_matcher.py:142-151`)
- merge/drop: group key `release_id` (corpus), keep top row from `row_number() OVER (PARTITION BY release_id ORDER BY deleted_at IS NULL DESC, deleted_at DESC NULLS LAST, iou DESC, record_id ASC)`; all other rows written as `duplicate_match` filter records and excluded from mapping (`building_matcher.py:157-179`)
- join: `v_feed LEFT JOIN v_mapping ON feed.id = mapping.id` (`building_matcher.py:197-228`)
- id change / rebind: `id = COALESCE(mapping.release_id, feed.id)` — matched building's id rebinds to corpus `release_id`; unmatched keeps ingest-minted id (`building_matcher.py:197-228`)
- new column: `iou = COALESCE(mapping.iou, 0)` (`building_matcher.py:197-228`)
- revision: `geometry = ST_AsBinary(ST_ForcePolygonCCW(ST_Force_2D(feed.geom)))` (`building_matcher.py:197-228`)
- merge: final dedup before write, group key `id`, active beats deleted then `record_id` tiebreak (`building_matcher.py:231-244`)
- write: `matched/source=<source>/`

### 2.5 Corpus update — black box
- black box: `corpus_register_theme_dag` → `corpus_data_load_dag` (`overture_corpus` Scala DataLoad job) writes matched rows into shared Iceberg corpus table, `IdField=id`, tagged `Source=<source>` (`theme_buildings_ingest_dag.py:415-485`)

### 2.6 OSM ingest branch inside this DAG
- (copy) `OsmBuildingExtract` reads `theme=buildings/type=building` out of `overture_rc` bundle, rewrites as standalone bundle (`osm_building_extract.py:1-6, 20-36`)
- drop: optional bbox filter applied during that copy
- then same pre-match-filter → violation-update → match-corpus → update-corpus chain as §2.2-2.5

### 2.7 Signal ingest — USGS LiDAR (`theme_buildings_ingest_dag.py:352-373`, `signal_ingest.py`)
- (copy) `spark.read.parquet` → `spark.write.parquet`, no filter, no schema change
- note: never enters corpus/matcher; consumed directly by `BuildingTagMerge` (§1.3) for height only

---

## 3. Raw source stage

### 3.1 Vendor building footprints (esri, google, microsoft, ign_spain, zenodo, vancouver)
- read boundary: `BuildingIngest`'s `spark.read.format("parquet"/"geoparquet")` against `SourceRawBundle(provider=..., resource=...)` path `datasets/provider={provider}/resource={resource}/version={version}/run={run}` (`src/public/overture_airflow/bundle.py:2235-2275`)
- no producing DAG for these bundles exists in this repo

### 3.2 USGS LiDAR
- read boundary: `SignalIngest` reads `SourceRawBundle(provider="usgs", resource="lidar")` directly
- no producing DAG for this bundle exists in this repo

### 3.3 OSM — the actual PBF/history read
- black box: `dataset_osm_history_dag.py`/`dataset_osm_history_reset_dag.py` convert `SourceRawBundle(provider="osm", resource="planet_history")` into Iceberg full-history table via `get_osm_history_table()` (`src/iceberg.py`); internals not traced (`dataset_osm_history_reset_dag.py:129-134`)
- (layering) `dataset_osm_geometry_dag.py` produces daily OSC-based geometry snapshots (`SourceRawBundle(provider="osm", resource="geometry_daily")`), layered onto history table
- black box: `BuildingFromOsm` raw SQL block classifies every `way`/`relation` row into `active` (currently tagged as building), `retagged` (now `building=no`/lifecycle-deleted but had a prior real building tag), or `deleted` (frozen last-visible row of an invisible-latest entity) (`building_from_osm.py:83-142`)
- merge: unions active/retagged/deleted classifications into one dataset with a `deleted_at` column (`building_from_osm.py:83-142`)
- id change (mint): `id = uuid()` random placeholder per row (`building_from_osm.py:146`), later replaced by matcher (§2.4) if matched
- black box: tag→attribute translation delegated to SQL-fragment builders in `building_sql.py`, e.g. `osm_tags_to_subtype()` maps ~40 `building=*` tag values into ~10 subtype buckets via `CASE WHEN ... IN (...)` (`building_sql.py:349-401`)
- new column: `subtype`, `class`, `height`, roof attributes, `names` derived from OSM tags
- join: `BuildingPartsFromOsm` — relation members tagged `part`/`outline`, plus free-standing `building:part` ways spatially covered by a building footprint via `ST_COVERS` with buffer (`building_parts_from_osm.py:156-159, 192-195`)
- drop: excludes buildings with anything other than exactly one outline, and parts sets where the only part is geometrically identical to the building itself (`building_parts_from_osm.py:220-239`)
- id change (mint): `BuildingPartFromOsm` derives each part's `id` via deterministic UUIDv3 (namespace `BUILDING_PART`, seeded from OSM type+id, `uuid_v3_sql(...)`) — stable/re-derivable across runs, unlike building ids (`building_part_from_osm.py:52-56`)
- write: output (`theme=buildings/type=building`, `type=building_part`) becomes the `osm_in_overture` bundle, then the weekly `overture_rc` bundle consumed at §2.6 and §1.5

---

## Stage / operation / id-impact table

| Stage | Operations | Id impact |
|---|---|---|
| 0.1 `release_publish_dag.py` | write | none |
| 0.2 `theme_promote_dag.py` | join, new column, drop, aggregation, write, column removed, black box (pmtiles only) | none |
| 1.1 Setup: corpus fetch | black box, write | none |
| 1.2 Spatial merge (`BuildingSpatialMerge`) | drop, merge, write | merge |
| 1.3 Tag merge (`BuildingTagMerge`) | black box, aggregation, revision | none |
| 1.4 Post-merge filters (`BuildingIntersect` ×2, `BuildingPostMergeFilter`, `BuildingFilter`) | aggregation, black box, split, write, merge, drop | merge, drop |
| 1.5 Stage (`BuildingPartsStage`, `BuildingStage`) | drop, join, new column, revision, write | none |
| 2.1 Feed ingest (`BuildingIngest`) | black box, revision, id change, merge, split, drop, new column, write | mint |
| 2.2 Pre-match filter (`BuildingPreMatchFilter`) | aggregation, write | none |
| 2.3 Violation update | write | none |
| 2.4 Match against corpus (`BuildingMatcher`) | black box, drop, join, aggregation, merge, id change, new column, revision, write | rebind |
| 2.5 Corpus update | black box, write | none |
| 2.6 OSM ingest branch | drop, write | none |
| 2.7 Signal ingest (USGS LiDAR) | (copy, no tagged ops) | none |
| 3.1 Vendor raw source | (read boundary, no tagged ops) | none |
| 3.2 USGS LiDAR raw | (read boundary, no tagged ops) | none |
| 3.3 OSM PBF/history (`BuildingFromOsm`, `BuildingPartsFromOsm`, `BuildingPartFromOsm`) | black box, merge, id change, join, drop, new column, write | mint |
