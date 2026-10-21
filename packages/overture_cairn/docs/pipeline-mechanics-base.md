# Base Theme Pipeline Mechanics

Terse companion to `pipeline-trace-base.md`. Mechanical facts only, tagged
with cairn's operation vocabulary: **drop, merge, split, revision, id
change/rebind, join, new column, column removed, aggregation, black box,
write**. No rationale, no background. Stage order/names match the source
trace (forward order here: raw source → release, i.e. reverse of the
trace's backward-to-forward narration).

**Scope note**: the source trace documents that `overture_base/overture_base/base_land.py`
and `base_common.py` import a throwaway prototype, `_cairn_stub.py`, whose
`track`/`drop`/`keep_best`/`rebind` wrappers exist only for early
`overture-cairn` API mockup purposes and whose `print()` calls are not real
provenance capture. Nothing below is tagged as coming from real cairn. The
bullets below describe only the actual Spark logic those wrappers execute
(a `.filter()`, a window "keep one row per group", an id comparison) —
i.e. `dedup()` in `base_common.py` really does call `keep_best()` for
production dedup of `base_land.py`/`base_water.py` output.

---

## 1. Release Publish (shared across all themes)

**File**: `airflow/dags/release_publish_dag.py`

- write: `data/`, `changelog/`, `bridgefiles/`, `registry/` DataSynced to AWS release bucket, Azure blob container, archive bucket (3 destinations, byte-identical)
- write: PMTiles copied via boto3 multipart copy (separate path from DataSync)
- black box: `PublishStac` (`overture_core.stac.job`) runs on Fargate; CloudFront invalidation follows
- black box: 4 Glue crawlers started in Distribution account
- new column: `released`, `released_at` written into RC bundle `metadata.json` (`ReleaseCandidateBundle.tag_as_released_from_uri`, lines 378-380)
- no per-feature transform at this stage

## 2. Theme Promote (shared across all themes)

**File**: `airflow/dags/theme_promote_dag.py`, `airflow/dags/src/public/overture_airflow/theme_promote.py`

Task order (`theme_promote.py:336-363`): `validate_data` → `compute_internal_changelog` → `validate_churn` → `process_data` → `compute_public_changelog` → `generate_metrics`, with `pmtiles_task`/bridge-file gen branching off `process_data`, then `validate_final`.

### 2a. `compute_internal_changelog` — version assignment

**File**: `overture_cdp/overture_cdp/compute_internal_changelog.py`, `ComputeInternalChangelogJob.get_changelog_df` (lines 83-176)

- join: `old_df` full-outer-joined to `new_df` on `(id, type)`
- new column: hash of every non-excluded column per side (geometry via `ST_AsText` → `sha2`)
- new column: `change_type` = `ADDED` (old.id null) / `REMOVED` (new.id null) / `DATA_CHANGED` (hash array size > 0) / `UNCHANGED` (otherwise)
- new column: `version` = `old.version + 1` if both sides present and hashes differ, else `coalesce(old.version, 1)`
- output is a side-table `changelog_df` keyed by `id`; no rows dropped here
- upstream of this stage (all of §3-§11 below), `version` is emitted as placeholder `0`

### 2b. `process_data` — join, bbox filter, block-list filter, repartition

**File**: `overture_cdp/overture_cdp/process_data.py`, `ProcessDataJob` (lines 842-995)

- join: `df` left-joined to `changelog_df.select("id","version","bbox")` on `id` (line 133)
- drop: rows whose `bbox` doesn't intersect the supplied `bbox` param, when one is passed (`_filter_by_bbox`)
- drop: rows with `id` in hardcoded `overture_cdp.block_list.BLOCKED_IDS` (`_filter_block_list`, lines 126-131)
- write: spatially repartitioned (KDB-tree over bbox centroids), rewritten as GeoParquet with Hilbert-curve-ordered row groups (`write_spatial_parquet`) — file layout only, not content
- `validate_data` (`overture_cdp/overture_cdp/validate_data.py`): schema-validation gate, no row drop/alter

## 3. Theme Base Stage (base-specific orchestrator)

**File**: `airflow/dags/theme_base_stage_dag.py`

- resolves 4 input bundles: `overture_rc_bundle` (osm), `coastline_bundle` (osm coastlines), `land_cover_bundle` (esa), `bathymetry_bundle` (ncei)
- fans out to 6 parallel Spark jobs, each writing directly into `type=<subtype>` partitions of one shared `ThemeAssembleBundle`:

| sub-theme | job class | reads |
|---|---|---|
| infrastructure | `base_infrastructure.BaseInfrastructure` | `overture_rc_bundle` |
| land_use | `base_land_use.BaseLandUse` | `overture_rc_bundle` |
| land | `base_land.BaseLand` | `overture_rc_bundle` + `coastline_bundle` |
| water | `base_water.BaseWater` | `overture_rc_bundle` + `coastline_bundle` |
| land_cover | `base_land_cover.BaseLandCover` | `land_cover_bundle` |
| bathymetry | `base_bathymetry.BaseBathymetry` | `bathymetry_bundle` |

- write: all 6 converge at `finalize_bundle`, stamping output `ThemeAssembleBundle` (input to §2)

## 4. Base sub-theme jobs — identity, filtering, dedup

**Package**: `overture_base/overture_base/base_land.py`, `base_water.py`, `base_land_use.py`, `base_infrastructure.py`, `base_land_cover.py`, `base_bathymetry.py`, `base_common.py`

Shared shape across `land`, `water`, `land_use`, `infrastructure`: read `overture_rc_bundle` at `theme=base/type=<subtype>` → drop bad geometry → promote common→primary name → re-mint id if invalid → column select → dedup on id.

### `promote_common_to_primary_name` (`base_common.py:27-47`)

- revision: `names.primary` set from `map_values(names.common)[0]` when `names.primary` is null and `names.common` is not null; else from `names.rules[0]["value"]` when `names.rules` is not null; else left null

### Identity re-minting — `base_land.py:109-132` (representative of all four OSM-derived jobs)

- new column: `_new_id` = `id` if it matches UUIDv3 regex `^[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, else `generate_uuid3("LAND", subtype || class || SPLIT(sources[0].record_id,'@')[0])`
- id change/rebind: `id` column dropped, `_new_id` renamed to `id` (no-op in steady state — only fires on malformed/legacy `id`)

### Dedup — `base_common.py:97-103`

- merge: `keep_best(df, per="id", by=sources[0].update_time desc)` — partition by `id`, order by `sources[0].update_time` desc, keep top row per partition, drop the rest
- runs in all 6 base jobs before final write

### `base_land.py` — coastline union (lines 20-69)

- id change/rebind (mint): each OSM-coastline "land" polygon gets `id = generate_uuid3("LAND", 'landland' || ST_AsText(geometry))`
- new column: synthetic `sources` entry via `coastline_sources()` (`base_common.py:67-94`), `provider=osm, resource=coastlines`
- join: `combine_osm_and_coastline_land` (lines 62-69) is a plain `.union()` of OSM-derived and coastline-derived land rows, unkeyed — collisions on `id` are resolved later by dedup, not here

### `base_water.py` — same coastline pattern + geometry rewrite

- id change/rebind (mint): coastline-derived ocean polygons keyed `oceanocean` via same `generate_uuid3` pattern
- join: same unkeyed `.union()` of OSM- and coastline-derived water rows
- revision (`water_from_osm.py:86-91`, see §6): `Polygon` geometry on `waterway` in (`canal`,`drain`,`ditch`) rewritten to `ST_ExteriorRing` (polygon → line)

### `base_land_cover.py` — pass-through + provenance stamp

- column removed: select down to `[id, version, subtype, cartography, geometry]`
- drop: `geometry IS NULL`
- drop: `ST_ISEMPTY(ST_GeomFromWKB(geometry)) = true`
- drop: `ST_ISVALID(ST_GeomFromWKB(geometry)) = false`
- new column: `sources` = single struct literal (`dataset="ESA WorldCover"`, `license="CC-BY-4.0"`, `update_time`, `provider`, `resource`, `version`)
- no id re-mint, no classification

### `base_bathymetry.py` — grid-chip inversion (lines 63-140)

- black box: `ST_SubDivide` splits each input depth-`d` coverage polygon into smaller pieces
- black box: 1°×1° world grid built; per-cell-per-depth `covered` geometry computed via `ST_Intersection` + `ST_Union_Aggr` — **raw `spark.sql()` block, ~lines 107-120**
- revision: geometry replaced per cell — `CASE WHEN c.covered IS NULL THEN g.cell ELSE ST_CollectionExtract(ST_Difference(g.cell, c.covered), 3) END` — **raw `spark.sql()` block, ~lines 125-139** (both flagged black box per this doc's brief)
- drop: `NOT ST_IsEmpty(geom)` filter removes fully-covered (now-empty) cells
- id change/rebind (mint): `id = generate_uuid3("BATHYMETRY", 'bathymetry' || depth || WKT)` per output polygon (no 1:1 correspondence to input polygons)

## 5. OSM Adjudicator — `overture_rc` bundle, violation-store merge

**Files**: `airflow/dags/osm/dataset_osm_adjudicator_dag.py`, `omf/omf/adjudicator/osm_adjudicator.py`

- black box: reads/writes managed Iceberg tables `entity_violations_table`, `entity_fast_forward_table` (`src/iceberg.py`)

**Sprint reset day** (`ResetCopyFormats`, `reset_copy_formats.py`):
- column removed: `ext_osm_id`, `ext_debug` dropped from `osm_in_overview` parquet
- write: both plain-Parquet and GeoParquet copies emitted; no adjudication logic runs

**Regular day** (`OSMAdjudicator.execute_job`, `osm_adjudicator.py:30-198`):
- black box: one multi-CTE `spark.sql()` query joining `entity_fast_forward` (Iceberg, reviewer `fixed`/`already_fixed` since last reset), `entity_violations` (Iceberg, active violation name in hardcoded `CRITICAL_VIOLATION_NAMES`, `dataset_osm_adjudicator_dag.py:52-68`), and `entity_geometry` (as of last sprint reset)
- aggregation: emits one row per `id` with `version_at_reset`, `violations_at_reset` → `adjudicator_output` routing table

**Merge** (`OSMAdjudicatorMerge.execute_job`, `osm_adjudicator.py:219-288`):
- black box: two raw `spark.sql()` filters
- drop: `df_reset` keeps rows where `split(sources[0].record_id,'@')[0] NOT IN (SELECT id FROM adjudicator_ops)`
- drop: `df_today` keeps rows where `split(sources[0].record_id,'@')[0] IN (SELECT id FROM adjudicator_ops)`
- merge: `df_reset.unionByName(df_today, allowMissingColumns=True)` — per `id`, exactly one of {reset version, today's version} survives; the other snapshot's row for that `id` never appears
- write: Parquet to `legacy_data/` (feeds §3/§4), GeoParquet to `data/`

## 6. OSM-to-Overture conversion (`osm_in_overture` bundle)

**Files**: `airflow/dags/osm/dataset_osm_ingest_dag.py`, `overture_base/overture_base/{land,water,land_use,infrastructure}_from_osm.py`, `overture_osm/overture_osm/osm_common.py`, `overture_base/overture_base/tag_classification.py` + `{land,water,land_use,infrastructure}_rules.py`

Reads from `geometry_daily` (§7).

### Pre-filter — `land_from_osm.py:34-46` (each sub-type has its own equivalent: `water_from_osm.py:32-48`, `land_use_from_osm.py:33-38`, `infrastructure_from_osm.py:36-39`)

- drop: WHERE `element_at(tags,'natural') IS NOT NULL OR element_at(tags,'surface') IS NOT NULL OR element_at(tags,'landcover') IS NOT NULL OR element_at(tags,'landuse') IN ('forest') OR element_at(tags,'place') IN ('archipelago','island','islet') OR element_at(tags,'geological') IN ('meteor_crater','volcanic_caldera_rim')) AND element_at(tags,'highway') IS NULL AND element_at(tags,'building') IS NULL AND element_at(tags,'golf') IS NULL AND element_at(tags,'leisure') IS NULL

### Classification rules — `tag_classification.py:235-262` (`rules_to_column`)

- new column: `overture` struct `<subtype:string, class:string>`, assigned by first-matching rule in an ordered rule list (`land_rules.py`, `water_rules.py`, etc., e.g. `water_rules.py:13-25`); no match → `NULL` struct
- drop: subsequent `.filter(F.col("overture.subtype").isNotNull())` removes unclassified rows

### Identity assignment — `overture_osm/osm_common.py:72-136`

- new column/id mint: `id = uuid3(namespace(LAND|WATER|LAND_USE|INFRASTRUCTURE), subtype || class || <osm-type-letter><osm-id>)` (e.g. `land_from_osm.py:71-76`) — content-derived from both OSM id and assigned subtype/class; a rules-table change yields a different `id` for the same OSM feature
- new column: `sources` array of one struct — `dataset="OpenStreetMap"`, `license="ODbL-1.0"`, `record_id = concat(osm_type[0:1], osm_id, '@', osm_version)`, `update_time`, `provider="osm"`, `resource="planet"`, `version = greatest(pull_ts, update_time)`

### `water_from_osm.py` — polygon-to-line geometry rewrite (lines ~86-91)

- revision: `geometry` = `ST_ExteriorRing(geometry)` when `ST_GeometryType(geometry) = 'ST_Polygon'` AND `tags['waterway'] IN ('canal','drain','ditch')`, else unchanged

## 7. OSM daily geometry construction (`geometry_daily`)

**Files**: `omf/omf/utilities/osm_geometry.py`, `omf/omf/osm/osm_geometry_osc.py`, `omf/omf/osm/osm_osc_collect.py`

### Raw ingest — `OSCData` (lines 909-1069), `download_daily_osc_data` (lines 1073-1106)

- black box: HTTP GET `https://planet.openstreetmap.org/replication/{frequency}/state.txt`, then GET the referenced `.osc` gzip
- black box: `_iterparse_xml` (lines 992-1059) parses OSC XML into `create`/`modify`/`delete` buckets per OSM type
- write: parquet via `OSMOscCollect`

### Applying the diff — `build_osm_geometry_with_cache` (lines 670-891)

- merge: `osc_dedup` (lines 18-37) — `GROUP BY id, type`, `max_by(<every field>, version)` — one row per id at latest version
- join: `apply_osc_with_geometry` (lines 545-572) full-outer-joins `base_data` (a) to `updated_data` (b) on `id`
- id-scoped revision: `id = IF(b.id IS NULL, a.id, b.id)`, `geom = IF(b.id IS NULL, a.geom, b.geom)` — today's row overwrites yesterday's for matching `id`
- drop: left-outer-join result against `deleted_data` on `id`, keep only `WHERE b.timestamp IS NULL` (anti-join drops OSM-reported deletes)
- black box: `build_node_geometry` (lat/lon → `ST_Point`)
- black box: `build_linestring_for_ways` (`ST_LineFromMultiPoint` over member node points in order)
- revision: `build_ways_geometry_from_linestring` (lines 225-276) — closed linestring → polygon only if way tags intersect an allow-list (`building`, `landuse`, `natural`, `amenity`, etc., lines 242-266); otherwise stays a line
- black box: `merge_ways_into_multipolygon`, `_merge_lines`, `aggregate_polygons_for_relation` — relation geometry via `ST_SymDifference` aggregation, plus custom cycle-detection Python (`_get_all_cycles`, lines 316-344)
- revision: `fix_invalid_geometries` (lines 285-312) — `ST_MakeValid` then `ST_ForcePolygonCCW`, repairs in place (not dropped)
- revision: relations over `MAXIMUM_RELATION_GEOMETRY_SIZE` (30MB WKB) run through `ST_SimplifyPreserveTopology` (`save_geo_parquet_osm`, lines 659-666) — lossy, not rejected

## 8. OSM planet bootstrap (`geometry_planet`)

**Files**: `airflow/dags/osm/dataset_osm_geometry_reset_dag.py`, `omf/omf/osm/osm_geometry_planet.py`

- black box: `S3KeySensor` waits on `PBF_KEY` = `.../planet-YYMMDD.osm.pbf` in external bucket (`dataset_osm_geometry_reset_dag.py:52-73`); file produced outside this repo
- black box: `EcsRunTaskOperator` runs `convert_planet_pbf_to_parquet` on Fargate — PBF→parquet conversion, non-Spark, not traced further
- black box/revision: `OSMGeometryPlanet` runs the same node→way→relation construction functions as §7 (`build_node_geometry`, `build_linestring_for_ways`, `build_ways_geometry_from_linestring`, `merge_ways_into_multipolygon`, `aggregate_polygons_for_relation`) against the full planet dump, no prior base table
- merge: reset DAG replays every OSC day from the planet date through yesterday using the §7 `OSMGeometryOSC` logic (join/drop/revision as in §7) to catch up to "today"

## 9. OSM Coastlines (`coastline_bundle`)

**Files**: `airflow/dags/dataset_osm_coastline_collect_dag.py`, `overture_coastlines/overture_coastlines/{shapefile_to_parquet.py,coastlines_cluster.py,coastline_diff_compare.py}`

### Raw source

- black box: shell script in ECS Fargate — `aria2c` zip download from `osmdata.openstreetmap.de/download` (land-polygons / water-polygons / coastlines shapefiles), `7z` extract, `aws s3 cp` to S3; no checksum/signature verification

### `ShapefileToParquet`

- column removed: `df.select(select_statement)` keeps only `geometry` (all other shapefile attribute columns dropped)
- new column: literal `type`/`ds`-style columns added from `input_dict` via `withColumn(key, lit(value))`
- split: `ST_SubDivide(geometry, max_vertices)` exploded (`F.explode`) — one input polygon → many output polygons, run a second time with `max_vertices=100` into a separate `subdivided/` output
- write: geoparquet

### `CoastlinesCluster` — diff/QA, does not feed live base data

- revision (diff-report scope only): both snapshots simplified via `ST_SimplifyPolygonHull`
- drop (diff-report scope only): equi-join on identical simplified geometry excludes unchanged polygons from the diff
- join: `ST_Intersects` (bbox prefilter, then exact) between new snapshot and `coastline_base_bundle` (pinned via `COASTLINE_BASE_DS`)
- aggregation: DBSCAN clusters centroids of no-overlap ("major change") geometries; raw geometries unioned per cluster → `osm_coastline_diff` report table
- does not modify data consumed downstream by base; side-channel artifact for human review before promoting a new `COASTLINE_BASE_DS`
- `coastline_diff_compare.py`: dead code, no DAG references it
- black box (separate tool, not `overture_coastlines`): `containers/coastline-compare/main.py` — raw DuckDB SQL against S3 parquet, shells out to `tippecanoe` via `os.system`, uploads via boto3

### Output

- write: `SourceRawBundle(provider="osm", resource="coastlines", version=DS)` at `datasets/provider=osm/resource=coastlines/version={DS}/run={RUN_ID}/data/{type=land|type=water}` — consumed by §3/§4 `base_land`/`base_water` as `s3_input_path_coastlines`
- `theme_base_stage_dag.py`'s `coastline_ds` param defaults to `COASTLINE_BASE_DS` (vetted snapshot), not necessarily the latest weekly collect

## 10. ESA WorldCover (`land_cover_bundle`)

**No ingest DAG in this repo** — trace dead-ends here on the raw side.

- input arrives pre-shaped as Parquet (`id, version, subtype, cartography, geometry`) via a process entirely outside this repo
- `theme_base_stage_dag.py` only resolves the S3 partition with a `success` marker (`resolve_partitions_task`)
- downstream transform: see §4 `base_land_cover.py` (geometry-sanity drops + `sources` stamp, pass-through otherwise)

## 11. NCEI ETOPO/GLOBathy (`bathymetry_bundle`)

**No ingest DAG in this repo** — trace dead-ends here on the raw side.

- input arrives pre-shaped as Parquet (`id, version, depth, cartography, geometry`) via a process entirely outside this repo
- downstream transform: see §4 `base_bathymetry.py` — `ST_MakeValid` repair (revision, not drop), then grid-chip inversion via the two raw `spark.sql()` blocks flagged black box in §4 (`base_bathymetry.py` ~lines 107-120 and ~125-139)

---

## Summary

| stage | operations | id impact |
|---|---|---|
| 1. Release Publish | write | none |
| 2a. compute_internal_changelog | join, new column, aggregation | none (side-table only) |
| 2b. process_data | join, drop, drop, write | none |
| 3. Theme Base Stage | write (fan-in of 6 jobs) | none |
| 4. base_common (name promotion) | revision | none |
| 4. base_common (id re-mint safety net) | id change/rebind | rebind (no-op in steady state) |
| 4. base_common (dedup) | merge | merge |
| 4. base_land.py (coastline union) | id change/rebind (mint), new column, join (union) | mint + merge (via dedup) |
| 4. base_water.py (coastline + geometry) | id change/rebind (mint), join (union), revision | mint + merge (via dedup) |
| 4. base_land_use.py / base_infrastructure.py | revision, id change/rebind, merge | rebind + merge |
| 4. base_land_cover.py | column removed, drop, drop, drop, new column | none |
| 4. base_bathymetry.py | black box, black box, revision, drop, id change/rebind (mint) | mint |
| 5. OSM Adjudicator (reset day) | column removed, write | none |
| 5. OSM Adjudicator (regular day) | black box, aggregation, drop, drop, merge, write | merge (per id, one snapshot wins) |
| 6. OSM-to-Overture (pre-filter) | drop | none |
| 6. OSM-to-Overture (classification) | new column, drop | none |
| 6. OSM-to-Overture (identity) | new column, id change/rebind (mint) | mint |
| 6. water_from_osm.py (geometry) | revision | none |
| 7. OSM daily geometry (ingest) | black box, write | none |
| 7. OSM daily geometry (diff apply) | merge, join, revision, drop | id-scoped overwrite |
| 7. OSM daily geometry (construction) | black box, revision, black box, revision, revision | none |
| 8. OSM planet bootstrap | black box, black box, black box/revision, merge | none |
| 9. OSM Coastlines (raw) | black box | none |
| 9. OSM Coastlines (shapefile→parquet) | column removed, new column, split, write | none |
| 9. OSM Coastlines (cluster/QA) | revision, drop, join, aggregation, black box | none (side artifact) |
| 10. ESA WorldCover | (see stage 4 base_land_cover.py) | none |
| 11. NCEI ETOPO/GLOBathy | (see stage 4 base_bathymetry.py) | mint |
