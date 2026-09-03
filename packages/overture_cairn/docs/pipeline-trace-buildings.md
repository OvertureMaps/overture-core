# Buildings Theme Pipeline Trace (Release → Raw Source)

This document traces the Overture **buildings** theme backward from the published
release to the earliest raw-source read. It is research material for provenance
tracking: at each stage it records what code runs, what actually happens to the
data (filters, joins, id assignment, merges, drops), and flags anything that is
hard to see into (violation-store writes, raw SQL, black-box/cross-language calls).

Order: this document reads **forward** (source → release) for narrative clarity,
even though it was researched backward from the release-publish DAG. See the
summary at the end for the strict pipeline order.

---

## 0. Shared stages (near-identical across all themes)

### 0.1 `release_publish_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/release_publish_dag.py`

Takes a staged release-candidate bundle (already fully assembled/promoted, all
themes present) and fans it out to production destinations. It does not
transform feature data at all — it is pure data movement and cataloguing:

- DataSync copies of `data/`, `changelog/`, `bridgefiles/`, `registry/` from the
  scratch bucket to the AWS release bucket, an Azure blob container, and an
  archive bucket (`release_publish_dag.py:75-211`).
- A boto3 multipart copy of PMTiles to an "extras" bucket (`:327-332`), done
  outside DataSync because of file size.
- Runs `PublishStac` (`overture_core.stac.job`) via a serverless Fargate task
  group to generate/update the STAC catalog scoped to this one release
  (`:334-351`).
- Invalidates the STAC CloudFront distribution, then starts four Glue crawlers
  in the Distribution account to refresh the release/registry/changelog/bridge
  Glue catalogs (`:355-376`).
- Stamps the release-candidate bundle as `released` via
  `ReleaseCandidateBundle.tag_as_released_from_uri` (`:378-380`).

No record is filtered, merged, or re-identified here — this stage is a copy/
publish/catalog step, not a transform step.

### 0.2 `theme_promote_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_promote_dag.py`

Parameterized over all six themes (`buildings` included, `PIPELINE_CONFIG` at
`:13-20`); one DAG instance per theme (`theme_buildings_promote_dag` etc). Pulls
together a theme's `theme_assemble` output bundle as input, and produces the
`theme_promote` bundle that release-publish later reads. Per the DAG's own
`dag_doc_md` (`:24-53`) and the `theme_promote_task_group` it calls (`:130-137`,
defined in `src/public/overture_airflow/theme_promote.py`, not opened in this
trace since it is shared plumbing, not buildings logic), it:

- validates the input theme data against `overture-schema`,
- computes an internal changelog / churn statistics,
- copies theme data from the assemble bundle into the promote staging area,
- generates PMTiles for map visualization,
- generates bridge files.

This is schema-validation, changelog computation, and format/packaging — not
feature-level filtering or re-identification. From here on, everything is
buildings-specific.

---

## 1. Assemble / conflation stage — `theme_buildings_assemble_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_buildings_assemble_dag.py`

This is the DAG that used to be called "buildings conflation" (only a stale
`.pyc` of that name was found; the live source is this assemble DAG — it
contains the spatial merge / tag merge / post-merge-filter / stage task groups
that conflation historically referred to). It no longer reads per-source feed
data directly; it reads a pre-matched **corpus** snapshot (see §2.4) that
already carries cross-source GERS ids.

### 1.1 Setup: corpus fetch

Task: `fetch_corpus` (`theme_buildings_assemble_dag.py:244-258`) triggers
`corpus_data_export_dag` for `ThemeName=buildings, TableName=building,
BranchName=main`, writing a combined parquet export to the output bundle's
`corpus/theme=buildings/type=building` path. This is a **black-box call**: the
actual work is an `overture_corpus` Scala/Iceberg job invoked by the corpus DAGs
(`corpus_data_export_dag.py`, `corpus_utils.get_corpus_jar_path_task`) — a JVM
job outside this Python codebase that reads/writes the shared Iceberg corpus
table. This trace does not have visibility into its internals.

Also in setup: `overture_water` task group runs `TileWater`
(`overture_buildings/tile_water.py`), reading `theme=base/type=water` from the
latest full Overture release and using Sedona `ST_SubDivide` to chop large
water polygons into ≤100-vertex pieces (`tile_water.py:48-63`) — purely a
performance transform for the later water-intersection filter, not a semantic
one.

### 1.2 Spatial merge — `BuildingSpatialMerge`

DAG task: `spatial_merge.building_spatial_merger_spark`
(`theme_buildings_assemble_dag.py:266-286`), running
`overture_buildings.building_spatial_merge.BuildingSpatialMerge`.

This is the core cross-source conflation step. For each source in priority
order (`SOURCE_NAMES = ["osm", "esri", "ign_spain", "vancouver", "google_high",
"microsoft", "google_low", "zenodo"]`, `theme_buildings_assemble_dag.py:34-43`):

```python
input_df = (
    self.spark.read.parquet(input_dir)
    .where(f"source = '{source_name}'")
    .where("corpus_state IS NULL OR corpus_state = 'active'")
)
...
source[source_name]["df"] = (
    input_df
    .withColumn("rank", F.row_number().over(
        Window.partitionBy(["id"]).orderBy(F.col("sources")[0].getField("record_id").asc())
    ))
    .filter(F.col("rank") == 1).drop("rank")
)
```
(`building_spatial_merge.py:26-50`)

Then, once at least one higher-priority source has already been placed into the
running `v_all` view, every lower-priority building that spatially intersects
an already-placed building is suppressed — **unless** the placed building is
itself deleted and the candidate's capture date is newer than that deletion:

```python
intersect = self.spark.sql(f"""
    SELECT i.id, i.source, i.geom AS geom, v_all.geom AS intersect_geom, v_all.source AS intersect_source
    FROM v_all, v_input_{source_name} i
    WHERE ST_Intersects(i.geom, v_all.geom)
      AND (
          v_all.deleted_at IS NULL
          OR CAST(i.sources[0].update_time AS TIMESTAMP) IS NULL
          OR CAST(i.sources[0].update_time AS TIMESTAMP) <= v_all.deleted_at
      )
""")
```
(`building_spatial_merge.py:72-104`)

Suppressed buildings are written as `filter_type=conflate` records (a
disposition record, not the violation-store Iceberg table) under
`s3_output_filter_path`. Everything else — the "spatial_merged" set — is what
survives to tag merge. **Data effect**: within-source duplicate `id`s are
collapsed to one row (lowest `record_id` wins); cross-source geometric
duplicates are dropped entirely in favor of the higher-priority source, except
when the higher-priority record was deleted and the lower-priority one is
newer imagery. No new ids are minted here — ids already came from the corpus
(assigned back in the matcher, §2.4).

### 1.3 Tag merge — `BuildingTagMerge`

DAG task: `tag_merge.building_tag_merge_spark`
(`theme_buildings_assemble_dag.py:293-313`), running
`overture_buildings.building_tag_merge.BuildingTagMerge`.

Enriches the spatially-merged buildings' `height` from three competing
sources — the input's own height, Esri's corpus height, Microsoft's corpus
height, and USGS LiDAR — picking the first non-outlier value in priority
order (`input`, `esri`, `lidar`, `ms`):

```python
best_expr = when(col("input.height").isNotNull(), col("input.height")) \
    .when(col("esri.height").isNotNull() & ~col("esri_is_outlier"), col("esri.height")) \
    .when(col("lidar.height").isNotNull() & ~col("lidar_is_outlier"), col("lidar.height")) \
    .when(col("ms.height").isNotNull() & ~col("ms_is_outlier"), col("ms.height")) \
    .otherwise(lit(None))
```
(`building_tag_merge.py:162-175`, condensed)

An "outlier" is a candidate whose relative distance from the min/max of the
*other* three candidates exceeds 50% (`building_tag_merge.py:117-160`). When a
non-input height wins, that source's `sources` struct is appended to the
building's `sources` array (`:227-236`) — this is a **provenance-append**
moment: a building whose footprint came from OSM can end up with a `sources`
array containing an Esri or LiDAR entry purely for the height attribute.

### 1.4 Post-merge filters — `BuildingIntersect` (×2) and `BuildingPostMergeFilter`

DAG task group: `post_merge_filter`
(`theme_buildings_assemble_dag.py:320-429`).

Three parallel Spark jobs each write **violation records** (not filtered
output directly) to a shared `violations/` path, later merged into the Iceberg
entity-violations table:

- `building_transportation_intersection_filter_spark` — `BuildingIntersect`
  against the current release's `theme=transportation/type=segment`, flags any
  building geometry that intersects a road segment
  (`overture_buildings/building_intersect.py`).
- `building_water_intersection_spark` — `BuildingIntersect` against the
  `TileWater` output, flags buildings intersecting water. Severity differs by
  source: ML-derived buildings (`zenodo`, `google_high/low`, `microsoft`) get
  severity 1 (real violation), everything else gets -1 (informational only):
  ```python
  F.when(F.col("building.source").isin("zenodo","google","google_high","google_low","microsoft"), 1)
   .otherwise(-1).alias("severity")
  ```
  (`building_intersect.py:50-57`)
- `building_post_merge_filter_spark` — `BuildingPostMergeFilter` computes two
  geometry-quality violations directly with raw Sedona SQL: `building_invalid_area`
  (spherical area is null/≤0) and `building_too_many_small_angles` (more than 5
  corners with interior angles <30° or >330°, computed via an exploded
  point-triplet `ATAN2` calculation — see `_find_buildings_with_small_angles`,
  `building_post_merge_filter.py:68-140`). **Note**: this angle-detection logic
  is a hand-rolled geometric SQL block, non-trivial to audit from outside.

All four violation types (`building_transportation_intersection`,
`building_water_intersection`, `building_invalid_area`,
`building_too_many_small_angles`) are merged into the Iceberg entity-violations
table by `update_entity_violations`
(`omf.entity_violations.entity_violations_update.EntityViolationsUpdate`,
`theme_buildings_assemble_dag.py:382-399`). **This is a violation-store write**:
a `MERGE INTO` Iceberg SQL statement keyed on `(id, violation_name, version,
counterpart, dataset)` (`entity_violations_update.py:29-63`) — upserts records,
never deletes them, so the violation table accumulates history.

`building_filter_spark` (`BuildingFilter`,
`overture_buildings/building_filter.py`) is the step that actually drops rows.
It:
1. dedups by `id` again (lowest `record_id` wins — same pattern as spatial
   merge, run again defensively) (`:16-29`),
2. anti-joins against the Iceberg violations table for the four post-merge
   violation names where `severity > 0`, matched on `(id, dataset, version)`
   (`:45-62`),
3. drops any row where `deleted_at IS NOT NULL` — buildings that exist only to
   participate in conflation (suppressing a stale ML building) never ship in
   the release:
   ```python
   # Exclude demolished/deleted buildings from the release. They
   # participated in merge (suppressing stale ML buildings) but must not
   # ship in released data. The corpus remains the record of deletions.
   output_df = output_df.where("deleted_at IS NULL")
   ```
   (`building_filter.py:64-67`)

### 1.5 Stage — `BuildingPartsStage` and `BuildingStage`

DAG task group: `stage` (`theme_buildings_assemble_dag.py:435-483`).

- `BuildingPartsStage` (`overture_buildings/building_parts_stage.py`) joins the
  filtered OSM-derived buildings (`sources[0].dataset = 'OpenStreetMap'`
  filter, `:25`) against the OSM-native building-parts feed
  (`building_parts_bundle`, the OSM `overture_rc` bundle, not the corpus) by
  matching each part's OSM `building_id` tag against the building's own OSM
  `record_id` (split on `@`) (`:26-56`). Only building parts that are valid
  Polygon/MultiPolygon geometry survive (`:38-45`). This is an **inner join**:
  building parts whose parent building was filtered out of the release (e.g.
  suppressed in spatial merge, or dropped for a violation) are silently
  dropped too, since the join has nothing to match against.
- `BuildingStage` (`overture_buildings/building_stage.py`) joins the filtered
  buildings against the distinct set of `building_id`s that got a part, to set
  `has_parts` (`:39-72`). Also promotes OSM `names.common`/`names.rules` into
  `names.primary` when no primary name is already set (`_promote_common_to_primary_name`,
  identical logic duplicated in both `building_stage.py:88-115` and
  `building_parts_stage.py:91-118`).

Output of `BuildingStage` (`data/theme=buildings/type=building`) and
`BuildingPartsStage` (`data/theme=buildings/type=building_part`) is what
`theme_buildings_assemble_dag`'s `finalize_bundle` (`:481`) ships as the
`theme_assemble` bundle — the input to `theme_promote_dag` in §0.2.

---

## 2. Ingest stage — `theme_buildings_ingest_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_buildings_ingest_dag.py`

Runs daily (`schedule=("0 0 * * *")`, `:93`). Per its own docstring
(`:1-7`), this DAG now performs corpus matching *at ingest time* (once per
source, when source data changes) rather than at every conflation run — a
structural change from the older buildings pipeline where matching happened
inside the conflation DAG.

For each vendor source (`esri`, `google_high`, `google_low`, `microsoft`,
`ign_spain`, `zenodo`, `vancouver` — all defined via `SourceInfo` at
`:192-244`), and separately for `osm` and the `usgs` LiDAR signal, the chain
is: **ingest → pre-match filter → violation update → match against corpus →
(optionally register new corpus schema) → update corpus**.

### 2.1 Feed ingest — `BuildingIngest`

Task: `<source>.building_feed_ingest`
(`theme_buildings_ingest_dag.py:277-305`), running
`overture_buildings.building_ingest.BuildingIngest`
(`overture_buildings/building_ingest.py`).

This is the first place raw vendor data is read. It calls
`building_sources.initialize_schema(...)`
(`overture_buildings/building_sources.py:17-332`), which is a **per-source
schema translator** — a long `if source == "esri": ... if source ==
"ign_spain": ... if source == "microsoft": ...` block that reads the vendor's
native parquet/geoparquet schema and produces Overture's `sources` struct and
a normalized `tags` map. Examples of source-specific identity/attribute
mapping baked in here:

- Esri: `record_id` is extracted by substringing `FeatureUID` (`:53`); license
  is hardcoded to "Creative Commons by Attribution (CC BY 4.0) with
  OpenStreetMap waivers" (`:50-52`).
- Google (`google_high`/`google_low`): `record_id` is the `full_plus_code` tag;
  a `Window.partitionBy("record_id").orderBy(monotonically_increasing_id())`
  dedup keeps only the first row per plus-code before the corpus even sees the
  data (`:151-161`). `google_high` vs `google_low` is not a real vendor split —
  it's the same `open_buildings` resource, split downstream in
  `theme_buildings_ingest_dag.py:199-212` by `tags['high_precision'] = 'true'`
  vs not, i.e. one raw feed is fissioned into two "sources" purely by an ingest
  filter expression.
- Microsoft: `update_time` falls back to a sentinel `"2000-01-01T00:00:00.000Z"`
  when `imagerycapturedate` is missing or in the future relative to the
  snapshot version (`:206-223`) — a data-quality workaround baked directly into
  ingest.
- Vancouver: `record_id` substrings `esri_id`; `roof:height` is *computed*
  (`bldgheight - eaveheight`) at ingest, not read from source (`:317-320`).

After schema normalization, `BuildingIngest.execute_job` applies a bbox filter,
mints a **new random id** per record (`F.expr("uuid()")`, `building_ingest.py:41`
— this is a throwaway pre-corpus id, later replaced or kept by the matcher),
and derives all attribute columns (`height`, `subtype`, `class`, `roof_*`,
etc.) via `building_common.py` helper functions
(`building_ingest.py:42-59`). Output is written as GeoParquet per source.

**Note**: `initialize_schema` has no `osm` or `usgs` branch — those two sources
bypass `BuildingIngest` entirely (see §2.5/§3).

### 2.2 Pre-match filter — `BuildingPreMatchFilter`

Task: `<source>.pre_match_filter`
(`theme_buildings_ingest_dag.py:307-328`), running
`overture_buildings.building_pre_match_filter.BuildingPreMatchFilter`.

Computes five violation types directly from geometry/area checks and writes
them (not filters them out yet) to a `violations/` path:
`building_tiny` (< `MIN_ML_BUILDING_AREA_SQ_METERS`, ML sources only, via
`~sources[0].dataset.isin(NON_ML_SOURCES)`), `building_large` (>
`MAX_ML_BUILDING_AREA_SQ_METERS`, ML only), `building_huge` (>
`MAX_BUILDING_AREA_SQ_METERS`, all sources), `building_invalid_geometry`
(`NOT ST_ISVALID` or empty after `ST_MakeValid`), and
`building_duplicate_record_id` (same vendor `record_id` appears more than once
in this batch, found via a `Window.partitionBy("record_id")` count)
(`building_pre_match_filter.py:65-165`). These limits come from
`src/public/building/limits.py` (`MIN_ML_BUILDING_AREA_SQ_METERS`,
`MAX_ML_BUILDING_AREA_SQ_METERS`, `MAX_BUILDING_AREA_SQ_METERS`).

### 2.3 Violation update

Task: `<source>.update_entity_violations`
(`theme_buildings_ingest_dag.py:330-350`), running
`omf.entity_violations.entity_violations_update.EntityViolationsUpdate`. Same
Iceberg `MERGE INTO` mechanism described in §1.4 — **another violation-store
write**, this time for the five pre-match violation types.

### 2.4 Match against corpus — `BuildingMatcher` (identity assignment)

Task: `<source>.match_corpus`
(`theme_buildings_ingest_dag.py:387-413`), running
`overture_buildings.building_matcher.BuildingMatcher`
(`overture_buildings/building_matcher.py`). **This is where GERS ids are
actually assigned** — the single most important identity-transformation point
in the whole buildings pipeline.

Inputs: the freshly-ingested source feed, a corpus snapshot exported once per
DAG run (`fetch_corpus` task, `theme_buildings_ingest_dag.py:491-503`, another
black-box `overture_corpus` Scala export), and the Iceberg entity-violations
table filtered to `severity > 0` pre-match violations
(`building_tiny/large/huge/invalid_geometry/duplicate_record_id`). Records that
match a violation on `(id, dataset, update_time)` are anti-joined out before
matching even starts (`building_matcher.py:82-101`).

Spatial match — intersection-over-union against every corpus building:
```sql
SELECT
    v_feed.id, v_corpus.id AS release_id,
    ST_AREA(ST_Intersection(v_feed.geom, v_corpus.geom)) / ST_AREA(ST_Union(v_feed.geom, v_corpus.geom)) AS iou,
    v_feed.deleted_at, v_feed.sources[0].record_id AS record_id
FROM v_corpus, v_feed
WHERE ST_Intersects(v_corpus.geom, v_feed.geom)
```
(`building_matcher.py:110-119`)

Only matches with `iou > 0.5` are kept, and only the single best (highest-IOU)
corpus match per feed `id` survives:
```python
match_df = (
    match_df.where("iou > 0.5")
    .withColumn("id_rank", F.row_number().over(Window.partitionBy(["id"]).orderBy(F.col("iou").desc())))
    .filter(F.col("id_rank") == 1)
)
```
(`building_matcher.py:142-151`)

Then, per corpus `release_id`, only the single best feed match is kept — active
beats deleted, latest `deleted_at` wins among deleted, `record_id` is the
tiebreaker — and every other candidate is written out as a `duplicate_match`
filter record and excluded from the mapping:
```python
filter_df = (
    match_df.withColumn("release_id_rank", F.row_number().over(
        Window.partitionBy(["release_id"]).orderBy(
            F.col("deleted_at").isNull().desc(),
            F.col("deleted_at").desc_nulls_last(),
            F.col("iou").desc(),
            F.col("record_id").asc(),
        )))
    .filter(F.col("release_id_rank") > 1)
    ...
)
```
(`building_matcher.py:157-179`)

Finally, the actual id assignment — a matched building **inherits the
corpus's existing GERS id**; an unmatched (new) building keeps its own
(ingest-minted) id:
```sql
SELECT
    COALESCE(mapping.release_id, feed.id) AS id,
    feed.names, feed.height, ..., feed.sources,
    ST_AsBinary(ST_ForcePolygonCCW(ST_Force_2D(feed.geom))) AS geometry,
    COALESCE(mapping.iou, 0) AS iou,
    feed.deleted_at
FROM v_feed feed
LEFT JOIN v_mapping mapping ON feed.id = mapping.id
```
(`building_matcher.py:197-228`)

A final per-`id` dedup (active beats deleted, then `record_id` tiebreak) runs
once more before write (`:231-244`). Output (`matched/source=<source>/`) feeds
both the next stage (corpus update, below) and the assemble DAG's spatial
merge (§1.2) reads from the corpus, not from this path directly.

### 2.5 Corpus update — black-box Scala/Iceberg write

Tasks: `<source>.branch_register` → optionally `<source>.register_corpus`
(`corpus_register_theme_dag`) → `<source>.update_corpus`
(`corpus_data_load_dag`) (`theme_buildings_ingest_dag.py:415-485`). Both
triggered DAGs run the `overture_corpus` Scala job
(`corpus_data_load_dag.py` doc: "Loads Overture data into corpus Iceberg
tables... using the ... overture_corpus DataLoad Scala job") — **a black-box,
cross-language write** into the shared Iceberg corpus table, keyed by
`IdField=id`, tagged with `Source=<source>`. This is the mechanism by which
each source's newly-matched (or newly-minted) GERS ids and geometry become
durable and visible to the next source's matcher and to the assemble DAG.
Nothing about the merge/conflict-resolution logic inside that Scala job is
visible from this repo.

### 2.6 OSM ingest branch inside this DAG

Task group `osm` (`theme_buildings_ingest_dag.py:517-712`) mirrors the same
pre-match-filter → violation-update → match-corpus → update-corpus chain, but
its input is not a raw vendor drop — it's `SourceIngestBundle(provider="osm",
resource="overture_rc")`, i.e. the weekly (last-Saturday) Overture RC feed
already produced by the OSM ingest pipeline (§3). The only OSM-specific step
here is `OsmBuildingExtract`
(`overture_buildings/osm_building_extract.py`), which just reads
`theme=buildings/type=building` out of the full `overture_rc` bundle and
re-writes it as a standalone bundle, with an optional bbox filter — a
straight copy, not yet a transform (`osm_building_extract.py:1-6, 20-36`).

### 2.7 Signal ingest — USGS LiDAR

Task group `usgs` (type=`"signal"`, `theme_buildings_ingest_dag.py:352-373`)
runs `overture_buildings.signal_ingest.SignalIngest`
(`overture_buildings/signal_ingest.py`) — a pure copy (`spark.read.parquet` →
`spark.write.parquet`, no filtering, no schema change). LiDAR never enters the
corpus/matcher; it's consumed directly by `BuildingTagMerge` in the assemble
stage (§1.3) for height enrichment only.

---

## 3. Raw source stage

### 3.1 Vendor building footprints (esri, google, microsoft, ign_spain, zenodo, vancouver)

These sources are read from `SourceRawBundle(provider=..., resource=...)`
(`src/public/overture_airflow/bundle.py:2235-2275`), whose docstring defines
the path convention `datasets/provider={provider}/resource={resource}/version={version}/run={run}`.
**No DAG in this repository produces these raw bundles** — a search of
`airflow/dags` found no ingest DAG for `community_maps_buildings`,
`open_buildings`, `ml_buildings`, `instituto_geografico_nacional_espana`,
`east_asian_buildings`, or Vancouver's `buildings` resource. These are external
vendor deliveries (or separately-run collection jobs not in this codebase) that
land directly at that raw S3 path. From this pipeline's point of view, the raw
read boundary is `BuildingIngest`'s `spark.read.format("parquet"/"geoparquet")`
against that raw path (§2.1) — the true collection mechanism is outside this
trace's visibility.

### 3.2 USGS LiDAR

Same situation: `SourceRawBundle(provider="usgs", resource="lidar")` is read
directly by `SignalIngest` (§2.7); no producing DAG for the raw LiDAR drop
exists in this repo.

### 3.3 OSM — the actual PBF/history read

OSM building data traces back further than the other sources, through a
shared OSM ingest system (also used by base and transportation themes):

1. **`dataset_osm_history_dag.py` / `dataset_osm_history_reset_dag.py`**
   (`airflow/dags/osm/`) convert OSM's full-history planet extract
   (`SourceRawBundle(provider="osm", resource="planet_history")`,
   `dataset_osm_history_reset_dag.py:129-134`) into an Iceberg **full-history**
   table (every version of every entity, including deletions) —
   `get_osm_history_table()` (`src/iceberg.py`). This is the actual OSM
   PBF/planet raw read; its internal conversion logic was not opened in this
   trace (out of buildings-specific scope — it is shared multi-theme
   infrastructure), but it is the terminus of the buildings backward trace.
2. **`dataset_osm_geometry_dag.py`** produces daily OSC-based geometry
   snapshots (`SourceRawBundle(provider="osm", resource="geometry_daily")`,
   `dataset_osm_ingest_dag.py:9-10`), layering daily changesets onto the
   history table.
3. **`dataset_osm_ingest_dag.py`** (`airflow/dags/osm/dataset_osm_ingest_dag.py`)
   converts history + daily geometry into Overture-format rows for base,
   buildings, and transportation. For buildings specifically
   (`:159-222`):
   - `BuildingFromOsm` (`overture_buildings/building_from_osm.py`) reads the
     full-history Iceberg table directly with a large hand-written SQL block
     that classifies every `way`/`relation` row into `active` (currently
     tagged as a building), `retagged` (tags now say `building=no` or a
     lifecycle-deleted tag, but a prior version had a real building tag), or
     `deleted` (the frozen last-visible row of an entity whose newest version
     is invisible), unioning all three into one dataset with a `deleted_at`
     timestamp column (`building_from_osm.py:83-142`). **This raw SQL block is
     the most opaque part of the OSM ingest path** — it encodes OSM lifecycle
     semantics (visible/invisible, is_latest/is_last_visible) that have no
     equivalent anywhere else in the pipeline. It also mints the row's `id` via
     `uuid()` (`:146`) — a random placeholder id, later replaced by the corpus
     matcher (§2.4) if the building matches an existing GERS id. Tag-to-attribute
     translation (height, subtype, class, roof attributes, names) is delegated
     to SQL-fragment builder functions in `overture_buildings/building_sql.py`
     (e.g. `osm_tags_to_subtype()` maps ~40 `building=*` tag values into ~10
     Overture subtype buckets via a large `CASE WHEN ... IN (...)` expression,
     `building_sql.py:349-401`).
   - `BuildingPartsFromOsm` (`overture_buildings/building_parts_from_osm.py`)
     does a geometric+relational join to find OSM building *parts*: members of
     `type=relation` buildings tagged `part`/`outline`, plus free-standing
     `building:part` ways that are spatially covered by a building's footprint
     (`ST_COVERS` with a small buffer, `:156-159, 192-195`). It explicitly
     excludes buildings with anything other than exactly one outline, and
     parts sets where the only part is geometrically identical to the building
     itself (`:220-239`).
   - `BuildingPartFromOsm` (`overture_buildings/building_part_from_osm.py`)
     turns that mapping into final `building_part` rows, deriving each part's id
     via a **deterministic UUIDv3** (namespace `BUILDING_PART`, seeded from OSM
     type+id, `uuid_v3_sql(...)`, `:52-56`) rather than a random uuid — parts
     get stable, re-derivable ids across runs, unlike buildings.

The output of step 3 (`osm_in_overture` ingest bundle,
`theme=buildings/type=building` and `type=building_part`) becomes, one level
up, the weekly `overture_rc` bundle consumed as `provider="osm",
resource="overture_rc"` at the very top of both `theme_buildings_ingest_dag.py`
(§2.6, via `OsmBuildingExtract`) and `theme_buildings_assemble_dag.py`'s
`building_parts_bundle` (§1.5, feeding `BuildingPartsStage`).

---

## Summary: linear pipeline order (raw source → release)

1. **OSM planet history PBF** → Iceberg full-history table (`dataset_osm_history_dag`/`_reset_dag`) — raw source read.
2. **OSM daily geometry snapshot** (`dataset_osm_geometry_dag`) — layers daily OSC changes onto history.
3. **`BuildingFromOsm`** (`dataset_osm_ingest_dag`) — classifies history rows into active/retagged/deleted buildings, assigns a random placeholder id, derives all Overture attributes via raw SQL tag mappings.
4. **`BuildingPartsFromOsm`** → **`BuildingPartFromOsm`** — derives OSM building parts and assigns them deterministic UUIDv3 ids.
5. **Vendor raw drops** (Esri, Google, Microsoft, IGN Spain, Zenodo, Vancouver, USGS LiDAR) land at `SourceRawBundle` paths — external delivery, no producing DAG in this repo.
6. **`BuildingIngest`** (per vendor source, `theme_buildings_ingest_dag`) — vendor-schema-to-Overture-schema translation via `initialize_schema`, mints a random placeholder id, splits Google's one feed into `google_high`/`google_low`.
7. **`OsmBuildingExtract`** — pulls `theme=buildings/type=building` out of the weekly OSM `overture_rc` feed for the ingest DAG's OSM branch.
8. **`SignalIngest`** (USGS LiDAR) — straight copy, no matching, feeds tag merge only.
9. **`BuildingPreMatchFilter`** (per source) — computes tiny/large/huge/invalid-geometry/duplicate-record-id violations.
10. **`EntityViolationsUpdate`** — merges pre-match violations into the Iceberg entity-violations table (violation-store write).
11. **`BuildingMatcher`** (per source, against a per-run corpus export) — spatial IOU match against the existing corpus; **assigns the final GERS id** (matched → inherit corpus id; unmatched → keep the ingest-minted id); dedups one-to-one between feed and corpus.
12. **Corpus update** (`corpus_register_theme_dag` + `corpus_data_load_dag`, black-box Scala/Iceberg job) — writes the matched, GERS-identified rows into the shared corpus table, per source.
13. **`fetch_corpus`** (`corpus_data_export_dag`, black-box Scala/Iceberg job) — exports a combined corpus snapshot for the assemble run.
14. **`BuildingSpatialMerge`** — walks sources in priority order, suppressing lower-priority buildings that spatially intersect an already-placed higher-priority building (unless the higher-priority one is deleted and the candidate is newer).
15. **`BuildingTagMerge`** — enriches `height` from Esri/Microsoft/LiDAR corpus data using outlier-aware priority selection; appends contributing sources to `sources`.
16. **`BuildingIntersect`** ×2 (transportation, water) + **`BuildingPostMergeFilter`** — compute post-merge violations (transportation overlap, water overlap, invalid area, too-many-small-angles).
17. **`EntityViolationsUpdate`** — merges post-merge violations into the Iceberg entity-violations table (violation-store write).
18. **`BuildingFilter`** — anti-joins out records with active post-merge violations; drops all `deleted_at IS NOT NULL` rows (deletions used only for merge conflict resolution, never released).
19. **`BuildingPartsStage`** — inner-joins OSM building parts onto surviving OSM-sourced buildings by OSM record id.
20. **`BuildingStage`** — sets `has_parts`, promotes common/rule-based names to primary.
21. **`theme_promote_dag`** — schema validation, changelog computation, PMTiles/bridge-file generation, staging copy.
22. **`release_publish_dag`** — DataSync/S3 copy to release/archive/Azure destinations, STAC publish, Glue crawler refresh, tag bundle as released.
