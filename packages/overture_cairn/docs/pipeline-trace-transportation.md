# Transportation Theme Pipeline Trace

Research material for overture-cairn (provenance/lineage tracker design). This
document traces the TRANSPORTATION theme's data flow end to end: every place a
transportation record is created, transformed, filtered, matched, merged,
split, or given an identity, from the earliest raw source read through to
release publish.

Research was performed **backward** from `release_publish_dag.py`, but the
sections below are presented **forward** (raw source → release) since that
reads more naturally as a provenance narrative. The final section gives the
backward-to-forward stage list.

Transportation combines two independent base-network source lines (TomTom
Orbis and OpenStreetMap) plus a US DOT HPMS attribute-enrichment line, and
is the theme with the most identity-changing machinery in the platform:
spatial matching, tag adjudication merge, and (in an experimental,
currently-disconnected pipeline) full segment splitting/merging with
production-ID reconciliation.

---

## Stage 0: Shared stages (near-identical across themes)

### 0a. Release Publish — `airflow/dags/release_publish_dag.py`

This DAG is shared by all themes (buildings, places, transportation, etc.)
and is the very last stage. It does not transform feature data — it is a
promotion/distribution step:

- DataSync-copies the release-candidate `data/`, `changelog/`, `bridgefiles/`,
  and `registry/` directories from the scratch bucket to the production
  release bucket (AWS), an Azure mirror, and an archive bucket
  (`release_publish_dag.py:314-326`).
- Copies PMTiles separately via boto3 multipart copy (large files,
  25-150GB) (`:327-332`).
- Runs `PublishStac` (`overture_core.stac.job`) in single-release mode and
  invalidates the STAC CloudFront distribution (`:334-353`).
- Starts four Glue crawlers to refresh the release Data Catalog (`:355-376`).
- Writes a `released` marker / `released_at` timestamp onto the release
  candidate bundle's `metadata.json` via
  `ReleaseCandidateBundle.tag_as_released_from_uri` (`:378-380`).

No record is filtered, merged, or re-identified here — this is pure
promotion/replication of already-finalized bytes.

### 0b. Bridging stage — `airflow/dags/cdp_release_candidate_dag.py`

Between theme promote and release publish, this shared DAG assembles a
combined `ReleaseCandidateBundle` by copying each theme's
`ThemePromoteBundle` output (`data/`, `changelog/`, `bridgefiles/`,
`pmtiles/`, `metrics/`) into per-theme subdirectories of one release
candidate bundle (`cdp_release_candidate_dag.py:133-324`). Also a pure copy —
no per-record transform.

### 0c. Theme Promote — `airflow/dags/theme_promote_dag.py` + `airflow/dags/src/public/overture_airflow/theme_promote.py`

Shared across all six themes (parameterized by `theme` in `PIPELINE_CONFIG`,
`theme_promote_dag.py:13-20`). For transportation this DAG's task group
(`theme_promote_task_group`, `theme_promote.py:32-68`) is triggered with
`input_bundle = ThemeAssembleBundle(theme="transportation")`, i.e. it reads
the `theme_stage/theme=transportation/run=.../` bundle that the
transportation-specific pipeline (Stage 5 below) produces.

Steps, none of which are transportation-specific:

- `validate_data` — runs `overture_cdp.validate_data.ValidateDataJob` against
  overture-schema per `type=` partition (`theme_promote.py:297-316`).
- `compute_internal_changelog` / `compute_public_changelog` — diffs against
  the previous release to compute per-feature churn
  (`theme_promote.py:193-198, 234-252`).
- `process_data` — runs `overture_cdp.process_data.ProcessDataJob` per
  `type=` partition; this is where the optional `bbox` spatial filter is
  applied and where output partitioning/bbox metadata is computed
  (`theme_promote.py:205-232`).
- `generate_metrics`, PMTiles generation, bridge file generation
  (via `TriggerDagRunOperator` → `bridge_file_create_dag`), churn-threshold
  validation, and a final validation pass (`theme_promote.py:254-363`).

This stage can drop/filter records theme-agnostically (bbox clip) and
computes changelog/lineage-adjacent metadata (churn), but does not touch
transportation-specific semantics (segment/connector identity, tag
combobulation, etc.).

---

## Stage 1: Raw source ingest (three independent lines)

Transportation's base road network comes from **TomTom Orbis**, tag/topology
corrections and turn-restriction/route/destination context come from
**OpenStreetMap**, and speed-limit/surface gap-fill comes from **US DOT
HPMS**. Each has its own raw ingest.

### 1a. TomTom Orbis raw ingest — `airflow/dags/theme_transportation_ingest_dag.py`

Runs every 6 hours. Calls the TomTom MCAPI (`get_latest_available_orbis_release`,
`airflow/dags/src/transportation_utils.py:10-70`) to find the latest released
"Overture Transportation" / "WRL" product version, then runs an ECS Fargate
task (`download_tomtom_map_content`) that downloads the OSM-PBF-formatted
Orbis extract straight to S3:

```python
# theme_transportation_ingest_dag.py:146-173
download_tomtom = EcsRunTaskOperator(
    task_id="download_tomtom_map_content",
    ...
    overrides={"containerOverrides": [{
        "name": ECS_CONTAINER_NAME,
        "environment": [
            {"name": "COMMAND", "value": "download_latest"},
            {"name": "S3_OUTPUT_PATH", "value": f"{output_bundle.data_uri}/"},
        ],
    }]},
)
```

This is a **black-box third-party call**: the ECS container talks to
TomTom's API using a key pulled from Secrets Manager
(`/managed-secrets/airflow/variables/tomtom_api_key`) and TomTom's own
extraction/versioning logic is opaque to this pipeline. Output:
`SourceRawBundle(provider="tomtom", resource="orbis", version=<YYWW>)`,
i.e. a single `ot_wrl_{YYWW}.osm.pbf` file. No per-record transform yet —
this is a byte-for-byte extract download.

### 1b. OSM raw ingest — planet bootstrap + daily changesets

Two DAGs cooperate to produce a `SourceRawBundle(provider="osm",
resource="geometry_daily")` snapshot for a given date, which is OSM's daily
"current state of the world" table used throughout the platform (not just
transportation):

**`airflow/dags/osm/dataset_osm_geometry_reset_dag.py`** (manual,
bootstrap/gap-recovery): downloads a weekly OSM **planet.pbf** dump from an
external mirror (`Dataset.from_name("osm", "planet")`,
`dataset_osm_geometry_reset_dag.py:52-57`), waits for it via `S3KeySensor`,
converts it to parquet on ECS, then runs `omf.osm.osm_geometry_planet.OSMGeometryPlanet`
to build a baseline `geometry_planet` bundle (`:99-196`). In parallel it
backfills daily OSM changeset (OSC) files for every day between the planet
date and yesterday.

**`airflow/dags/osm/dataset_osm_geometry_dag.py`** (daily, `depends_on_past=True`):
production incremental path. Downloads yesterday's OSC changeset
(`omf.osm.osm_osc_collect.OSMOscCollect`) and applies it on top of
yesterday's `geometry_daily` table to produce today's:

```python
# dataset_osm_geometry_dag.py:165-184
generate_daily_geometries = spark_agnostic_task_group(
    ...
    module_name="omf.osm.osm_geometry_osc",
    class_name="OSMGeometryOSC",
    parameters=json.dumps({
        "base_table_path": existing_prev_daily.data_uri,
        "osc_paths": json.dumps([osc_bundle.data_uri]),
        "daily_output_path": output_bundle.data_uri,
        "invalid_geom_output_path": output_bundle.sub_directory("geometry_invalid"),
    }),
)
```

This is where OSM records first get filtered on geometric validity
(invalid geometries are split off to a side path, not silently dropped) and
where OSM's own edit history begins driving Overture record churn — every
downstream OSM-derived transportation feature inherits the identity of an
OSM node/way at this layer. There is also a bespoke one-off DataSync hand-off
of `geometry_daily` to a Meta-internal bucket (`:198-278`) that exists purely
for an external consumer's polling signal — not part of the Overture data
flow proper.

### 1c. US DOT HPMS raw ingest — `airflow/dags/source_us_dot_hpms_collect_dag.py`

One ECS task per US state, each pulling from FHWA's public ArcGIS
FeatureServer:

```python
# source_us_dot_hpms_collect_dag.py:45-49 (comment) and :236-245
# Source: https://geo.dot.gov/server/rest/services/Hosted/HPMS_FULL_{state}_{year}
...
{"name": "BASE_URL", "value": BASE_URL},
{"name": "DATASET_NAME", "value": f"HPMS_FULL_{state}_{{{{ params.year }}}}"},
{"name": "S3_OUTPUT_PATH", "value": f"{RUN_OUTPUT}/data/state={state}/{state}.jsonl"},
```

Raw GeoJSON per state, one row per HPMS "section" (a route subdivided every
time an attribute like AADT/lane-count/speed changes — see the ASCII diagram
at `source_us_dot_hpms_collect_dag.py:26-40`). `objectid` is globally unique;
`(route_id, begin_point)` is also unique. No transform at this stage, just
collection — another black-box external API call (ArcGIS FeatureServer,
opaque server-side logic/pagination).

---

## Stage 2: OSM raw geometry → Overture-shaped OSM data

### 2a. `dataset_osm_ingest_dag.py` — `osm_to_connectors` / `osm_to_segments`

Reads `geometry_daily` and converts the OSM subset relevant to
transportation into partially-hydrated Overture `connector`/`segment`
records, reusing the same job classes the (separate, disconnected) OSM
forge pipeline uses:

```python
# dataset_osm_ingest_dag.py:229-268
osm_to_connectors = spark_agnostic_task_group(
    module_name="overture_transportation.osm_nodes_to_connectors",
    class_name="OSMNodesToConnectors",
    parameters=json.dumps({"input_path": osm_geometry_bundle.data_uri,
                            "output_path": connectors_output, ...}),
)
osm_to_segments = spark_agnostic_task_group(
    module_name="overture_transportation.osm_ways_to_segments",
    class_name="OSMWaysToSegments",
    parameters=json.dumps({"input_path": osm_geometry_bundle.data_uri,
                            "output_path": segments_output,
                            "input_path_connectors": connectors_output, ...}),
)
```

Output: `SourceIngestBundle(provider="osm", resource="osm_in_overture")`.
(See Stage 5-groundwork job details below for what `OSMNodesToConnectors`
and `OSMWaysToSegments` actually do to the data — identical code path is
also used inside the Orbis DAG for the OSM adjudicated side.)

### 2b. OSM tag/geometry adjudication — `airflow/dags/osm/dataset_osm_adjudicator_dag.py`

**This is a violation-store read/write and is worth flagging explicitly.**
On non-reset days, this DAG runs `omf.adjudicator.osm_adjudicator.OSMAdjudicator`,
which reads from **managed Iceberg violation and fast-forward tables**
(`get_entity_violation_table()`, `get_entity_fast_forward_table()`,
`dataset_osm_adjudicator_dag.py:31-34, 204-225`) and replaces any OSM
feature currently flagged with a "critical" violation
(`CRITICAL_VIOLATION_NAMES`, `:52-68` — e.g.
`suspicious_name_changes`, `relation_broken_geometry`,
`important_feature_geometry`) with that feature's version *as it stood at
the last sprint reset*, rather than today's edited version:

```python
# dataset_osm_adjudicator_dag.py:204-225
adjudicator_run = spark_agnostic_task_group(
    module_name="omf.adjudicator.osm_adjudicator",
    class_name="OSMAdjudicator",
    iceberg_bucket="{{ var.value.managed_bucket_violations }}",
    parameters=json.dumps({
        "entity_violations_table": get_entity_violation_table(),
        "entity_fast_forward_table": get_entity_fast_forward_table(),
        "osm_geometry_path": osm_geometry_reset.data_uri,
        "osm_in_overture_path": ingest_today.data_uri,
        "critical_violations": ",".join(CRITICAL_VIOLATION_NAMES),
        ...
    }),
)
```

`OSMAdjudicatorMerge` then does a per-theme/type merge of "reset version of
violated features" + "today's version of clean features"
(`:227-248`). This means: **a transportation segment/connector's content on
a given day can silently be a stale (sprint-reset) copy rather than today's
OSM edit**, if OSM's own edit tripped a QA rule. The adjudicator's own
decisions are logged (`id`, `version_at_reset`, `violations_at_reset`) to a
CSV report and Slack (`:251-315, 601-605`), but the actual merge logic
(`omf.adjudicator.osm_adjudicator.OSMAdjudicator` /
`OSMAdjudicatorMerge`) lives in the separate `omf` package and was not read
in depth here — it is a black box relative to this trace and a prime
candidate for provenance instrumentation, since it is a point where record
content is substituted based on an external violation-tracking system
rather than a transform of the immediate input.

On sprint-reset days this DAG skips adjudication entirely and just format-
copies (`omf.adjudicator.reset_copy_formats.ResetCopyFormats`,
`:320-343`).

Output: `SourceIngestBundle(provider="osm", resource="overture_rc")` — this
is the "adjudicated OSM data" that the Orbis DAG later reads as its tag
source (`osm_adjudicated_bundle` in `theme_transportation_orbis_dag.py:243-257`).

---

## Stage 3: HPMS ingest and pre-match — `airflow/dags/source_us_dot_hpms_ingest_dag.py`

Two steps:

1. `hpms_to_segments` (`overture_transportation.hpms_to_segments.HpmsToSegments`)
   converts raw per-state GeoJSON into Overture segment parquet, one HPMS
   section → one segment, no reconstruction needed since each HPMS feature
   already carries its own LineString (`source_us_dot_hpms_ingest_dag.py:187-205`,
   design notes at `source_us_dot_hpms_collect_dag.py:51-97`).
2. `hpms_match` (`overture_transportation.match_layer_to_network_v2.MatchLayerToNetworkV2`,
   spatial-only mode) matches HPMS segments against the **Orbis network from
   the latest successful production run** of the transportation theme:

```python
# source_us_dot_hpms_ingest_dag.py:40-42, 208-230
ORBIS_BUCKET = _scratch_bucket.replace("-dev", "-prod")
ORBIS_PREFIX = "theme_stage/theme=transportation"
...
hpms_match = spark_agnostic_task_group(
    module_name="overture_transportation.match_layer_to_network_v2",
    class_name="MatchLayerToNetworkV2",
    parameters=json.dumps({
        "input_layer_path": output_bundle.data_uri,
        "input_network_path": ORBIS_RUN + "/orbis_segments/",
        "spatial_only": "true",
        "spatial_buffer_m": "15.0",
        "passthrough_columns": "speed_limits,road_flags,routes,road_surface",
    }),
)
```

**Notable provenance wrinkle**: this pins HPMS matching to whatever
production Orbis run is currently live (resolved via `resolve_orbis_run_path`,
`:65-93`, which lists S3 `run=` partitions and picks the latest one with a
`success` file) rather than to the specific run of `theme_transportation_orbis_dag`
that will ultimately consume the match output. The DAG param `orbis_run_path`
lets an operator override this, but by default HPMS enrichment is matched
against a *different pipeline run's* network than the one it will be merged
into later (Stage 5, `merge_attributes`) — a cross-run dependency that would
be invisible without reading this file.

---

## Stage 4: OSM-only resegmentation forge (disconnected experimental pipeline) — `airflow/dags/theme_transportation_osm_forge_dag.py`

**This DAG is not triggered by, and does not feed, any other DAG in the
repository** — confirmed by searching for references to its output paths,
its job classes (`OSMSegmentMergeGroups`, `OSMMergeSegmentsIterative`,
`OSMMatchSegments`), and its own DAG id anywhere else in `airflow/dags/`;
none exist. `schedule=None` and its docstring says "trigger manually." It
is presented here because it is the one place in the whole pipeline that
performs full OSM-way **splitting and merging into new segments with
production-ID reconciliation** — exactly the resegmentation/identity
problem this research is scoped to document, even though it does not
currently feed the production transportation theme_stage output.

Pipeline (`theme_transportation_osm_forge_dag.py:305-314`):

```
osm_nodes_to_connectors → osm_ways_to_segments ─┐
                        └→ osm_nodes_lr ─────────┴→ osm_segment_split_points
                                                        → osm_split_segments
                                                        → osm_segment_merge_points
                                                        → osm_segment_merge_groups (GraphFrames connected components)
                                                        → osm_merge_segments (iterative, max 2 passes)
                                                        → osm_match_segments (vs. last Overture release, "for ID stability")
```

Key stages:

- `osm_nodes_lr` — computes linear-referencing (LR) position of every
  connector along its parent way, output feeds both split-point and
  merge-point detection (`:150-169`).
- `osm_segment_split_points` / `osm_split_segments` — identifies where a way
  needs to be cut (e.g. a connector appears mid-way) and performs the actual
  split using the LR positions (`:171-210`).
- `osm_segment_merge_points` / `osm_segment_merge_groups` / `osm_merge_segments` —
  the merge side: candidate merge points are found, then connected via
  **GraphFrames connected-components** to group chains of segments that
  should collapse into one (`osm_segment_merge_groups` pulls in the
  `graphframes-py` package and a dedicated Spark checkpoint dir,
  `:233-257`), then `OSMMergeSegmentsIterative` performs up to 2 iterative
  merge passes (`:260-279`).
- `osm_match_segments` — the identity-stability step: matches the newly
  split/merged segment set against **the actual last-published Overture
  release** parquet on `overturemaps-us-west-2`:

```python
# theme_transportation_osm_forge_dag.py:281-303
osm_match_segments = spark_agnostic_task_group(
    module_name="overture_transportation.osm_match_segments",
    class_name="OSMMatchSegments",
    parameters=json.dumps({
        "input_path_new_segments": osm_merge_segments_path,
        "input_path_prod_segments": f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE_DATE}/theme=transportation/type=segment/*.parquet",
        "output_path_match_new_buffered": ...,
        "output_path_match_prod_buffered": ...,
        "output_path_match_join": ...,
        "output_path_match_score": ...,
        "output_path_match_rank": ...,
        "output_path_match": osm_match_segments_path,
    }),
)
```

Five distinct intermediate outputs (buffered-new, buffered-prod, join,
score, rank) before a final match table — a multi-stage spatial
best-match-selection pipeline, i.e. the resegmented graph is reconciled
against yesterday's published IDs so that a segment that didn't
conceptually change keeps its ID even though it was mechanically
re-split/re-merged. `OVERTURE_RELEASE_DATE` is presently hardcoded
(`:39-41`, with a `TODO` to source it from STAC).

(Job-level detail on all nine `overture_transportation` classes in this
pipeline is in the Group B appendix section below.)

---

## Stage 5: Production Orbis pipeline — `airflow/dags/theme_transportation_orbis_dag.py`

This is the theme-specific pipeline that actually produces
`theme_stage/theme=transportation/...` (the `ThemeAssembleBundle` that
`theme_promote_dag` consumes). It runs twice a week and has its own
in-file ASCII data-flow diagram (`theme_transportation_orbis_dag.py:73-121`).
Output bundle: `ThemeAssembleBundle(theme="transportation", run=RUN_ID)`
(`:190`).

### Inputs resolved up front

- `orbis_bundle` — latest (or overridden) TomTom Orbis PBF from Stage 1a.
- `osm_adjudicated_bundle` — the adjudicated OSM `overture_rc` bundle from
  Stage 2b, pinned to a specific date (`AdjudicatorDS` param, defaulting to
  "most recent Saturday" / sprint end) (`:206-257`).
- `osm_geometry_daily` — raw OSM geometry for that same date, used only to
  regenerate relation members (Stage 5, Phase 3) (`:356-365`).
- `hpms_bundle` — latest HPMS `matched` output from Stage 3 (`:124, 277-279`).

### Phase 0 — PBF → parquet

`convert_ot_wrl_pbf_to_parquet` (ECS task, `submit_pbf_parquet_task`,
`:30-60, 338-353`) converts the raw Orbis `.osm.pbf` into parquet. No
Overture semantics applied yet — this is the boundary between "raw
third-party binary" and "queryable rows."

### Phase 1 — base network from Orbis

```python
# theme_transportation_orbis_dag.py:404-423
orbis_ways_to_segments = spark_agnostic_task_group(
    module_name="overture_transportation.orbis_ways_to_segments",
    class_name="OrbisWaysToSegments",
    parameters=json.dumps({
        "input_path_orbis": ot_wrl_parquet_path,
        "output_path": orbis_segments_path,
        "output_path_geometry": orbis_ways_with_geometry_path,
        "source_dataset": "orbis",
    }),
)
```

and `orbis_nodes_to_connectors` (`OrbisNodesToConnectors`, `:383-402`), which
also takes `adjudicated_connectors_path` as input — i.e. Orbis-derived
connectors are cross-referenced against adjudicated OSM connectors even at
this early stage. (See appendix for the actual per-record logic in these
two job files.)

**Identity finding**: both jobs assign `id = tags.gers_identifier` /
`node_tags.gers_identifier` — i.e. the Overture GERS identifier for the
*entire Orbis line* is not computed anywhere in this codebase. It arrives
**pre-baked into TomTom's OSM-PBF export tags**. TomTom's own conversion
process (a black box relative to this repo) is where Orbis-sourced segment
and connector identity actually originates; `OrbisWaysToSegments` /
`OrbisNodesToConnectors` merely read it off the `gers_identifier` tag. This
is a sharp contrast with the OSM-forge line (Stage 4 above), where
identity is freshly minted (`uuid()`) at the equivalent step and only
reconciled against production IDs at the very end.

### Phase 2 — apply adjudicated OSM tags onto the Orbis network

Three-step tag transplant, all using the shared matcher
(`overture_transportation.match_layer_to_network_v2.MatchLayerToNetworkV2`):

```python
# theme_transportation_orbis_dag.py:450-494
adjudicated_tags_match = spark_agnostic_task_group(
    module_name="overture_transportation.match_layer_to_network_v2",
    class_name="MatchLayerToNetworkV2",
    parameters=json.dumps({
        "input_layer_path": adjudicated_tags_normalized_path,
        "input_network_path": orbis_segments_path,
        "geometry_column": "geometry",
        "enable_gap_fill": "true",
    }),
)
...
adjudicated_tags_apply = spark_agnostic_task_group(
    module_name="overture_transportation.adjudicated_tags_apply",
    class_name="AdjudicatedTagsApply",
    parameters=json.dumps({
        "input_path_network_segments": orbis_segments_path,
        "input_path_adjudicated_matches": adjudicated_tags_matched_path,
        "input_path_adjudicated_normalized": adjudicated_tags_normalized_path,
        "confidence_threshold": "0.95",
        "gap_fill_confidence_threshold": "0.80",
    }),
)
```

`AdjudicatedTagsNormalize` prepares OSM tags for matching;
`MatchLayerToNetworkV2` spatially (and by ID) matches each adjudicated OSM
segment onto exactly the Orbis segment(s) it overlaps, scoring the match;
`AdjudicatedTagsApply` only transplants tags where match confidence ≥ 0.95
(or ≥ 0.80 for "gap fill" matches). **This is the core cross-source identity
join of the whole pipeline**: an Orbis-identified segment's final tags can
originate from a *different, OSM-identified* feature, joined purely by
geometry/confidence score, not by any shared source ID. A TODO in the DAG
(`:445-449`) notes connector/point-feature matching isn't handled yet.

### Phase 3 — relations (routes, destinations, turn restrictions)

`RelationsRemappable` extracts OSM relation members from raw
`geometry_daily` (`:366-381`); `RelationsNormalize` flattens them
(`:518-535`); the same `MatchLayerToNetworkV2` job matches relation members
onto the Orbis network a second time, with relation-specific passthrough
columns and an explicit note that **the same way can appear in multiple
turn restrictions**, so uniqueness is redefined for this call:

```python
# theme_transportation_orbis_dag.py:538-564
relations_match_to_network = spark_agnostic_task_group(
    module_name="overture_transportation.match_layer_to_network_v2",
    class_name="MatchLayerToNetworkV2",
    parameters=json.dumps({
        "input_layer_path": relations_normalized_path,
        "input_network_path": orbis_segments_path,
        "input_network_connectors_path": orbis_connectors_path,
        "geometry_column": "member_geometry",
        "ignore_inconsistent_layer_geometries": "true",
        "passthrough_columns": "relation_id,relation_version,relation_type,member_idx,member_type,member_ref,member_role,relation_tags",
        # Additional columns beyond record_id to define unique records
        # (same way can appear in multiple turn restrictions)
        "additional_unique_columns": "relation_id,member_idx",
    }),
)
```

`RelationsPrecombobulate` is then reused three times via an `output_type`
switch to produce `routes`, `destinations` (derived from segment tags, not
matched relations), and `turn_restrictions` (`:567-633`) — three distinct
record types manufactured from the same matched-relations input, each with
its own confidence threshold (0.9 / n/a / 0.8).

### Phase 4 — combobulation (raw tags → structured Overture properties)

```python
# theme_transportation_orbis_dag.py:636-659
orbis_combobulate = spark_agnostic_task_group(
    module_name="overture_transportation.combobulate_segments",
    class_name="CombobulateSegments",
    parameters=json.dumps({
        "input_segments_path": orbis_precombobulated_segments_path,
        "input_relations_path": relations_routes_path,
        "input_destinations_path": relations_destinations_path,
        "input_turn_restrictions_path": relations_turn_restrictions_path,
        "output_path": orbis_combobulated_segments_path,
        "mode": "full",
    }),
)
```

This is the single point where every upstream line (Orbis geometry +
adjudicated OSM tags + relation-derived routes/destinations/turn
restrictions) converges into one segment record with final Overture
`class`, `subtype`, `access_restrictions`, `speed_limits`, `prohibited_transitions`,
etc. (see appendix for `CombobulateSegments`/`combobulation_driver` detail).

### Phase 4b — HPMS gap-fill (currently a dead end)

```python
# theme_transportation_orbis_dag.py:664-685, 1009-1010
merge_attributes = spark_agnostic_task_group(
    module_name="overture_transportation.merge_attributes",
    class_name="MergeAttributes",
    parameters=json.dumps({
        "match_results_path": hpms_bundle.sub_directory("matched"),
        "combobulated_segments_path": orbis_combobulated_segments_path,
        "output_path": orbis_enriched_segments_path,
        "confidence_threshold": "0.9",
        "attributes": "speed_limits,road_surface",
        "changed_only": "true",
    }),
)
# comment in DAG: "HPMS ATTRIBUTE MERGE (post-combobulation gap-fill)"
chain([orbis_combobulate, resolve_hpms], merge_attributes)
```

**Finding**: `orbis_enriched_segments_path` (the output of `merge_attributes`)
is not referenced by any downstream task in this DAG — not by
`transportation_staging_segment`, not by QA, not by finalize. The DAG's own
comment calls this phase "experimental, runs in parallel with staging."
HPMS-sourced speed-limit/surface gap-fill is computed every run but
**silently discarded** before reaching the theme_stage output. Anyone
tracing "where did this segment's speed_limit come from" by reading schema
alone would wrongly assume HPMS is a contributing source.

### Phase 5 — QA issue tables, source sidecars, staging output

```python
# theme_transportation_orbis_dag.py:711-747
segments_issue_detail = spark_agnostic_task_group(
    module_name="overture_transportation.transportation_qa",
    class_name="TransportationQA",
    parameters=json.dumps({"input_path": orbis_combobulated_segments_path,
                            "output_path": segments_issue_detail_path,
                            "output_type": "detail"}),
)
```

`TransportationQA` writes per-run issue detail/overview tables
**scoped to this bundle's own `segments_issue_detail`/`segments_issue_overview`
subdirectories** — this is a local, per-run QA output, not a write to the
shared/central Iceberg violation store used by the OSM adjudicator (Stage
2b). Worth flagging as a second, structurally different "issue" concept in
the same pipeline.

`TransportationStaging` is reused across four `output_type`s
(`source_tags`, `source_tags_connector`, `sources_lr`, `connector`,
`segment` — five total) to produce: a segment-level source-attribution
sidecar, a connector-level source-attribution sidecar, a linear-referenced
sources table, and finally the actual staged `connector`/`segment` output
written to `output_bundle.data_uri + "/theme=transportation/type=connector|segment"`
— this **is** the `theme_stage` bundle content that Stage 0c
(`theme_promote_dag`) later reads (`:812-849`).

`staging_metrics` (shared `overture_cdp` metrics framework) and
`staging_summary` (`TransportationStagingSummary`, generates a markdown
report) run after staging (`:851-884`).

### Phase 6 — 13 validation rule checks (run after staging, non-blocking)

```python
# theme_transportation_orbis_dag.py:142-182, 888-964
def create_validation_task_group(rule_name, module_name, class_name, ...):
    return spark_agnostic_task_group(
        module_name=module_name, class_name=class_name,
        parameters=json.dumps({
            "input_path": output_bundle.data_uri + "/theme=transportation",
            "output_path": output_bundle.sub_directory(f"violations/{rule_name}"),
        }),
    )
```

Thirteen rule classes under `overture_transportation/validations/` (tunnel
level, bridge level, motorway speed limit, motorway-link minimum speed,
speed-limit-value rule, duplicate name, duplicate common names,
street/route name invalid character, access-restriction conflict,
superfluous speed limit, minimum segment length, paved-road-class/surface,
max-speed plausibility) each read the **final staged** transportation data
and write to their own `violations/{rule_name}` subdirectory inside this
run's bundle (`:1044-1063`) — again bundle-local, not the shared Iceberg
violation store. They run after `staging_complete` but do **not** gate
`finalize_bundle` — a segment can fail every validation rule and still ship
in `theme_stage`.

`finalize_bundle` (`output_bundle.finalize_tg()`, `:886`) stamps
`metadata.json` and writes the `success` marker that `theme_promote_dag`'s
`resolve_input_partitions` looks for.

---

## Appendix: per-job transform detail (`overture_transportation/overture_transportation/*.py`)

Direct source reads of every Spark job class named above. All jobs subclass
`overture_spark.job.SparkSedonaJob` and implement `execute_job()`. Group A =
production Orbis chain (Stage 5); Group B = the disconnected OSM
resegmentation forge (Stage 4).

### Group A — Orbis production chain

**1. `orbis_ways_to_segments.py` — `OrbisWaysToSegments`**

```python
ways_multi_node = ways_with_node_arrays.filter(F.size(F.col("ext_nodes")) > 1)
ways_with_geom_obj = ways_multi_node.withColumn(
    "geometry_obj",
    F.expr("ST_MakeLine(transform(ext_nodes, node -> ST_Point(node.lon, node.lat)))"),
)
```

Reads Orbis `ways`+`nodes` parquet, filters to transportation ways
(highway/railway/ferry), explodes each way's `nds` ref array with position,
joins to node coordinates, and builds a LINESTRING via `ST_MakeLine` +
`ST_RemoveRepeatedPoints`. Merges "paged" OSM step tags
(`osm_identifier:step#1#`, `#2#`, …) into one tag by stripping the page
suffix and concatenating by base key, then parses `osm_identifier`/
`osm_identifier:step` into `ext_source_ids` (start_cm/end_cm ranges per OSM
way), explodes those into one row per source range, and builds Overture
`sources[]` structs (OSM `w<id>@0` when an identifier is known, else a
null-record_id TomTom fallback), re-aggregating back to one row per
segment. `id = tags.gers_identifier` (see identity finding above — assigned
upstream by TomTom, not here). Output keeps `ext_nodes` (not yet a
`connectors[]` array — that's built in `precombobulate_segments.py`).
No raw SQL, no violation writes, no ML libraries.

**2. `orbis_nodes_to_connectors.py` — `OrbisNodesToConnectors`**

```python
connecting_nodes = referenced_nodes.groupBy("nd", "is_start_node", "is_end_node") \
    .agg(F.count("*").alias("degree")) \
    .filter((F.col("degree") > 1) | F.col("is_start_node") | F.col("is_end_node"))
```

Explodes way `nds` to compute node degree and endpoint-ness; keeps nodes
with degree > 1 or that are way endpoints. Left-joins a normalized
"adjudicated connectors" table (extracts OSM node id from the first
`n...`-prefixed source) by `osm_identifier`, so a crosswalk node
(`gers_identifier` present + adjudicated `highway=crossing`) is kept even
if it isn't a geometric intersection. `id = node_tags.gers_identifier`.
Sources prefer the adjudicated `sources[]` array (augmented with
`license`/`provider="osm"`/`resource="planet"`/`version`), else a
constructed `n<id>@0` fallback, else a null-record_id placeholder. No raw
SQL, no violation writes, no ML libraries.

**3. `adjudicated_tags_normalize.py` — `AdjudicatedTagsNormalize`**

```python
osm_way_count = F.size(osm_way_sources)
validated_df = adjudicated_df.withColumn("_validation", F.when(
    osm_way_count > 1,
    F.raise_error(F.concat(F.lit("Adjudicated segment has "), osm_way_count.cast("string"),
                            F.lit(" OSM way sources, expected exactly 1 (1:1 relationship)"))),
))
```

Strips the `@version` suffix from each adjudicated segment's OSM way
`record_id` to produce a join key, and enforces — lazily, via
`F.raise_error` evaluated at write time — a 1:1 relationship between an
adjudicated segment and its single OSM way source. Derives `subtype`/
`class` from `source_tags`. No IDs generated; purely a matching-key
normalization. No raw SQL, no violation writes.

**4. `match_layer_to_network_v2.py` — `MatchLayerToNetworkV2`** (subclasses
`match_layer_to_network.py`'s `MatchLayerToNetwork`; invoked three times in
the DAGs — adjudicated-tag matching, relation-member matching, and HPMS
matching)

Three-phase pipeline inherited/overridden from the base class:
- **Phase 1 — ID matching**: explodes network `sources[]`, strips
  `@version`, inner-joins the layer's `record_id` directly onto network
  `record_id`. Exact hits get `confidence=1.0`, `match_type="id"`.
- **Phase 2 — Spatial matching (V2 override)**: candidate generation via
  linestring alignment/coverage filtering (inherited from v1), then ML
  feature computation — SQL-derived features, **rapidfuzz** name
  similarity (`ratio`/`token_sort_ratio`), Sedona geometry-UDF features
  (Hausdorff distance, buffer IoU, heading delta) — then scored by a
  **baked-in pre-trained XGBoost model**:

```python
booster = xgb.Booster(); booster.load_model(bytearray(model_json.encode("utf-8")))
@F.pandas_udf(DoubleType())
def _predict_proba(*feature_cols):
    ...
    return pd.Series(bst.predict(xgb.DMatrix(X, feature_names=features)))
sufficient_matches = scored.filter(F.col("ml_confidence") >= MIN_MATCH_CONFIDENCE)  # 0.4
```

  Per-sequence confidence is the **average ML confidence** across matched
  segments in that sequence. v1 (non-V2, not otherwise used in this
  pipeline) used a hand-tuned weighted formula instead (25% buffer IoU
  15m / 25% coverage / 20% bearing / 15% buffer IoU 5m / 15% Hausdorff).
- **Phase 3 — Gap-fill** (optional): spatially matches *all* layer records
  (not just previously-unmatched ones) to cover gaps from ID churn between
  OSM snapshots; gap-fill matches get a lower confidence threshold applied
  downstream (0.60 vs 0.95 in `AdjudicatedTagsApply`).

**Black-box flag**: the model artifact (`non_topological_model.json` +
manifest) is trained in an entirely separate repository
(`brad-richardson/matcher`) and just copied in as static JSON — its
training data/method is invisible from this codebase; a match's confidence
score is opaque model output, not a transform you can read off the Spark
plan. rapidfuzz is a second black-box dependency for the name-similarity
feature. No raw SQL, no violation-store writes.

**5. `adjudicated_tags_apply.py` — `AdjudicatedTagsApply`** (the core
cross-source identity/tag join)

```python
regular_matches = matches_df.filter(
    (F.col("match_type") != "gap_fill") & (F.col("confidence") >= confidence_threshold))  # 0.95
gap_fill_matches = matches_df.filter(
    (F.col("match_type") == "gap_fill") & (F.col("confidence") >= gap_fill_confidence_threshold))  # 0.60
```

Joins network segments to match results and to normalized adjudicated
segments to pull `source_tags`, gated by the confidence thresholds above.
Classifies each segment into `tag_apply_mode`: UNMATCHED (fallback tags
from subtype/class), SIMPLE (single source, full coverage, tags applied
directly), or COMPLEX (partial coverage / multiple sources → `key:step`
range-encoded tags, e.g. `"0-10000#Main St;10000#Main St"`). Network
(TomTom) transport classification (`highway`/`railway`/`route`) always
wins over adjudicated OSM classification when both exist, to prevent
subtype/class mismatches. Crucially, **`_update_sources_from_adjudicated`
rewrites each segment's `sources[]` array in place**: for every source
whose stripped `record_id` matches an adjudicated lookup, the *adjudicated*
`record_id@version` and `update_time` replace the network's original
values — so a segment's provenance metadata can point at a different
source record than the one that originally produced its geometry. Also
validates (`F.raise_error`) that a segment with multiple matched sources
never has a source with a null `between_cm` range (would make step
formatting ambiguous). No raw SQL, no violation-store writes.

**6. `precombobulate_segments.py` — `PrecombobulateSegments`**

```sql
SELECT ext_tomtom_way_id, segment_length_cm, end_node AS ext_node,
    SUM(length_cm) OVER (
        PARTITION BY ext_tomtom_way_id ORDER BY end_node.idx
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / segment_length_cm AS connected_at
FROM subsegments
```

Computes each segment's subsegments between consecutive `ext_nodes`,
sums subsegment lengths via a window function to get each connector's
fractional position along the segment, snaps that position to 0.0/1.0
within a small threshold, and groups connectors per segment (sorted by
position) — **this is where the `connectors[]` array used by the Overture
segment schema is actually built**, from the raw `ext_nodes` produced in
job #1. No splitting or merging of segments happens here — Orbis segments
arrive already pre-cut by TomTom; this job only attaches connector
positions. **Flag**: the most SQL-heavy file in Group A — six-plus raw
`spark.sql()` blocks over registered temp views
(`segments_raw`, `subsegments`, `nodes_connected_at`,
`nodes_connected_at_that_are_connectors`, `segment_connectors_grouped`,
`orbis_overture_source_joined`). No violation writes, no ML libraries.

**7. `relations_remappable.py` — `RelationsRemappable`**

Filters raw OSM `geometry_daily` to `type=relation` rows matching
connectivity/destination_sign/`route=road`/restriction, explodes each
relation's `members[]` (order-preserving), pre-filters the full OSM
node/way table down to only referenced IDs (comment: "reduces join input
size from billions of OSM elements") and deliberately avoids forcing a
broadcast hint on that lookup ("can exceed driver limits" — a documented
OOM mitigation, not a risk). Regroups members back into an ordered array
per relation, with a `missing_geometry_count`/`missing_refs` debug map
inline in `ext_debug` (not a separate violation-store write). No raw SQL.

**8. `relations_normalize.py` — `RelationsNormalize`**

```python
return df.withColumn("record_id", F.concat(
    F.when(F.col("member_type") == "way", F.lit("w"))
     .when(F.col("member_type") == "node", F.lit("n")).otherwise(F.lit("unknown")),
    F.col("member_ref").cast(StringType()),
))
```

Re-filters/tags each relation by type, `posexplode`s members again into
one row per member with full relation context replicated, and synthesizes
OSM-style `record_id`s (`w123`, `n456`, unversioned) as the join key into
`MatchLayerToNetworkV2`. Mandatory (`only_*`) turn restrictions are *not*
expanded here — that happens downstream in `relations_precombobulate.py`
once members are matched to network segments. No raw SQL, no violation
writes.

**9. `relations_precombobulate.py` — `RelationsPrecombobulate`** (~2,800
lines — largest file in the package; three modes via `output_type`)

- **`routes`**: joins matched relation members (confidence ≥ 0.9 default)
  against network segments and builds each relation's member sequence into
  an ordered `sequence[]` of `{segment_id, connector_id, start_at, end_at,
  member}` for the `routes` combobulator property.
- **`destinations`**: purely tag-based, no relation matching — filters
  segments carrying `destination*` OSM tags, explodes `connectors[]`, and
  builds `destination_candidates[]` for the adjoining downstream segment.
- **`turn_restrictions`**: handles a 2×2 matrix of prohibitory (`no_*`) vs
  mandatory (`only_*`), simple (VIA=node) vs complex (VIA=way).
  **Mandatory restrictions are algorithmically expanded into equivalent
  prohibitory restrictions** via a Python UDF
  (`turn_restriction_lib.mandatory_expansion.expand_mandatory_restriction`
  — e.g. "only straight" becomes "no left" + "no right"), then both cases
  build `sequence[]` chains of `{connector_id, segment_id,
  segment_geometry, connector_geometry, connected_at, role}` for the
  `ProhibitedTransition` schema. Segments whose VIA connector can't be
  spatially resolved within ~1.1km are flagged with a sentinel score
  (`VIA_INVALID_SEGMENT_SCORE = 999.0`) and filtered. No raw SQL, no
  violation writes; the mandatory-expansion UDF is per-relation (bounded
  fan-out).

**10. `combobulate_segments.py` — `CombobulateSegments`** (delegates the
actual tag→property conversion to a Python UDF wrapping
`combobulator/combobulation_driver.py`)

```python
df = df.withColumn("combobulated", pyspark_udf.combobulate_segment_tags_udf(
    F.col("id"), F.col("subtype"), F.col("ext_tomtom_tags"),
    F.col("ext_length_cm") / 100.0, F.col("geometry"),
    F.col("relations"), F.col("ext_source_ids"), F.col("destination_candidates"),
))
```

Reads segments filtered to `subtype in (road, water, rail)`, with a **hard
duplicate-ID invariant check** that raises an `Exception` if any `id`
appears more than once. Left-joins route/turn-restriction relation data
(unioned and grouped by `segment_id` into a sorted `relations[]` array) and
tag-based destination candidates, then calls the single per-row UDF that
converts raw OSM/TomTom tags into every structured Overture property:
names, class/subclass, road surface, rail flags, width rules, access
restrictions, speed limits, prohibited transitions, routes, destinations.
**This is the single point where the Orbis geometry line, the adjudicated
OSM tag line, and all three relation-derived lines converge into one final
segment record.** Two raw `spark.sql()` reads exist for a legacy
Hive-table input path (`_read_relation_data`, `_read_turn_restriction_data`,
`_read_destinations`) but aren't used when S3 paths are supplied (the
production DAG always supplies S3 paths). **Flag**: the combobulation UDF
itself is an opaque per-row Python UDF wrapping a large rules library
(`combobulib/`) — none of its internal logic is visible in the Spark
query plan, making it a natural instrumentation point for provenance.

**11. `merge_attributes.py` — `MergeAttributes`** (HPMS gap-fill, currently
a dead end per Stage 5 Phase 4b above)

```python
def _subtract_intervals(start, end, covered):
    result = [(start, end)]
    for cs, ce in sorted(covered):
        ...  # standard interval-subtraction
    return result
```

Takes spatially-matched HPMS candidate values (`speed_limits`,
`road_surface`) and merges them into combobulated segments **only where
OSM coverage is missing** — existing values always win. Validates
class-appropriate speed ranges and blocks certain surface values on
motorway/trunk, computes existing coverage as `between`-fraction ranges,
subtracts already-covered ranges from each candidate (interval subtraction
via a Python UDF) so only true gaps get filled, and for speed limits
additionally rejects HPMS values disagreeing with existing OSM values
beyond tolerance (min of 30% relative / 30 kph absolute). Additive
(`_merge_array` concatenates, never replaces) — not a destructive drop.
No raw SQL, no violation writes.

**12. `transportation_qa.py` — `TransportationQA`** — **violation/issue
writer** (flagged per the task's explicit ask)

```sql
SELECT CASE
    WHEN geom IS NULL THEN 'geometry: invalid: insufficient points'
    WHEN NOT ST_IsValid(geom) THEN CONCAT('geometry: invalid: ', ...)
    ...
    ELSE 'OK' END AS category
FROM with_geom
```

`output_type="detail"` counts records by category × the combobulator's own
`ext_combobulator_issues` list. `output_type="overview"` runs a **raw
`spark.sql()`** geometry-validation pass (`ST_IsValid`/`ST_IsValidReason`)
bucketing every segment into invalid-geometry / missing-class / has-issues
/ OK. This is a per-run QA ledger distinct from both the 13 rule-based
`validations/*.py` writers (below) and the central Iceberg violation store
used by the OSM adjudicator (Stage 2b) — three structurally different
"violation" concepts coexist in this one pipeline.

**13. `transportation_staging.py` — `TransportationStaging`** (5
`output_type` branches; this is the job that writes the actual `theme_stage`
output)

- `source_tags` / `source_tags_connector`: pass-through projections of
  pre-computed tag-rule sidecars.
- `sources_lr` (**raw SQL**): joins exploded `sources[]` against
  `ext_source_ids`, computes linear-referenced `between = [start_cm/length,
  end_cm/length]`, snaps to 1.0 within a threshold, collapses full-coverage
  `[0.0, 1.0]` to `NULL`, and stamps `version` per source provider
  (`osm_version` for OSM, `orbis_version` for TomTom) — both version
  strings are validated against a stray `'` before f-string interpolation
  into SQL, a deliberate injection guard.
- `connector`: trivial projection (`id`, `version=0`, `sources`,
  `geometry`).
- `segment` (**raw SQL**): left-joins combobulated properties with
  `sources_lr`, parses WKT geometry guarded by regex, and applies the hard
  release-quality gate: `(subtype in (road, rail) AND class IS NOT NULL) OR
  subtype = water`, geometry must parse and be non-degenerate (`ST_NumPoints
  > 1`, `ST_Length > 0`, no duplicate consecutive interior points), and
  `SIZE(sources) > 0`. **Segments failing any of these are silently
  dropped from the final release output at this exact step** — this is the
  last-chance filter before `theme_stage`.

**14. `staging_summary.py` — `TransportationStagingSummary`** — reporting
only, not a transform: reads pre-aggregated metrics parquet plus the QA
overview/detail outputs from job #12, and renders a Markdown run summary
(counts, lengths, churn deltas, top-20 issue categories).

### Group B — OSM-only resegmentation forge (identity-critical, disconnected)

**15. `osm_nodes_to_connectors.py` — `OSMNodesToConnectors`**

```python
transformed_df = df.select(
    F.expr("uuid()").alias("id"),
    F.col("geometry"),
    F.array(F.struct(..., F.concat(F.lit("n"), F.col("id").cast("string"),
                                    F.lit("@"), F.col("version").cast("string")).alias("record_id"), ...)),
    F.col("id").alias("ext_osm_id"),
)
```

Keeps a raw OSM node as a connector if it has way-degree ≥ 2
(intersection), OR is the sole endpoint of a `highway=crossing` way, OR is
a way endpoint, OR is a "loop split" point (forced split for a
self-looping way, since a LINESTRING can't legally revisit a point).
**Identity finding**: unlike the Orbis line, `id` here is a **freshly
generated random UUIDv4 on every run** (`F.expr("uuid()")`) — not derived
from the OSM node id. `ext_osm_id` preserves the original OSM id purely for
downstream joins/debugging, not as the record's actual identity. No raw
SQL, no violation writes. (Also used, unmodified, by
`dataset_osm_ingest_dag.py`'s daily transportation ingest — Stage 2a.)

**16. `osm_ways_to_segments.py` — `OSMWaysToSegments`**

```python
.withColumn("has_valid_endpoints",
    (F.size("at_values") >= 2)
    & (F.abs(F.element_at("at_values", 1) - 0.0) < 0.001)
    & (F.abs(F.element_at("at_values", -1) - 1.0) < 0.001))
.select("way_id", F.when(F.col("is_monotonic") & F.col("has_valid_endpoints"), F.col("connectors_raw"))
    .otherwise(F.array()).alias("connectors"))
```

Filters OSM ways to transportation-relevant ones, computes each way's
`connectors[]` by joining `refs` to job #15's connector nodes and computing
a **geodetic** linear-reference fraction per connector
(`ST_LineLocatePoint`/`ST_LineSubstring`/`ST_LengthSpheroid`) — a code
comment flags this as deliberately different from
`precombobulate_segments.py`'s node-summation approach (the Orbis "source
of truth"), with a TODO to unify them. Validates monotonicity and 0.0/1.0
endpoint bounds; invalid sequences get an empty `connectors[]` rather than
a silently-wrong one. **`id = uuid()`, freshly minted here too** — this is
the initial identity for every OSM-forge segment, before split/merge/match
downstream. No raw SQL, no violation writes.

**17. `osm_nodes_lr.py` — `OSMNodesLR`**

For every connector node, computes its position (`at_cm`) along every OSM
way that references it: `0`/full-length shortcuts for first/last node, a
spatial `ST_LineLocatePoint` fallback for topologically-simplified
geometries, or exact `ref_index`-based `ST_PointN` vertex extraction
otherwise. Also runs loop/self-intersection detection
(`detect_invalid_ways`) and joins that flag on as `is_invalid_way`. This
output is the positional backbone for both split-point and merge-point
detection below. No raw SQL, no violation writes; explicit column pruning
to reduce shuffle size is called out in comments as deliberate.

**18. `osm_segment_split_points.py` — `OSMSegmentSplitPoints`**

```python
multi_way_splits = node_1.join(node_2,
    (F.col("node_1.node_id") == F.col("node_2.node_id")) & (F.col("node_1.way_id") < F.col("node_2.way_id")),
    "inner")
```

Finds candidate split points via a self-join of the LR output on
`node_id` (nodes where two different ways meet mid-way, not at an
endpoint), plus a separate path for self-looping ways. Joins candidates
against `osm_segments` to pull each way's road class, and filters to keep
only splits where **both sides' class** falls in
`SEGMENT_SPLIT_ROAD_CLASSES` — i.e. minor paths don't force a split of a
major road and vice versa. This is the rule that decides *where* OSM ways
get cut into Overture-style segments. Equi-join on `node_id`, not a
cartesian — bounded by node degree.

**19. `osm_split_segments.py` — `OSMSplitSegments`** — the concrete split
operation and the sharpest identity-preservation-vs-new-ID line in the
whole pipeline:

```python
processed_pairs = exploded_pairs_df.withColumn(
    "both_original_endpoints",
    F.col("start_connector.is_endpoint") & F.col("end_connector.is_endpoint")
    & ~F.col("start_connector.is_splitting_connector") & ~F.col("end_connector.is_splitting_connector"),
)
result_df = processed_pairs.withColumn(
    "new_segment_id",
    F.when(F.col("both_original_endpoints"), F.col("original_segment_id")).otherwise(F.expr("uuid()")),
)
```

Gathers every connector for a way (from job #18), sorts/dedupes adjacent
same-node connectors, filters to endpoints-plus-split-points, and
pairwise-explodes consecutive connectors — a way with N split points
becomes N+1 candidate subsegments. **If a pair spans the entire original
way (both original endpoints, no split point in between), the original
`id` is kept unchanged. Otherwise a brand-new UUID is minted for the
new sub-segment.** Geometry for each piece is sliced by exact `ref_index`
(`ST_PointN`/`ST_MakeLine`) when unsimplified, or by normalized `at_cm`
ratio (`ST_LineSubstring`) when the source geometry was simplified. Every
output row carries `split_debug_info`/`ext_debug` linking back to
`original_segment_id`, `start_node_id`/`end_node_id`, and `start_at_cm`/
`end_at_cm` — the only trace connecting a new UUID back to the way it was
cut from. No raw SQL, no violation writes.

**20. `osm_segment_merge_points.py` — `OSMSegmentMergePoints`** — the
inverse of splitting: identifies where adjacent same-class split-segments
should be re-merged, because Overture's segmentation is less aggressive
about minor intersections than Orbis's raw network. Excludes any 3+-way
junction node from merge eligibility — a comment notes this rule "wasn't
originally part of Overture's segmentation rules, just Orbis's, and could
be removed if desired" (an inherited legacy policy choice). Self-joins
remaining connectors on `node_id`, scores each candidate pair by length,
bearing/heading delta, and interior angle, and keeps the best-scoring
candidate per node. Equi-join, bounded by node degree.

**21. `osm_segment_merge_groups.py` — `OSMSegmentMergeGroups`** — explicit
black-box: **GraphFrames connected components.**

```python
jvm_graph = graph._impl._jvm_graph
jdf = (jvm_graph.connectedComponents()
       .setAlgorithm("graphframes").setCheckpointInterval(2).setBroadcastThreshold(1000000).run())
```

Builds a graph (vertices = candidate segments, edges = merge-point pairs
from job #20) and finds connected components — a chain of 5 collinear
split-segments becomes one component of size 5. Calls the underlying JVM
API directly rather than the Python wrapper, with a comment explaining
this works around a `graphframes-py` 0.9.3 bug calling a method that
doesn't exist in older Scala JARs — a fragile version-compatibility shim.
Requires a Spark checkpoint directory (raises without one), since
`connectedComponents` checkpoints iteratively.

**22. `osm_merge_segments.py` — `OSMMergeSegmentsIterative`** — **flagged
OOM risk pattern**:

```python
component_groups_df = segments_with_groups_df.groupBy("component").agg(
    F.collect_list("id").alias("segment_ids"),
    F.collect_list("geometry").alias("geometries"),
    F.collect_list("sources").alias("sources_list"),
    F.collect_list("connectors").alias("connectors_list"),
    ...
).join(merge_points_by_component_df, "component", "left")
merged_components_raw_df = component_groups_df.withColumn(
    "merged_segments_array",
    merge_component_udf(F.col("segment_ids"), F.col("sources_list"), ...),
)
...
final_merged_df = merged_components_df.withColumn("id", F.expr("uuid()"))
```

For every connected component, `collect_list`s the *entire component's*
geometries/sources/connectors into per-row arrays, then hands them whole
into a plain (non-vectorized) Python UDF that does an in-memory graph/DFS
traversal — walking the merge-point adjacency starting from
degree-≤1 "endpoint" segments, splitting the walk where merging would
create duplicate connector nodes. **If GraphFrames (job #21) ever produces
an anomalously large connected component — e.g. a bug or degenerate input
chains much of a region's road network into one component — this
`collect_list` + single-UDF-invocation pattern could spike one executor's
memory well past normal row sizes.** This is exactly the kind of "wide
aggregation feeding a black-box UDF" pattern the task asked to watch for,
and plausibly related to the transportation executor-OOM issue this branch
is named after. After the UDF, `ST_LineMerge(ST_Collect(...))` stitches
surviving geometries, and **every merged segment gets a brand-new UUID
regardless of how many originals fed into it** — the old→new mapping
survives only in `ext_debug["merged_segment_ids"]`, not in `id` itself.
Segments with no merge group pass through unmerged with their original id.

**23. `osm_match_segments.py` — `OSMMatchSegments`** — the ID-stability
reconciliation step, and the payoff for all the identity churn above:

```python
initial_candidates = new_with_bbox_df.alias("new").join(
    prod_with_bbox_df.alias("prod"),
    ST_Intersects(F.col("new.buffered_envelope"), F.col("prod.buffered_envelope")), "inner")
...
result_df = new_segments_df.join(id_mapping, F.col("id") == F.col("new_segment_id"), "left").select(
    F.coalesce(F.col("matched_prod_id"), F.col("id")).alias("id"),
    F.col("id").alias("old_id"),
    ...
)
```

Buffers/envelopes both the newly split-and-merged segments and the last
published Overture release's segments, spatial-joins on envelope
intersection only (comment: "any other clauses will kill spatial index
performance"), pre-filters cheaply (centroid distance, length difference)
before the expensive **Fréchet distance** computation
(`ST_FrechetDistance`), and scores candidates on Fréchet distance +
length-ratio + class match + OSM-source-ID overlap + length-difference
penalty into a `match_score` bucketed as HIGH/MED confidence. Only
high-confidence matches get to reassign an ID; 1-to-many conflicts are
resolved by max score with deterministic tie-breaking. **The payoff:**
`id = coalesce(matched_prod_id, id)` — a segment that structurally
corresponds to something in the last release **gets the old published ID
back**, even though it was mechanically re-split/re-merged this run;
anything that doesn't match keeps its freshly-minted UUID. `old_id`
(the pre-match id) is retained on every row as the breadcrumb linking back
through job #22's `merged_segment_ids` and job #19's `split_debug_info`. A
final `_validate_no_duplicate_ids` check raises `RuntimeError` if any `id`
collides post-reassignment. **Flag**: no explicit spatial partitioning
(no `.repartition()` by grid, no broadcast hint) before the `ST_Intersects`
join — correctness/performance depends entirely on Sedona's automatic
join-plan selection.

### Validations framework (`validations/*.py`, 13 rule files)

Each rule (tunnel/bridge level, motorway speed limit, motorway-link min
speed, speed-limit-value, duplicate name, duplicate common names,
street/route name invalid character, access-restriction conflict,
superfluous speed limit, minimum segment length, paved-road-class/surface,
max-speed plausibility) is its own `SparkSedonaJob` subclass with the same
shape: read staged transportation parquet, explode the relevant
array-typed property, apply a rule-specific boolean condition, format
violating rows into a standard schema (`id, version, geometry, dataset,
ds, context, violation_name, violation_count`), and
`write.mode("overwrite").parquet(output_path)`. Some checks subclass a
shared `overture_spark.validations.base_spark_check.SparkCheck`, which
registers input DataFrames as temp views and runs a **caller-supplied raw
SQL `query` string via `self.spark.sql(self.query)`** — the query text
itself lives in the DAG/config layer, not in this repo, so a full
SQL-injection/audit trail would need to follow that call site too. Per the
DAG (`theme_transportation_orbis_dag.py:179`), each rule writes to its own
named subdirectory under a shared `violations/` prefix
(`violations/tunnel_level_violation_check/`, etc.) — a shared *path
namespace*, not a shared table — distinct from both `transportation_qa.py`
(Group A #12) and the central Iceberg violation store used by the OSM
adjudicator (Stage 2b).

---

## Summary: linear stage list, raw source to release

1. **TomTom Orbis raw download** — `theme_transportation_ingest_dag.py` — ECS task calls TomTom MCAPI, downloads `ot_wrl_{YYWW}.osm.pbf` (black-box third-party extract).
2. **OSM planet bootstrap** — `dataset_osm_geometry_reset_dag.py` — downloads OSM planet.pbf, converts to parquet, builds baseline `geometry_planet`.
3. **OSM daily incremental** — `dataset_osm_geometry_dag.py` — downloads daily OSC changeset, applies on top of yesterday's `geometry_daily` (`OSMGeometryOSC`), splits off invalid geometries.
4. **US DOT HPMS raw collect** — `source_us_dot_hpms_collect_dag.py` — per-state ECS pulls from FHWA ArcGIS FeatureServer.
5. **OSM-in-Overture ingest** — `dataset_osm_ingest_dag.py` — `OSMNodesToConnectors`/`OSMWaysToSegments` convert `geometry_daily` into partial Overture transportation records.
6. **OSM adjudication (violation-store read/write)** — `dataset_osm_adjudicator_dag.py` — `omf.adjudicator.osm_adjudicator.OSMAdjudicator` reads Iceberg violation + fast-forward tables, substitutes sprint-reset versions of critical-violation features; produces `overture_rc` bundle.
7. **HPMS ingest + pre-match** — `source_us_dot_hpms_ingest_dag.py` — `HpmsToSegments` then `MatchLayerToNetworkV2` (spatial-only) against latest **production** Orbis network.
8. *(Disconnected, not in production path)* **OSM-only resegmentation forge** — `theme_transportation_osm_forge_dag.py` — nodes→connectors, ways→segments, LR, split (new UUIDs), merge (GraphFrames connected components + new UUIDs), match against last release (`coalesce(matched_prod_id, id)`).
9. **Orbis PBF → parquet** — `theme_transportation_orbis_dag.py` (ECS task).
10. **Orbis base network** — `OrbisWaysToSegments` / `OrbisNodesToConnectors` — `id = gers_identifier` from TomTom's own tags (identity assigned upstream, outside this codebase).
11. **Adjudicated OSM tag transplant** — `AdjudicatedTagsNormalize` → `MatchLayerToNetworkV2` (XGBoost-scored) → `AdjudicatedTagsApply` (confidence-gated tag merge; rewrites `sources[]` provenance in place).
12. **Connector positions** — `PrecombobulateSegments` (raw-SQL-heavy; builds `connectors[]`, no resegmentation).
13. **Relations pipeline** — `RelationsRemappable` → `RelationsNormalize` → `MatchLayerToNetworkV2` → `RelationsPrecombobulate` ×3 (routes / destinations / turn_restrictions, with mandatory→prohibitory expansion).
14. **Combobulation** — `CombobulateSegments` — converges Orbis geometry + adjudicated tags + all three relation outputs into final Overture properties via an opaque per-row UDF.
15. **HPMS gap-fill** — `MergeAttributes` — computed every run, **currently discarded** (not wired to staging).
16. **QA issue tables** — `TransportationQA` — bundle-local violation/issue ledger (raw SQL geometry categorization).
17. **Source sidecars + staging** — `TransportationStaging` — source_tags, source_tags_connector, sources_lr (raw SQL), connector, segment (raw SQL, final quality-gate drop) → this **is** the `theme_stage` bundle.
18. **Per-run metrics + summary** — `staging_metrics` / `TransportationStagingSummary`.
19. **13 validation rule checks** — non-blocking, bundle-local `violations/{rule_name}/` writes.
20. **Bundle finalize** — `output_bundle.finalize_tg()` — writes `success` marker `theme_promote_dag` looks for.
21. **Theme promote (shared)** — `theme_promote_dag.py` — schema validation, changelog/churn, `ProcessDataJob` (bbox filter), PMTiles, bridge files, final validation.
22. **Release candidate assembly (shared)** — `cdp_release_candidate_dag.py` — pure copy of each theme's promote output into one release-candidate bundle.
23. **Release publish (shared)** — `release_publish_dag.py` — DataSync to AWS/Azure/archive, PMTiles copy, STAC publish, Glue crawlers, tag-as-released.

