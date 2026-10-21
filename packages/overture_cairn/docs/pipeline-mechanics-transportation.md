# Transportation Theme Pipeline — Mechanics

Companion to `pipeline-trace-transportation.md`. Mechanical facts only, tagged
by cairn operation vocabulary: drop, merge, split, revision, id change/rebind,
join, new column, column removed, aggregation, black box, write. Same stage
names/order as the source doc.

---

## Stage 0: Shared stages

### 0a. Release Publish — `release_publish_dag.py`

- write: `data/`, `changelog/`, `bridgefiles/`, `registry/` dirs -> production release bucket (AWS), Azure mirror, archive bucket (`:314-326`)
- write: PMTiles via boto3 multipart copy (`:327-332`)
- black box: `PublishStac` (`overture_core.stac.job`), STAC CloudFront invalidation (`:334-353`)
- black box: 4x Glue crawler runs (`:355-376`)
- revision: writes `released`/`released_at` onto release-candidate `metadata.json` via `ReleaseCandidateBundle.tag_as_released_from_uri` (`:378-380`)
- no drop/merge/split of feature records

### 0b. Bridging stage — `cdp_release_candidate_dag.py`

- write: copies each theme's `ThemePromoteBundle` (`data/`, `changelog/`, `bridgefiles/`, `pmtiles/`, `metrics/`) into per-theme subdirs of one `ReleaseCandidateBundle` (`:133-324`)
- no per-record transform

### 0c. Theme Promote — `theme_promote_dag.py` + `theme_promote.py`

- (plumbing) `setup`: `validate_bucket_accessibility` x2 + `validate_input_data_path` — existence/permission checks, no data read (`theme_promote.py:175-191`)
- (plumbing) `discover_input_types`: lists `type=` partitions under the input bundle via S3 `list_objects_v2` (`theme_promote.py:89-103`)
- join: `ComputeInternalChangelogJob` full-outer-joins old vs new theme data on `(id, type)` (`compute_internal_changelog.py:83-176`)
- new column: per-column content hash (`sha2`, geometry via `ST_AsText`) on each side (`compute_internal_changelog.py:83-176`)
- new column: `change_type` = ADDED/REMOVED/DATA_CHANGED/UNCHANGED (`compute_internal_changelog.py:83-176`)
- new column: `version` = `old.version + 1` if hashes differ else `coalesce(old.version, 1)` (`compute_internal_changelog.py:83-176`)
- write: `changelog_df` keyed by `id`; no rows dropped here (`compute_internal_changelog.py:83-176`)
- aggregation: `ComputeChurnJob` groups the changelog by `type`, computes added/removed/data_changed/unchanged counts and percentages (`compute_churn.py:133-142`)
- write: churn stats to CSV/Markdown/Parquet, plus a telemetry write (`compute_churn.py:55-124`)
- drop (job-level, not record-level): `validate_churn_thresholds` reads `changelog_stats.csv`, compares each type's percentages against `THEME_THRESHOLDS` — transportation has tighter `segment`/`connector` thresholds and its own length-diff check — fails the whole run if exceeded (`changelog.py:131-263`)
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
- write: bridge file generation triggers `bridge_file_create_dag` per configured type (`theme_promote.py:264-289`)
- black box: `pmtiles_task` submits an AWS Batch job running the public `ghcr.io/overturemaps/overture-tiles` Docker image; tiling logic itself is not in this repo (`pmtiles.py:25-90`)
- (plumbing) `validate_final_output`: checks S3 for expected output directories (existence check only, no data transform) (`validation.py:151-181`)
- (plumbing) `cleanup_hidden_files`: S3 cleanup of hidden files

---

## Stage 1: Raw source ingest

### 1a. TomTom Orbis raw ingest — `theme_transportation_ingest_dag.py`

- black box: ECS Fargate task calls TomTom MCAPI `get_latest_available_orbis_release` to resolve latest "Overture Transportation"/"WRL" version (`transportation_utils.py:10-70`)
- black box: ECS task `download_tomtom_map_content` downloads OSM-PBF Orbis extract to S3 (`:146-173`)
- write: `SourceRawBundle(provider="tomtom", resource="orbis", version=<YYWW>)` — single `ot_wrl_{YYWW}.osm.pbf` file
- no per-record transform (byte-for-byte extract)

### 1b. OSM raw ingest

**`dataset_osm_geometry_reset_dag.py`** (manual bootstrap):
- black box: downloads OSM planet.pbf from external mirror (`:52-57`), `S3KeySensor` wait
- black box: ECS converts planet.pbf to parquet
- black box: `omf.osm.osm_geometry_planet.OSMGeometryPlanet` builds baseline `geometry_planet` bundle (`:99-196`)
- write: `geometry_planet` bundle
- (parallel) backfills daily OSC changeset files for every day between planet date and yesterday

**`dataset_osm_geometry_dag.py`** (daily, `depends_on_past=True`):
- black box: `omf.osm.osm_osc_collect.OSMOscCollect` downloads yesterday's OSC changeset
- join: `OSMGeometryOSC` applies OSC changeset onto yesterday's `geometry_daily` table -> today's `geometry_daily` (`:165-184`, `base_table_path` + `osc_paths` -> `daily_output_path`)
- drop/split: invalid geometries routed to separate `invalid_geom_output_path` (`geometry_invalid`), not merged into `geometry_daily` (`:156`)
- write: `daily_output_path` = today's `geometry_daily`
- write: bespoke DataSync copy of `geometry_daily` to Meta-internal bucket (`:198-278`)

### 1c. US DOT HPMS raw ingest — `source_us_dot_hpms_collect_dag.py`

- black box: one ECS task per US state, pulls from FHWA ArcGIS FeatureServer `HPMS_FULL_{state}_{year}` (`:45-49, 236-245`)
- write: raw GeoJSON per state -> `.../data/state={state}/{state}.jsonl`
- id note: `objectid` globally unique; `(route_id, begin_point)` also unique (source-native keys, no transform here)
- no transform

---

## Stage 2: OSM raw geometry → Overture-shaped OSM data

### 2a. `dataset_osm_ingest_dag.py`

- black box/job: `OSMNodesToConnectors` reads `geometry_daily` -> connectors (`:229-268`) — mechanics in Appendix job #15
- black box/job: `OSMWaysToSegments` reads `geometry_daily` + connectors output -> segments (`:229-268`) — mechanics in Appendix job #16
- write: `SourceIngestBundle(provider="osm", resource="osm_in_overture")`

### 2b. OSM tag/geometry adjudication — `dataset_osm_adjudicator_dag.py`

- join: `OSMAdjudicator` reads managed Iceberg `entity_violation_table` + `entity_fast_forward_table` (`get_entity_violation_table()`, `get_entity_fast_forward_table()`, `:31-34, 204-225`)
- revision: any OSM feature flagged with a `CRITICAL_VIOLATION_NAMES` violation (`:52-68` — e.g. `suspicious_name_changes`, `relation_broken_geometry`, `important_feature_geometry`) has its content replaced with the version as of the last sprint reset, not today's edit (`:204-225`)
- merge: `OSMAdjudicatorMerge` merges "reset version of violated features" + "today's version of clean features" per theme/type (`:227-248`)
- black box: `OSMAdjudicator`/`OSMAdjudicatorMerge` internals live in separate `omf` package, not inspected in source trace
- write: CSV report + Slack log of `id`, `version_at_reset`, `violations_at_reset` (`:251-315, 601-605`)
- write: `SourceIngestBundle(provider="osm", resource="overture_rc")`
- alt path: on sprint-reset days, adjudication skipped, `omf.adjudicator.reset_copy_formats.ResetCopyFormats` format-copies instead (`:320-343`)

---

## Stage 3: HPMS ingest and pre-match — `source_us_dot_hpms_ingest_dag.py`

- black box/job: `HpmsToSegments` converts raw per-state GeoJSON to Overture segment parquet, 1 HPMS section -> 1 segment, no geometry reconstruction (`:187-205`)
- join: `MatchLayerToNetworkV2` (`spatial_only=true`, `spatial_buffer_m=15.0`) matches HPMS segments against Orbis network from the latest successful **production** run of the transportation theme, resolved via `resolve_orbis_run_path` (lists S3 `run=` partitions, picks latest with a `success` file) (`:65-93, 208-230`)
- new column: `passthrough_columns = speed_limits,road_flags,routes,road_surface` carried onto match output
- note: matched against a different pipeline run's network than the one it is later merged into at Stage 5 Phase 4b (`merge_attributes`); overridable via `orbis_run_path` param

---

## Stage 4: OSM-only resegmentation forge (disconnected) — `theme_transportation_osm_forge_dag.py`

Not triggered by / does not feed any other DAG. `schedule=None`, manual trigger.

Chain (`:305-314`): `osm_nodes_to_connectors` → `osm_ways_to_segments` (+ `osm_nodes_lr`) → `osm_segment_split_points` → `osm_split_segments` → `osm_segment_merge_points` → `osm_segment_merge_groups` → `osm_merge_segments` → `osm_match_segments`

- id change/mint: `osm_nodes_to_connectors` mints `id = uuid()` (Appendix job #15)
- id change/mint: `osm_ways_to_segments` mints `id = uuid()` (Appendix job #16)
- new column: `osm_nodes_lr` computes LR position of every connector along its parent way (`:150-169`, Appendix job #17)
- split: `osm_segment_split_points`/`osm_split_segments` cut a way at mid-way connector points using LR positions (`:171-210`, Appendix jobs #18-19)
- id change/rebind: on split, `new_segment_id = original_segment_id` when both endpoints of the piece are original endpoints; otherwise `uuid()` minted (Appendix job #19)
- merge: `osm_segment_merge_points` finds candidate merge points between adjacent same-class segments (Appendix job #20)
- black box: `osm_segment_merge_groups` — GraphFrames connected components, direct JVM API call, dedicated Spark checkpoint dir, `graphframes-py` dependency (`:233-257`, Appendix job #21)
- merge: connected components group chains of segments into one group, keyed by component id
- black box: `OSMMergeSegmentsIterative` — up to 2 iterative merge passes, non-vectorized Python UDF over `collect_list`ed component arrays (`:260-279`, Appendix job #22)
- id change/mint: every merged segment gets a brand-new `uuid()` regardless of input count; unmerged segments keep original id
- join: `osm_match_segments` spatial-joins new split/merged segment set against last-published Overture release parquet on `overturemaps-us-west-2` (`:281-303`, Appendix job #23)
- write: 5 intermediate outputs — match_new_buffered, match_prod_buffered, match_join, match_score, match_rank — before final match table
- id change/rebind: `id = coalesce(matched_prod_id, id)` — high-confidence match reassigns id to the production id; no match keeps freshly-minted uuid
- note: `OVERTURE_RELEASE_DATE` hardcoded (`:39-41`, TODO to source from STAC)

---

## Stage 5: Production Orbis pipeline — `theme_transportation_orbis_dag.py`

Inputs resolved up front: `orbis_bundle` (Stage 1a), `osm_adjudicated_bundle` (Stage 2b, pinned to a date), `osm_geometry_daily` (same date), `hpms_bundle` (Stage 3) (`:206-257, 356-365, 124, 277-279`).

### Phase 0 — PBF → parquet

- black box: ECS task `convert_ot_wrl_pbf_to_parquet` (`submit_pbf_parquet_task`, `:30-60, 338-353`)

### Phase 1 — base network from Orbis

- job: `OrbisWaysToSegments` — `input_path_orbis` -> `output_path` (orbis_segments_path), `output_path_geometry` (`:404-423`); mechanics in Appendix job #1
- job: `OrbisNodesToConnectors` — also takes `adjudicated_connectors_path` as input (`:383-402`); mechanics in Appendix job #2
- id change/mint (external): `id = tags.gers_identifier` / `node_tags.gers_identifier` — GERS id arrives pre-baked in TomTom's PBF export tags; not computed in this codebase (black box: TomTom's own conversion process) (`:444-453`)

### Phase 2 — apply adjudicated OSM tags onto Orbis network

- job: `AdjudicatedTagsNormalize` strips `@version` from adjudicated segment OSM-way `record_id` to build join key (Appendix job #3)
- join: `MatchLayerToNetworkV2` (`geometry_column=geometry`, `enable_gap_fill=true`) matches adjudicated OSM segments onto Orbis network segments (`:450-471`); internals black-box XGBoost scoring, Appendix job #4
- revision: `AdjudicatedTagsApply` transplants tags where `confidence >= 0.95` (regular) or `>= 0.80` (gap-fill) (`:473-483`, `confidence_threshold`, `gap_fill_confidence_threshold`)
- revision: `_update_sources_from_adjudicated` rewrites `sources[]` in place — replaces `record_id@version` and `update_time` with adjudicated values when stripped `record_id` matches (Appendix job #5)
- note: connector/point-feature matching not yet handled (`:445-449` TODO)

### Phase 3 — relations (routes, destinations, turn restrictions)

- job: `RelationsRemappable` extracts OSM relation members from raw `geometry_daily` (`:366-381`; Appendix job #7)
- job: `RelationsNormalize` flattens members (`:518-535`; Appendix job #8)
- join: `MatchLayerToNetworkV2` matches relation members onto Orbis network + connectors, `geometry_column=member_geometry`, `ignore_inconsistent_layer_geometries=true`, `passthrough_columns=relation_id,relation_version,relation_type,member_idx,member_type,member_ref,member_role,relation_tags`, `additional_unique_columns=relation_id,member_idx` (`:538-564`)
- split: `RelationsPrecombobulate` run 3x via `output_type` switch on same matched-relations input -> `routes`, `destinations` (from segment tags, not matched relations), `turn_restrictions` (confidence thresholds 0.9 / n/a / 0.8) (`:567-633`; Appendix job #9)

### Phase 4 — combobulation

- join: `CombobulateSegments` merges `orbis_precombobulated_segments_path` + `relations_routes_path` + `relations_destinations_path` + `relations_turn_restrictions_path` -> `orbis_combobulated_segments_path`, `mode=full` (`:636-659`)
- black box: per-row UDF (`combobulator/combobulation_driver.py`) converts raw tags into `class`, `subtype`, `access_restrictions`, `speed_limits`, `prohibited_transitions`, etc. (Appendix job #10)

### Phase 4b — HPMS gap-fill (dead end)

- job: `MergeAttributes` — `match_results_path` (HPMS matched), `combobulated_segments_path` -> `output_path` (`orbis_enriched_segments_path`), `confidence_threshold=0.9`, `attributes=speed_limits,road_surface`, `changed_only=true` (`:664-685, 1009-1010`)
- drop (structural): `orbis_enriched_segments_path` is not read by any downstream task in this DAG — computed every run, discarded before `theme_stage`

### Phase 5 — QA issue tables, source sidecars, staging output

- write: `TransportationQA` writes `segments_issue_detail`/`segments_issue_overview` to bundle-local subdirectories (`:711-747`) — not the shared Iceberg violation store
- write: `TransportationStaging` reused across 5 `output_type`s: `source_tags`, `source_tags_connector`, `sources_lr`, `connector`, `segment` (`:812-849`)
- write: staged `connector`/`segment` output -> `output_bundle.data_uri + "/theme=transportation/type=connector|segment"` — this is the `theme_stage` bundle content
- write: `staging_metrics` (metrics framework) + `staging_summary` (`TransportationStagingSummary`, markdown report) run after staging (`:851-884`)

### Phase 6 — 13 validation rule checks (non-blocking)

- black box/aggregation: 13 rule classes under `overture_transportation/validations/` each read final staged data and write to their own `violations/{rule_name}` subdirectory inside the run bundle (`:1044-1063`); do not gate `finalize_bundle`
- write: `finalize_bundle` (`output_bundle.finalize_tg()`) stamps `metadata.json`, writes `success` marker (`:886`)

---

## Appendix: per-job transform detail

### Group A — Orbis production chain

**1. `orbis_ways_to_segments.py` — `OrbisWaysToSegments`**
- drop: filter `F.size(F.col("ext_nodes")) > 1`
- black box: `ST_MakeLine(transform(ext_nodes, node -> ST_Point(node.lon, node.lat)))` + `ST_RemoveRepeatedPoints` build geometry
- revision: merges paged step tags (`osm_identifier:step#1#`, `#2#`, ...) into one tag by stripping page suffix, concatenating by base key
- split: parses `osm_identifier`/`osm_identifier:step` into `ext_source_ids` (start_cm/end_cm ranges per OSM way), explodes into one row per source range
- new column: builds `sources[]` structs (OSM `w<id>@0` when identifier known, else null-record_id TomTom fallback)
- merge: re-aggregates exploded rows back to one row per segment
- id change/mint (external): `id = tags.gers_identifier`
- column state: retains `ext_nodes`; `connectors[]` not yet built (built in job #6)

**2. `orbis_nodes_to_connectors.py` — `OrbisNodesToConnectors`**
- aggregation: `groupBy("nd","is_start_node","is_end_node").agg(count(*) as degree)`
- drop: filter `degree > 1 OR is_start_node OR is_end_node`
- join: left-join normalized adjudicated-connectors table on `osm_identifier` (extracted OSM node id from first `n...`-prefixed source)
- id change/mint (external): `id = node_tags.gers_identifier`
- new column: `sources[]` preference order — adjudicated `sources[]` (augmented with `license`/`provider="osm"`/`resource="planet"`/`version`), else constructed `n<id>@0` fallback, else null-record_id placeholder

**3. `adjudicated_tags_normalize.py` — `AdjudicatedTagsNormalize`**
- revision: strips `@version` suffix from OSM way `record_id` to produce join key
- black box/validation: `F.raise_error` at write time if `osm_way_count > 1` (enforces 1:1 adjudicated-segment : OSM-way)
- new column: derives `subtype`/`class` from `source_tags`
- no id minted

**4. `match_layer_to_network_v2.py` — `MatchLayerToNetworkV2`** (invoked 3x: adjudicated-tag match, relation-member match, HPMS match)
- join (Phase 1, ID): explode network `sources[]`, strip `@version`, inner-join layer `record_id` onto network `record_id`; exact hits get `confidence=1.0`, `match_type="id"`
- black box (Phase 2, spatial): candidate generation via linestring alignment/coverage filtering; ML features = rapidfuzz name similarity (`ratio`/`token_sort_ratio`) + Sedona geometry UDFs (Hausdorff distance, buffer IoU, heading delta)
- black box: scored by pre-trained XGBoost model (`xgb.Booster.load_model`, `pandas_udf _predict_proba`); filter `ml_confidence >= 0.4` (`MIN_MATCH_CONFIDENCE`)
- aggregation: per-sequence confidence = average ML confidence across matched segments in sequence
- join (Phase 3, gap-fill, optional): spatially matches ALL layer records (not just unmatched); lower confidence threshold applied downstream (0.60 vs 0.95)
- black box: model artifact (`non_topological_model.json`) trained in separate repo (`brad-richardson/matcher`), loaded as static JSON; rapidfuzz second black-box dependency

**5. `adjudicated_tags_apply.py` — `AdjudicatedTagsApply`**
- drop/filter: `regular_matches` = `match_type != gap_fill AND confidence >= 0.95`
- drop/filter: `gap_fill_matches` = `match_type == gap_fill AND confidence >= 0.60`
- join: network segments to match results + normalized adjudicated segments (pulls `source_tags`)
- new column: `tag_apply_mode` ∈ {UNMATCHED, SIMPLE, COMPLEX}
- revision: UNMATCHED -> fallback tags from subtype/class; SIMPLE -> tags applied directly; COMPLEX -> `key:step` range-encoded tags (e.g. `"0-10000#Main St;10000#Main St"`)
- revision: network (TomTom) classification (`highway`/`railway`/`route`) always overrides adjudicated OSM classification when both exist
- revision: `_update_sources_from_adjudicated` rewrites `sources[]` in place — for each source whose stripped `record_id` matches adjudicated lookup, replaces `record_id@version` and `update_time` with adjudicated values
- black box/validation: `F.raise_error` if a segment with multiple matched sources has a null `between_cm` range

**6. `precombobulate_segments.py` — `PrecombobulateSegments`**
- black box: raw SQL, 6+ `spark.sql()` blocks over temp views (`segments_raw`, `subsegments`, `nodes_connected_at`, `nodes_connected_at_that_are_connectors`, `segment_connectors_grouped`, `orbis_overture_source_joined`)
- aggregation: window function `SUM(length_cm) OVER (PARTITION BY ext_tomtom_way_id ORDER BY end_node.idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` -> fractional connector position
- revision: snaps connector position to 0.0/1.0 within threshold
- new column: builds `connectors[]` array (grouped per segment, sorted by position) from `ext_nodes`
- no split/merge of segments (Orbis segments pre-cut by TomTom)

**7. `relations_remappable.py` — `RelationsRemappable`**
- drop/filter: `geometry_daily` filtered to `type=relation` matching connectivity/`destination_sign`/`route=road`/restriction
- split: explodes `members[]` per relation (order-preserving)
- drop/filter: pre-filters OSM node/way table to referenced ids only
- merge: regroups members back into ordered array per relation
- new column: `missing_geometry_count`/`missing_refs` debug map inline in `ext_debug`

**8. `relations_normalize.py` — `RelationsNormalize`**
- drop/filter: re-filters/tags relations by type
- split: `posexplode` members again -> one row per member with relation context replicated
- id change/mint: synthesizes OSM-style `record_id` (`w123`, `n456`, unversioned) as join key for `MatchLayerToNetworkV2`
- note: mandatory (`only_*`) turn restrictions not expanded here (deferred to job #9)

**9. `relations_precombobulate.py` — `RelationsPrecombobulate`** (3 modes via `output_type`)
- `routes`: join matched relation members (confidence ≥ 0.9) against network segments; new column `sequence[]` of `{segment_id, connector_id, start_at, end_at, member}`
- `destinations`: drop/filter segments carrying `destination*` OSM tags; split: explode `connectors[]`; new column `destination_candidates[]`
- `turn_restrictions`: revision/split — mandatory (`only_*`) restrictions algorithmically expanded into equivalent prohibitory restrictions via Python UDF (`turn_restriction_lib.mandatory_expansion.expand_mandatory_restriction`), one record becomes multiple (e.g. "only straight" -> "no left" + "no right"); new column `sequence[]` of `{connector_id, segment_id, segment_geometry, connector_geometry, connected_at, role}`
- drop: segments whose VIA connector unresolved within ~1.1km flagged with sentinel `VIA_INVALID_SEGMENT_SCORE = 999.0` and filtered

**10. `combobulate_segments.py` — `CombobulateSegments`**
- drop/filter: segments filtered to `subtype in (road, water, rail)`
- black box/validation: raises `Exception` if any `id` appears more than once
- join: left-joins route/turn-restriction relation data (unioned, grouped by `segment_id` into sorted `relations[]`) + tag-based destination candidates
- black box: per-row UDF `combobulate_segment_tags_udf` (wraps `combobulator/combobulation_driver.py`) converts raw tags into names, class/subclass, road surface, rail flags, width rules, access restrictions, speed limits, prohibited transitions, routes, destinations
- black box: 2 legacy raw `spark.sql()` reads for Hive-table input path (`_read_relation_data`, `_read_turn_restriction_data`, `_read_destinations`) — unused when S3 paths supplied (production always supplies S3 paths)

**11. `merge_attributes.py` — `MergeAttributes`** (HPMS gap-fill, discarded per Stage 5 Phase 4b)
- revision: merges HPMS candidate values (`speed_limits`, `road_surface`) into combobulated segments only where OSM coverage is missing; existing values always win; additive (`_merge_array` concatenates, never replaces)
- black box: validates class-appropriate speed ranges, blocks certain surface values on motorway/trunk
- black box: interval subtraction via Python UDF (`_subtract_intervals`) computes true gaps from `between`-fraction coverage ranges
- drop/filter: rejects HPMS speed values disagreeing with existing OSM values beyond tolerance (min of 30% relative / 30 kph absolute)

**12. `transportation_qa.py` — `TransportationQA`**
- black box: raw `spark.sql()` geometry-validation pass (`ST_IsValid`/`ST_IsValidReason`) — `output_type="overview"`
- aggregation: `output_type="detail"` counts records by category × `ext_combobulator_issues` list
- write: bundle-local violation/issue ledger (categories: invalid-geometry / missing-class / has-issues / OK)

**13. `transportation_staging.py` — `TransportationStaging`** (5 `output_type` branches; writes `theme_stage`)
- write: `source_tags` / `source_tags_connector` — pass-through projections of tag-rule sidecars
- black box: `sources_lr` — raw SQL joins exploded `sources[]` against `ext_source_ids`
- revision: computes `between = [start_cm/length, end_cm/length]`, snaps to 1.0 within threshold, collapses full-coverage `[0.0, 1.0]` to `NULL`
- revision: stamps `version` per source provider (`osm_version` for OSM, `orbis_version` for TomTom)
- write: `connector` — trivial projection (`id`, `version=0`, `sources`, `geometry`)
- black box: `segment` — raw SQL, left-joins combobulated properties with `sources_lr`, parses WKT geometry (regex-guarded)
- drop: hard release-quality gate — `(subtype in (road, rail) AND class IS NOT NULL) OR subtype = water`, geometry must parse + be non-degenerate (`ST_NumPoints > 1`, `ST_Length > 0`, no duplicate consecutive interior points), `SIZE(sources) > 0` — failing segments silently dropped here
- write: `theme_stage/theme=transportation/type=connector|segment`

**14. `staging_summary.py` — `TransportationStagingSummary`**
- aggregation: reads pre-aggregated metrics parquet + QA overview/detail outputs, renders Markdown run summary (counts, lengths, churn deltas, top-20 issue categories) — reporting only, not a transform

### Group B — OSM-only resegmentation forge (disconnected)

**15. `osm_nodes_to_connectors.py` — `OSMNodesToConnectors`**
- id change/mint: `id = F.expr("uuid()")` — fresh random UUIDv4 every run, not derived from OSM node id
- drop/filter: keeps node as connector if way-degree ≥ 2 (intersection), OR sole endpoint of `highway=crossing` way, OR way endpoint, OR "loop split" point (forced split for self-looping way)
- new column: `ext_osm_id` preserves original OSM id (join/debug only, not identity)
- new column: `sources[]` struct with `record_id = concat("n", id, "@", version)`
- also used unmodified by Stage 2a `dataset_osm_ingest_dag.py`

**16. `osm_ways_to_segments.py` — `OSMWaysToSegments`**
- drop/filter: filters OSM ways to transportation-relevant ones
- join: joins way `refs` to job #15 connector nodes
- new column: `connectors[]` with geodetic LR fraction per connector (`ST_LineLocatePoint`/`ST_LineSubstring`/`ST_LengthSpheroid`)
- revision: `has_valid_endpoints` check (`size(at_values) >= 2`, first `at_value` ≈ 0.0, last ≈ 1.0); when `is_monotonic AND has_valid_endpoints` fails, `connectors` set to empty array instead of the computed value
- id change/mint: `id = uuid()`
- also used unmodified by Stage 2a `dataset_osm_ingest_dag.py`

**17. `osm_nodes_lr.py` — `OSMNodesLR`**
- new column: `at_cm` position for every connector node along every OSM way referencing it — `0`/full-length shortcut for first/last node, spatial `ST_LineLocatePoint` fallback for simplified geometries, exact `ref_index`-based `ST_PointN` otherwise
- new column: `is_invalid_way` flag joined on, from `detect_invalid_ways` (loop/self-intersection detection)

**18. `osm_segment_split_points.py` — `OSMSegmentSplitPoints`**
- join: self-join of LR output on `node_id` (`node_1.node_id == node_2.node_id AND node_1.way_id < node_2.way_id`) — finds nodes where two different ways meet mid-way; separate path for self-looping ways
- join: joins candidates against `osm_segments` to pull each way's road class
- drop/filter: keeps only splits where both sides' class is in `SEGMENT_SPLIT_ROAD_CLASSES`

**19. `osm_split_segments.py` — `OSMSplitSegments`**
- split: gathers every connector for a way, sorts/dedupes adjacent same-node connectors, filters to endpoints+split-points, pairwise-explodes consecutive connectors — a way with N split points becomes N+1 candidate subsegments
- id change/rebind: `new_segment_id = original_segment_id` when `both_original_endpoints` (both connectors are original endpoints and not splitting connectors); else `uuid()` minted
- revision: geometry sliced by exact `ref_index` (`ST_PointN`/`ST_MakeLine`) when unsimplified, or normalized `at_cm` ratio (`ST_LineSubstring`) when simplified
- new column: `split_debug_info`/`ext_debug` carries `original_segment_id`, `start_node_id`/`end_node_id`, `start_at_cm`/`end_at_cm`

**20. `osm_segment_merge_points.py` — `OSMSegmentMergePoints`**
- drop/filter: excludes any 3+-way junction node from merge eligibility
- join: self-joins remaining connectors on `node_id`
- aggregation: scores each candidate pair by length, bearing/heading delta, interior angle; keeps best-scoring candidate per node

**21. `osm_segment_merge_groups.py` — `OSMSegmentMergeGroups`**
- black box: GraphFrames connected components via direct JVM call — `jvm_graph.connectedComponents().setAlgorithm("graphframes").setCheckpointInterval(2).setBroadcastThreshold(1000000).run()`
- merge: builds graph (vertices = candidate segments, edges = merge-point pairs from job #20), connected components -> chain of N collinear split-segments becomes one component
- requires Spark checkpoint directory (raises without one)

**22. `osm_merge_segments.py` — `OSMMergeSegmentsIterative`**
- aggregation: `groupBy("component").agg(collect_list("id"), collect_list("geometry"), collect_list("sources"), collect_list("connectors"), ...)`
- black box: `merge_component_udf` — non-vectorized Python UDF, in-memory graph/DFS traversal over the collected component arrays; walk starts from degree-≤1 endpoint segments, splits walk where merge would create duplicate connector nodes
- merge: N segments in a component -> merged segment(s), grouping key = `component`
- black box: `ST_LineMerge(ST_Collect(...))` stitches surviving geometries
- id change/mint: `final_merged_df.withColumn("id", F.expr("uuid()"))` — every merged segment gets a brand-new uuid regardless of input count; old→new mapping survives only in `ext_debug["merged_segment_ids"]`
- pass-through: segments with no merge group keep original id, unmerged

**23. `osm_match_segments.py` — `OSMMatchSegments`**
- join: `ST_Intersects(new.buffered_envelope, prod.buffered_envelope)` — spatial join of new split/merged segments to last-published Overture release segments, envelope-intersection only
- drop/filter: pre-filters candidates by centroid distance, length difference before Fréchet distance computation
- black box: `ST_FrechetDistance` computation
- aggregation: `match_score` = Fréchet distance + length-ratio + class match + OSM-source-ID overlap + length-difference penalty, bucketed HIGH/MED confidence
- drop/filter: only high-confidence matches reassign id; 1-to-many conflicts resolved by max score with deterministic tie-break
- id change/rebind: `id = coalesce(matched_prod_id, id)`; `old_id` column retains pre-match id
- black box/validation: `_validate_no_duplicate_ids` raises `RuntimeError` on any post-reassignment id collision
- note: no explicit spatial partitioning (`.repartition()` by grid) or broadcast hint before `ST_Intersects` join

### Validations framework (`validations/*.py`, 13 rule files)

Rules: tunnel level, bridge level, motorway speed limit, motorway-link minimum speed, speed-limit-value, duplicate name, duplicate common names, street/route name invalid character, access-restriction conflict, superfluous speed limit, minimum segment length, paved-road-class/surface, max-speed plausibility.

- drop/filter (per rule): reads staged transportation parquet, explodes the relevant array-typed property, applies a rule-specific boolean condition
- write (per rule): formats violating rows into standard schema (`id, version, geometry, dataset, ds, context, violation_name, violation_count`), `write.mode("overwrite").parquet(output_path)` -> `violations/{rule_name}/`
- black box: some rules subclass `overture_spark.validations.base_spark_check.SparkCheck`, which registers input DataFrames as temp views and runs a caller-supplied raw SQL `query` string via `self.spark.sql(self.query)` (query text lives outside this repo, in the DAG/config layer)

---

## Summary table

| Stage | Operations | ID impact |
|---|---|---|
| 0a Release Publish | write | none |
| 0b Bridging (release candidate assembly) | write | none |
| 0c Theme Promote | join, new column, drop, aggregation, write, column removed, black box (pmtiles only) | none |
| 1a Orbis raw ingest | black box, write | none |
| 1b OSM planet bootstrap | black box, write | none |
| 1b OSM daily incremental | black box, join, drop, split, write | none |
| 1c HPMS raw collect | black box, write | none |
| 2a OSM-in-Overture ingest | black box (jobs #15/#16) | mint |
| 2b OSM adjudication | join, revision, merge, black box, write | none |
| 3 HPMS ingest + pre-match | black box, join, new column | none |
| 4 OSM resegmentation forge | id change/mint, new column, split, merge, black box, join, id change/rebind, write | mint, then rebind |
| 5 Phase 0 PBF→parquet | black box | none |
| 5 Phase 1 Orbis base network | join, id change/mint (external), new column | mint (external, TomTom) |
| 5 Phase 2 adjudicated tag transplant | join, revision, black box | none (tags/sources revised, id untouched) |
| 5 Phase 3 relations | join, split, merge, id change/mint (synthetic join key) | none (segment id untouched) |
| 5 Phase 4 combobulation | join, black box, revision | none |
| 5 Phase 4b HPMS gap-fill (discarded) | revision, black box, drop | none |
| 5 Phase 5 QA + staging | write, revision, black box, drop | none |
| 5 Phase 6 validations | black box, aggregation, write | none |
| Appendix Group A job #1 OrbisWaysToSegments | drop, black box, revision, split, new column, merge, id change/mint (external) | mint (external) |
| Appendix Group A job #2 OrbisNodesToConnectors | aggregation, drop, join, new column, id change/mint (external) | mint (external) |
| Appendix Group A job #3 AdjudicatedTagsNormalize | revision, black box | none |
| Appendix Group A job #4 MatchLayerToNetworkV2 | join, black box, aggregation | none |
| Appendix Group A job #5 AdjudicatedTagsApply | drop, join, new column, revision, black box | none (sources[] revised) |
| Appendix Group A job #6 PrecombobulateSegments | black box, aggregation, revision, new column | none |
| Appendix Group A job #7 RelationsRemappable | drop, split, merge, new column | none |
| Appendix Group A job #8 RelationsNormalize | drop, split, id change/mint (synthetic) | none (synthetic join key only) |
| Appendix Group A job #9 RelationsPrecombobulate | join, drop, split, new column, revision | none |
| Appendix Group A job #10 CombobulateSegments | drop, black box, join | none |
| Appendix Group A job #11 MergeAttributes | revision, black box, drop | none |
| Appendix Group A job #12 TransportationQA | black box, aggregation, write | none |
| Appendix Group A job #13 TransportationStaging | write, black box, revision, drop | none |
| Appendix Group A job #14 TransportationStagingSummary | aggregation | none |
| Appendix Group B job #15 OSMNodesToConnectors | id change/mint, drop, new column | mint |
| Appendix Group B job #16 OSMWaysToSegments | drop, join, new column, revision, id change/mint | mint |
| Appendix Group B job #17 OSMNodesLR | new column | none |
| Appendix Group B job #18 OSMSegmentSplitPoints | join, drop | none |
| Appendix Group B job #19 OSMSplitSegments | split, id change/rebind, revision, new column | rebind (conditional mint) |
| Appendix Group B job #20 OSMSegmentMergePoints | drop, join, aggregation | none |
| Appendix Group B job #21 OSMSegmentMergeGroups | black box, merge | none |
| Appendix Group B job #22 OSMMergeSegmentsIterative | aggregation, black box, merge, id change/mint | mint |
| Appendix Group B job #23 OSMMatchSegments | join, drop, black box, aggregation, id change/rebind | rebind |
| Validations framework | drop, write, black box | none |
