# Places Pipeline Mechanics (terse companion to pipeline-trace-places.md)

Mechanical facts only, tagged with cairn's operation vocabulary: drop, merge, split, revision,
id change/rebind, join, new column, column removed, aggregation, black box, write. Same stage
order as the source doc (backward-to-forward research order; numbered 1-10 as in the source).

---

## Stage 1: Release Publish (release_publish_dag.py)

- write: `data/`, `changelog/`, `bridgefiles/`, `registry/` synced scratch bucket -> AWS release bucket, Azure blob container, archive bucket (lines 314-332)
- write: PMTiles copied via boto3 multipart copy (lines 327-332)
- black box: `PublishStac` (overture_core.stac.job) generates/mirrors STAC items, invalidates CloudFront (lines 334-353)
- black box: starts 4 Glue crawlers in Distribution account (lines 355-376)
- revision: RC bundle tagged `released`, `released_at` stamped in `metadata.json` (lines 378-380)
- no record-level transform; no filter/merge/id change

---

## Stage 2: Theme Promote (theme_promote_dag.py, theme_promote.py)

- (plumbing) `setup`: `validate_bucket_accessibility` x2 + `validate_input_data_path` — existence/permission checks, no data read (theme_promote.py:175-191)
- (plumbing) `discover_input_types`: lists `type=` partitions under the input bundle via S3 `list_objects_v2` (theme_promote.py:89-103)
- join: `ComputeInternalChangelogJob` full-outer-joins old vs new theme data on `(id, type)` (compute_internal_changelog.py:83-176)
- new column: per-column content hash (`sha2`, geometry via `ST_AsText`) on each side (compute_internal_changelog.py:83-176)
- new column: `change_type` = ADDED/REMOVED/DATA_CHANGED/UNCHANGED (compute_internal_changelog.py:83-176)
- new column: `version` = `old.version + 1` if hashes differ else `coalesce(old.version, 1)` (compute_internal_changelog.py:83-176)
- write: `changelog_df` keyed by `id`; no rows dropped here (compute_internal_changelog.py:83-176)
- aggregation: `ComputeChurnJob` groups the changelog by `type`, computes added/removed/data_changed/unchanged counts and percentages (compute_churn.py:133-142)
- write: churn stats to CSV/Markdown/Parquet, plus a telemetry write (compute_churn.py:55-124)
- drop (job-level, not record-level): `validate_churn` reads `changelog_stats.csv`, compares each type's percentages against `THEME_THRESHOLDS` (places has its own `place` threshold set), fails the whole run if exceeded (changelog.py:131-263)
- aggregation: `validate_data` (`ValidateDataJob`) runs `compare_schemas` against the resolved `overture-schema` version's registry entry for the type, then `evaluate_checks` (declared per-field checks from that registry), groups violations by `field:check` (validate_data.py:141-246, theme_promote.py:297-316)
- write: samples of violating rows to `output_path` when errors exist (validate_data.py:219-230)
- drop (job-level): `validate_data` fails the run if `error_rows > 0` (validate_data.py:241-245)
- join: `process_data` (`ProcessDataJob`) left-joins the theme data to `changelog_df.select("id","version","bbox")` on `id` (process_data.py:~133, theme_promote.py:219-232)
- drop: rows whose `bbox` doesn't intersect the run's `bbox` param, when one is passed (process_data.py `_filter_by_bbox`)
- drop: rows with `id` in the hardcoded `overture_cdp.block_list.BLOCKED_IDS` (process_data.py:126-131)
- new column: bbox/version columns applied by `process_data` (process_data.py:~133)
- write: spatially repartitioned (KDB-tree over bbox centroids) GeoParquet with Hilbert-ordered row groups (process_data.py `write_spatial_parquet`)
- column removed: `compute_public_changelog` drops `version` before writing the public changelog (compute_public_changelog.py:30)
- write: public changelog spatially repartitioned, written partitioned by `change_type` (compute_public_changelog.py:23-35)
- aggregation, write: `generate_metrics` runs a Spark metrics-suite job for the theme against the latest published release baseline (metrics.py:101+) — real Spark logic, not traced deeper in this pass
- write: bridge-file generation for places reads from corpus directly (`use_corpus=True`), not from assembled path (theme_promote.py:264-289)
- black box: `pmtiles_task` submits an AWS Batch job running the public `ghcr.io/overturemaps/overture-tiles` Docker image; tiling logic itself is not in this repo (pmtiles.py:25-90)
- (plumbing) `validate_final_output`: checks S3 for expected output directories (existence check only, no data transform) (validation.py:151-181)
- (plumbing) `cleanup_hidden_files`: S3 cleanup of hidden files
- no per-provider attribute merging at this stage (already done in Stage 4)

---

## Stage 3: Theme Places Assemble (theme_places_assemble_dag.py)

- black box: triggers `corpus_data_export_dag` twice — `TableName="place"` and `TableName="patch"` (lines 120-144)
- black box: triggers `theme_places_merge_dag` with `merge_type="non_attribute"`
- pure orchestration; no record-level transform in this DAG itself
- `use_patches` param (default `True`) gates whether patch export runs; if `False`, `patches_uri=None`

---

## Stage 4: Theme Places Merge (places_merge.py, merge/*.py)

### Mode selection
- black box: `merge_type` param selects `NonAttributeMerger` (production default, set by Stage 3) or `BasicAttributesMerger` (DAG's own default) (places_merge.py:10-53)

### Grouping / base selection (base_properties_feeds_merger.py:118-146)
- merge: `groupBy("id")` (matcher-assigned id), `collect_list(struct(*columns))` per group
- aggregation-adjacent: group sorted by `(license_priority[sources[0].dataset], -confidence)`; rank 0 = base, rest = lower_ranked (basic_attributes_merger.py:16-36 / non_attribute_merger.py:10-29)
- id change/rebind: none — output row keeps the group's existing `id` verbatim; no new id minted

### BasicAttributesMerger per-field rules (basic_attributes_merger.py:38-94)
- revision: `geometry` — base only, verbatim, never touched by fold-in loop
- revision: `websites`/`emails`/`socials`/`phones` — `ListMerger.merge`, union + de-dup, base's items first, order-preserving
- revision: `categories` — base's `primary` always wins; differing lower `primary` demoted into `alternate` (deduped); lower's own `alternate` folded in (deduped)
- revision: `names` — base's `primary` wins; differing lower `primary` demoted into `rules` as `{"variant": None, "value": <other_primary>}`; `common` gap-filled only where base value is null; `rules` unioned with exact-match dedup
- revision: `sources` — only lower-ranked record's own whole-POI entry (`property` is `None`/`""`) appended, first match only (`break`)
- revision: `brand`, `addresses` — `properties_to_ignore`, base only always (addresses never unioned despite list type, since ignore-check precedes list-type check)
- revision: `version` — `properties_to_ignore`, base only, untouched
- revision: `operating_status`, `basic_category`, `taxonomy` — no special-case branch, not list type; base value passes through unchanged (silent, by omission)
- aggregation: `confidence` — `merged_confidence = 1 - Π(1 - confidence_i)` across base + all lower_ranked (basic_attributes_merger.py / non_attribute_merger.py)
- new column value: synthetic source appended to `sources` — `{"property": "/properties/confidence", "dataset": "Overture", "license": "CDLA-Permissive-2.0", "record_id": None, "update_time": <run date>, "provider": "overture", "resource": "confidence_calculation", "version": <confidence_source_version>}` (base_properties_feeds_merger.py:33-44)

### NonAttributeMerger (production mode, non_attribute_merger.py:9-62)
- revision: base kept wholesale; all lower-ranked fields (names, categories, geometry, socials, etc.) discarded entirely except confidence
- revision: `sources` on all entries — `property` blanked via `change_source_prop_to_empty_string`, then confidence_source appended
- aggregation: same `1 - Π(1-confidence_i)` recombination as basic mode

### Patches overlay (properties_patcher.py, base_properties_feeds_merger.py:148-182)
- join: left join, `merged_places.id == patches.id` — unmatched merged rows pass through unchanged
- drop (job-level): patch `type` must be `UPSERT` or `DELETE`; anything else hard-fails the job (`_validate_patch_types`)
- revision: single-value attrs (`geometry`, `confidence`, `brand.wikidata`, `brand.names.primary`, `version`, `names.primary`, `operating_status`) — newest UPSERT by source `update_time` wins; if no UPSERT, first DELETE applies
- revision: multi-value attrs — all patches for the attribute replayed in sequence, oldest-source-first, DELETE before UPSERT on ties
- new column value: `sources` gains one normalized entry per applied patch (except confidence/existence patches)

### Dedup/drop in this stage
- drop: none — every `id` group in `matched_uri` (including singleton clusters) produces exactly one output row; no size/completeness threshold

### Black box / opaque items
- black box: `get_uri()` JSON-or-raw-path parsing swallows `JSONDecodeError` only
- black box: `SourcesMerger.merge` `break`s after first whole-POI match — extra whole-POI entries on a lower-ranked record silently dropped
- black box: no raw SQL; one Sedona expr `ST_GeomFromWKB(geometry)` at final write
- write: merged output to `merged_uri` param (path value not given in source doc)

---

## Stage 5: Theme Places Match, and the Corpus store (theme_places_match_dag.py, matching_operator.py, matching_utils.py)

- black box: entire corpus service (`org.overturemaps.store.JobRunner`, Scala/Spark on Iceberg via Glue REST catalog) — `RegisterThemeType`, `DataLoad`, `DataExport`, `CreateBranch`, `GetBranches`, `GetBranchVersions` verbs only, internals opaque
- black box: `parse_inputs`/`parse_baseline` resolve input/baseline paths via `corpus_data_export_dag` (pure read/export, no transform)
- black box: candidate-clustering/"same place" decision — either Databricks Jobs API call to named job (e.g. `"[places] Matching Pipeline"`), output pulled from `"assign"` task (matching_utils.py:226-236); or Glue path running `org.overturemaps.matching.Main` from a pinned CodeArtifact JAR (`matching_scala_version` Airflow Variable) (matching_utils.py:445-522)
- id change/rebind: matcher assigns/preserves `id` on each output record (decision logic opaque); this is where record identity for the cluster is fixed
- write: `write_match_output_metadata` stamps `metadata.json` (input/baseline/result paths)
- black box: `load_corpus` gated on `scenario=="overture"` and `output_corpus_branch` set; if target branch != `main`, forks branch first (`corpus_create_branch_dag`)
- write: `corpus_data_load_dag` -> Scala `DataLoad` job, `IdField` hardcoded to `"id"`, `TableName="place"` (matching_operator.py:643-651)
- aggregation: `DataLoad` with `computeChangelog=True` (default) appends entity-level changelog rows to an Iceberg side table (opaque Scala)
- black box: `corpus_data_export_dag` — validates table exists, reads Iceberg metadata side table, exports branch/version to S3 parquet; optional split into `corpus/`+`corpus_deletes/`, or combined with `status` field when `CombineOutput=True`
- black box: `emit_matcher_telemetry` reads Scala-written metrics parquet, forwards to metrics store tagged `stage="matching"` (no effect on data path)
- black box: CodeArtifact auth token embedded directly in JAR download URL (matching_utils.py:456-465)
- no raw SQL anywhere in this stage's Python/Airflow code

---

## Stage 6: Theme Places Ingest Orchestrator (theme_places_ingest_orchestrator_dag.py)

- black box (change detection, not a data transform): `check_provider_changes` compares provider's raw S3 last-modified vs stored `ingestion_metadata/<provider>/places/`; if unchanged and `force_ingest=False`, provider skipped for the run (lines 60-110)
- black box: per non-patch provider with new data, triggers Collect (Stage 9) -> Ingest (Stage 7) -> builds matcher config -> triggers Match (Stage 5)
- black box: `Patches` provider skips collection, goes straight to `theme_places_ingest_patch_dag` (Stage 8), bypassing matching
- revision: on success, updates stored `last_ingestion_time`/`ds_partition` per provider
- new column (config only, not record data): threads 4 spatial-filter dataset paths + `enable_spatial_filters` into Stage 7's config
- no record-level filtering/merging in this DAG

---

## Stage 7: Theme Places Ingest ("Overturize") (places_feed_ingest.py, places_data_provider.py, providers/*)

### Pipeline order (places_data_provider.py:537-674)
`_load` -> `_normalize` -> `_filter_by_bbox` -> `_categorize` -> `_check_operating_status` -> `_apply_patches` (no-op default) -> `_pre_geocode_filter` (no-op default) -> `_get_legacy_category` -> `_geocode` (conditional) -> `_filter` (PlacesFilterChain) -> `_post_process` -> `_validate` -> write parquet (mode=overwrite)

### `_normalize` (provider-specific, raw -> Overture schema)
- id change/rebind: provider's native id column renamed to `id` (value unchanged) — e.g. Foursquare `fsq_place_id` -> `id` (foursquare_places_provider.py:107-203); no new id value is minted at this stage for any provider
- new column: Foursquare `geometry` <- `ST_AsBinary(ST_Point(longitude, latitude))` (Sedona expr, black box)
- new column: Foursquare `confidence` <- constant `FOURSQUARE_DEFAULT_CONFIDENCE`
- revision: Foursquare category <- `element_at(fsq_category_labels, 1)`, trimmed last `>`-delimited segment
- drop: Foursquare — pre-normalize logistic regression (hardcoded coefficients) on website/phone/social presence + freshness; drops `attribution_score <= 0.65`, excluded-category-list records, and closed places; **not logged to invalid-features/violation store**
- revision: Meta (`meta_places_provider.py:16-78`) — near-pure column rename, no pre-filtering
- new column: AllThePlaces maps OSM tags to a curated taxonomy CSV in `_categorize` override
- drop: AllThePlaces `_categorize` override hard-drops rows where resolved `taxonomy` is null (`.filter(col("taxonomy").isNotNull())`)
- column removed / not produced: BrightQuery (`requires_geocoding=True`) never produces `geometry` in `_normalize`, only lat/lon "bias hint" columns — geometry populated later by geocoding step

### `_filter_by_bbox` (places_data_provider.py:120-152)
- drop: Sedona `ST_Intersects` bbox predicate — silent geographic crop, applied right after normalize, not part of filter chain, not logged as violation

### `_categorize` / category matching (category_matcher.py:289-352)
- black box: escalating match — (1) curated `overture_taxonomy.json` per-source mapping, (2) direct taxonomy leaf match, (3) reuse of a different provider's learned mapping, (4) semantic match via `sentence-transformers/multi-qa-mpnet-base-cos-v1` embeddings, cosine threshold `0.85`, (5) no match -> `None`
- new column: successful new mappings appended to `CategoryCandidateLedger` (append-only Iceberg audit table, black box `CREATE TABLE IF NOT EXISTS ... USING iceberg`)
- revision: unmatched category -> `None` (kept, not dropped) except AllThePlaces (hard-drop per above)

### `_check_operating_status`
- revision: default-fills only; never drops rows

### `_get_legacy_category`
- new column: back-fills deprecated `categories` field

### `_geocode` (BrightQuery, Krick — `requires_geocoding=True`)
- black box: TomTom Geocoding API call from Spark executors (`util/tomtom_geocoder.py`); API key from AWS Secrets Manager `/managed-secrets/tomtom/geocoder_api_key`
- revision: only `"Point Address"` results with `score >= 0.7` accepted
- new column: `geometry` populated from accepted geocode result
- black box: geocode results cached in Iceberg table keyed by SHA-256 of normalized address fields + rounded bias coords; anti-join for cache misses; raw SQL `MERGE INTO {cache_table} ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT *` to write results back

### `_filter` -> `PlacesFilterChain.apply_filters()` (places_filter_chain.py:107-249, cheap filters first, spatial join last, `left_anti` join at lines 154-171)
- drop: duplicate provider `id` — window function ordered by `names.primary`, keep 1 row per `id` (places_filter_chain.py:124-139, 693)
- drop: duplicate by attributes — group by `(wkt_geometry, name_primary, category_primary)`, any group with `count > 1` flagged/dropped (places_filter_chain.py:746)
- drop: missing required fields — schema-driven required leaf fields (`id`, `geometry`, `sources`, nested fields like `categories.primary` only when parent struct non-null) (places_filter_chain.py:825)
- drop: invalid geometry — condition kept is `x.between(-180,180) & y.between(-90,90) & x!=0 & y!=0`; rows failing this are dropped (places_filter_chain.py:524)
- revision: country code aliasing before boundary check — `UK`->`GB`, `AN`->`AW/BQ/CW/SX`, `PS`->`XG/XW`
- drop: invalid country boundary — Sedona spatial join vs Overture Divisions (buffered 300m), disputed-areas GeoJSON, Overture water tiles; decision hierarchy (spatial_filter_mixin.py:211-437, 413-421): declared-match -> keep; declared-mismatch-in-disputed-area -> keep; declared-mismatch -> reject; declared-unrecognized-code -> reject; no-declared-and-not-in-water -> keep; no-declared-and-in-water -> reject
- revision (soft filter, not a drop): invalid categories — checked against broadcast CSV allowlist; failing records kept with `categories`/`taxonomy` nulled (places_filter_chain.py:574)
- drop: name matches address component — AllThePlaces-only, conditionally registered (places_filter_chain.py:75-78); drops where `names.primary` == normalized address locality (places_filter_chain.py:620)

### `_post_process`
- revision: cosmetic null-normalization

### `_validate`
- black box (hard-fail, not a drop): schema-shape assertion, raises on mismatch

### Write
- write: `validated_df.write.parquet(s3_output_prefix_uri, mode="overwrite")`

### Side write: invalid/rejected features (places_filter_chain.py `_write_invalid_df`, always writes even if empty)
- write: `places_invalid_features_repository_uri`, schema = `dataset` (prefixed `places/{provider}`), `violation_name`, `id`, `version`, `severity`, `geometry`, `context`, `counterpart` (overture_spark/entity_violations.py:14-37)
- revision: `unfiltered_ingest=True` (dev-only) skips the filter chain entirely and writes an empty violations frame instead

### Side write: entity violations store (theme_places_ingest_dag.py:536-561, `EntityViolationsUpdate`)
- drop: input filtered to exactly 7 whitelisted violation names (`duplicate_provider_id`, `missing_required_fields`, `duplicate_by_attributes`, `invalid_geometry`, `invalid_country_boundary`, `invalid_categories`, `name_matches_address_component`); any other tag silently dropped
- write: Iceberg `MERGE INTO`, keyed on `(id, violation_name, version, dataset, counterpart)` (existing rows refresh `severity`/`geometry`/`context`; new rows inserted)
- terminal sink: no re-injection/retry back into places feed, corpus, or merge; no deletes (separate maintenance job handles snapshot/compaction)

### Side write: places embeddings cache (theme_places_embed_dag.py -> `PlacesEmbedJob`)
- black box: model = pinned `jinaai/jina-embeddings-v5-text-nano-text-matching` at fixed revision, shared source of truth with Scala matcher
- new column (cache row): text embedded = `names.primary` (lowercased/trimmed), `taxonomy.primary` (underscores -> spaces)
- new column: cache key = SHA-256 hash of normalized text + `field_type` (`"name"`/`"taxonomy"`) — global content-addressed, shared across providers
- join: left-anti join vs existing Iceberg cache to isolate new `(hash, field_type)` pairs
- revision: vectors L2-normalized then int8-quantized
- write: `writeTo(table).append()` — new rows only, Iceberg embeddings cache table
- (downstream, Stage 5 matcher consumes this table by name as `--embeddingsCacheTable`, candidate-lookup aid only, not a source-of-truth field)

### Side branches: feed changelog / attribute completeness
- aggregation: `feed_changelog_task_group` diffs new feed version vs previous for changelog/churn stats
- aggregation: `ingest_attribute_completeness` (`AttributeCompletenessJob`) records per-field completeness metrics
- neither modifies the record stream

### Flags
- black box: raw SQL — `CREATE TABLE IF NOT EXISTS ... USING iceberg` (category ledger, TomTom cache); `MERGE INTO` (TomTom cache)
- black box: TomTom Geocoding API; `sentence-transformers/multi-qa-mpnet-base-cos-v1` pretrained model (loaded locally)
- black box: `PROVIDERS` dict (`places_data_providers/providers.py`) — static but runtime string-keyed dispatch on `provider_name`
- revision (mode-dependent): `unfiltered_ingest=True` changes behavior inside individual providers (Foursquare skips ML attribution filter; AllThePlaces skips dedup, keeps taxonomy-unmatched rows; BrightQuery skips pre-geocode filter) rather than centrally in `PlacesFilterChain`

---

## Stage 8: Theme Places Ingest Patch (patches_ingest.py)

- black box: input read as Spark structured streaming from `PATCHES_RAW_DATA_URI = s3://3ppp-output-places-omf/`, `recursiveFileLookup=true` (patches_ingest.py:49-53)
- schema (patches_ingest.py:25-34): `pid`, `id`, `type`, `attribute`, `value`, `sources[]`
- new column: `version` derived from source file path (`.../(manual|scheduled)__<version>/...`), else job's `version` param; hard-fails if neither parses to a date
- revision: every `sources[]` entry stamped with `provider="overture"`, `resource=f"{attribute}_signal"`
- drop: none; no filtering, no dedup, no id assignment
- write: appended (streaming, checkpointed) to `SourceIngestBundle` data path
- write: `corpus_data_load_dag`, `TableName="patch"`, `BranchName="main"`, `InputPath=<bundle>/*.parquet`, `IdField="pid"`, `Source="Overture-signals"` (theme_places_ingest_patch_dag.py:110-124)
- id change/rebind: none — keyed by existing `pid` (the place id being patched), distinct from Stage 5's `IdField="id"`; bypasses matcher entirely

---

## Stage 9: Theme Places Collect (theme_places_collect_dag.py)

- write: `DataSyncOperator` copies provider raw data from `omf-places-data-providers/<provider>/ds=<version>/` or `meta-overture-staging/...` into `SourceRawBundle` path `s3://<managed_bucket_source_data>/datasets/...` (lines 111-130)
- config: `Options: {"PreserveDeletedFiles": "REMOVE"}` — destination mirrors source exactly
- drop: none; no records filtered/mapped/re-identified

---

## Stage 10: Source-Specific Collectors (earliest raw reads)

General: each collector is a `SparkSedonaJob` subclass used for IAM access only, no distributed transform; none assigns an Overture/GERS id; each writes to `s3://omf-places-data-providers/<provider>/ds=<date>/` + `metadata.json`.

### AllThePlaces (alltheplaces_collect.py)
- black box: external system `alltheplaces.xyz`; bare `urlopen` fetch of `latest.json` (lines 71-83); downloads zip of per-spider `.geojson` files (lines 97-111)
- drop: `FILTER_OUT_SPIDERS = {"moneygram", "little_free_library", "gbfs"}` (lines 15-16, 137-150)
- drop: `license.lower() not in ACCEPTABLE_LICENSES = {"creative commons zero", "cc0"}` (lines 12-13, 137-150); files lacking `dataset_attributes` bypass both checks (kept)
- new column: `atp_run_end_datetime` stamped per feature
- write: `s3://omf-places-data-providers/alltheplaces/ds=<run_date>/<run_date_compact>_alltheplaces.json`
- id: none assigned; native ids kept

### Foursquare (foursquare_collect.py)
- black box: HuggingFace Hub dataset `datasets/foursquare/fsq-os-places/release` via `HfFileSystem`, token from AWS Secrets Manager `/managed-secrets/huggingface_api_token`
- drop: none — no filter/license/quality logic; per-job docstring, byte-for-byte transfer
- black box: skips entirely if release's `metadata.json` already exists in S3 (idempotency, run-level skip not a record drop)
- write: parallel-copies each `.parquet` byte-for-byte; `fsq_place_id` untouched

### LLM Toolkit (llm_toolkit_collect.py)
- black box: external HTTP API `https://places-llm-api-test.ds.io` ("Places LLM Toolkit API")
- drop: server-side filter via query params before download — `min_quality_score` default `0.7`, `is_open_license` default `true`, `commercial_use_allowed` default `true` (lines 33-42, 92-97); filtering happens entirely inside the external API, invisible to repo code
- black box: `RuntimeError("No results matched the configured filters...")` if zero results match
- black box: each result's `output.parquet` downloaded via redirect-following GET to a presigned S3 URL (external hop)
- write: `s3://omf-places-data-providers/llm_toolkit/ds=<today>/<dataset_name>_<result_id[:8]>.parquet`
- id: none assigned; native row-level ids from LLM Toolkit pass through

---

## Side pipelines (not part of Collect -> Ingest -> Match -> Merge -> Assemble -> Promote -> Publish)

- black box: `places_eval_coverage_dag.py`, `places_eval_precision_dag.py`, `places_eval_stats_dag.py` — `placeeval` subcommands on ECS, read published data, quality-monitoring only
- black box: `feature_places_quality_compute_dag.py` — `QualityScoreJob` reads `corpus_places.place` (Glue REST Iceberg), scores with a bundled XGBoost model; writes a quality score, doesn't alter places records
- black box: `feature_places_quality_cross_theme_dag.py` — spatial/textual features between places and other themes
- black box: `feature_places_quality_threeppp_dag.py` — confidence-feature producer against raw 3PPP patch parquet (`3ppp-output-places-omf`)
- black box: `feature_places_website_resolve_dag.py` — 5-stage ECS website URL resolution (discovery -> resolution -> merge back)

---

## Summary table

| Stage | Operations (tags) | ID impact |
|---|---|---|
| 1. Release Publish | write | none |
| 2. Theme Promote | join, new column, drop, aggregation, write, column removed, black box (pmtiles only) | none |
| 3. Theme Places Assemble | (orchestration only, no tagged ops) | none |
| 4. Theme Places Merge | merge, revision, aggregation, join, new column, black box, write | none (id preserved verbatim from matcher) |
| 5. Theme Places Match / Corpus | black box, id change/rebind, aggregation, write | rebind (matcher assigns/finalizes `id`) |
| 6. Ingest Orchestrator | revision (metadata only), black box (scheduling) | none |
| 7. Theme Places Ingest ("Overturize") | id change/rebind, new column, revision, drop, black box, join, aggregation, write | rebind (native id column renamed to `id`, value unchanged; no new value minted) |
| 8. Ingest Patch | new column, revision, black box, write | none (keyed by existing `pid`, matcher bypassed) |
| 9. Theme Places Collect | write | none |
| 10. Source-Specific Collectors | drop, new column, black box, write | none (native ids kept) |
