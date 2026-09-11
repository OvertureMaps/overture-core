# Divisions Pipeline Mechanics (Cairn Vocabulary)

Terse companion to `pipeline-trace-divisions.md`. Mechanical facts only, tagged
with cairn operation vocabulary. Same stage numbering/order as the source doc.

---

## 0. Shared stages

### 0.1 `release_publish_dag.py`
File: `airflow/dags/release_publish_dag.py`
- write: DataSync copy of `data/`, `changelog/`, `bridgefiles/`, `registry/` → AWS release bucket, Azure blob container, archive bucket (`:75-211`)
- write: boto3 multipart copy of PMTiles → extras bucket (`:327-332`)
- write: `PublishStac` generates/updates STAC catalog for the release (`:334-351`)
- no filter/merge/split/id-change on feature data

### 0.2 `theme_promote_dag.py` + `theme_promote.py`
File: `airflow/dags/theme_promote_dag.py`, `airflow/dags/src/public/overture_airflow/theme_promote.py`
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
- write: bridge file generation triggers `bridge_file_create_dag` per configured type (`theme_promote.py:264-289`)
- black box: `pmtiles_task` submits an AWS Batch job running the public `ghcr.io/overturemaps/overture-tiles` Docker image; tiling logic itself is not in this repo (`pmtiles.py:25-90`)
- (plumbing) `validate_final_output`: checks S3 for expected output directories (existence check only, no data transform) (`validation.py:151-181`)
- (plumbing) `cleanup_hidden_files`: S3 cleanup of hidden files

---

## 1. Collect stage — `dataset_divisions_collect_dag.py`
File: `airflow/dags/dataset_divisions_collect_dag.py`
- write: raw vendor drops land in `SourceRawBundle` paths via ECS download (`ecs_download_to_s3`)
- geoBoundaries: per-country/ADM-level API call, returns GeoJSON URL + `buildDate`-derived version (`resolve_geoboundaries`, `divisions/utils.py:104-110`)
- Dados Abertos: static URL, version = HTTP `Last-Modified` (`resolve_last_modified_version`)
- Microsoft MEVN: static URL, version = `Last-Modified`
- LINZ: Koordinates async Exports API — POST job → poll 10s → 302 to presigned S3 URL (`resolve_linz_download_url`, `divisions/utils.py:127-195`); two separate downloads (main table + major-name lookup); API key from Secrets Manager `/managed-secrets/koordinates/api_key`
- gate: `should_download` `ShortCircuitOperator` skips download when version unchanged (`:102`)
- no data transform (fetch-and-land only)

---

## 2. Ingest / normalization stage (primary sources) — `dataset_divisions_ingest_dag.py`
File: `airflow/dags/dataset_divisions_ingest_dag.py`
- all steps run `org.overturemaps.divisions.JobRunner` Scala JAR, one `--jobClass` per source

### 2.1 geoBoundaries — `GeoBoundariesCreator`
`GeoBoundariesCreator.scala`
- revision: `ADM1`→`region`, `ADM2`→`county` subtype mapping (`boundaryToSubtypeMapping`, `:33-34`)
- revision: `nameOverrides` map hardcodes corrected names for ~40 shapes (Japan, Colombia, New Zealand) (`:36-73`)
- id change / rebind (mint): division id derived from source's `shapeID`/`shapeGroup` composite

### 2.2 Dados Abertos (favelas + núcleos) — `FavelaCreator`
`FavelaCreator.scala`; one job, two GeoJSON inputs (`--inputPaths favelas,nucleos`)
- id change / rebind (mint): `id = sourcePrefix + "_" + fav.id"` → `"favela_FA_<gid>"` / `"favela_NU_<gid>"` (`:19-20, 120-125`)
- revision: geometry reprojected EPSG:31983 → WGS84 (`GeometryUtils.transformCrs`, `:29-30`)
- drop: rows with empty `names.primary` (`:99-100`)

### 2.3 LINZ — `LinzCreator`
`LinzCreator.scala`
- id change / rebind: suburb `record_id = id` from main CSV (`:45`); suburb `id = divisionId` from source row (`:70`)
- new column: synthesized `division_area`, `id = "area_$divisionId"` (`:82`)
- merge: city-level locality = union of geometries of every suburb sharing `major_name` (grouping key: `major_name`)
- id change / rebind (mint): city id = `s"Linz#city_${major_name.toLowerCase.replaceAll("\\s+","_")}"` (`:106`)
- join: city `record_id` comes from separate lookup table `nz_suburb_locality_major_name` (`--additionalDataPath`), keyed on `major_name_id` (`:109`)
- split: single-suburb "cities" filtered out of city-aggregation branch, kept as plain suburb divisions instead (group-by-then-branch split) (`:212-222`)

### 2.4 Feed changelog (all primary sources)
`feed_changelog_task_group` (shared plumbing)
- aggregation: churn vs. previous version computed; gate fails run if threshold exceeded (manual override possible)
- no record content change

---

## 3. Enrichment ingest stage — `dataset_divisions_enrichment_ingest_dag.py`
File: `airflow/dags/dataset_divisions_enrichment_ingest_dag.py`

### 3.1 Microsoft MEVN — `MevnCreator`
`MevnCreator.scala`; `--additionalDataPath` = latest published release
- join: `ExtractGersOsmIdMap(divisions)` builds GERS id → OSM id from `sources[0].record_id` of released divisions (`:46-56`)
- join: `mevnInput.col("osmId") === gersToOsmId.col("osmId")` (`:46-56`)
- id change / rebind (mint patch record): `pid = s"msft_${id}_names"`, `id` = existing GERS id, `record_id` = MEVN row's OSM id (`:56-65`)
- no drop: unmatched divisions/MEVN rows simply produce no patch record
- write: patch records feed into `DivisionsMerger` (§6.2) as enrichments

---

## 4. OSM ingest stage — `dataset_divisions_osm_ingest_dag.py`
File: `airflow/dags/dataset_divisions_osm_ingest_dag.py`

### 4.1 Point-in-time geometry build — `OSMGeometrySpecificTime` (PySpark)
`omf.osm.osm_geometry_specific_time.OSMGeometrySpecificTime`
- revision: `geometry_daily` snapshot rolled forward via OSC-changeset replay filtered to `timestamp < cut_off`, producing geometry state at the LKG instant (`:78-88`)

### 4.2 Filter to division-relevant entities — `FilterOsmInputsForDivisions`
`FilterOsmInputsForDivisions.scala`
- drop: relations kept only if `type == "relation" && !ST_IsEmpty(geometry) && (admin_level tag present || tags["place"] in {borough,neighbourhood,quarter,square,suburb,city,hamlet,town,township,village} || tags["type"]=="boundary" || id in CountryInfoList/RelationsSubtypeOverrideList keys)` (`:33-42`)
- drop: ways get the equivalent filter restricted to `POLYGON` geometry
- drop: nodes kept only if tagged `place=<locality-ish>` or explicitly overridden
- exception to drop: nodes referenced as a relation's `label` member kept even without qualifying tags (`nodesFromRelationLabels`, `:62-70`)
- drop: optional bbox filter and `SampleFilterWkts.txt` sample-country filter (dev/test only) (`:74-107`)
- revision: geometry snapped via `ST_ReducePrecision(ST_MakeValid(...), 7)` before write (`:110`)

### 4.3 Wikidata enrichment — `WikidataEnrichment`
`WikidataEnrichment.scala`
- join: OSM entities with `wikipedia` tag but no `wikidata` tag joined to Wikidata dump (`wiki_map` bundle) on Wikipedia page title (`:55-66`)
- new column: synthetic `wikidata` tag injected on match

### 4.4 OSM → Overture normalization — `OsmToDivisions`
`OsmToDivisions.scala`
- new column / revision: country assigned via point-in-polygon (nodes) or max-area-overlap (ways/relations) against an STR-tree of country polygons; antimeridian-crossing countries indexed twice, split at dateline (`CreateStrTree`, `:308-331`; `:138-235`)
- drop: relation/way with no qualifying country match gets no country ("effectively dropped downstream")
- revision: `CountryInfo.json`/`GeometryManipulations.json` `AddOsm`/`RemoveOsm`/`AddWkt`/`RemoveWkt` ops applied to country polygons before point-in-polygon test (`:77-108, 188-206`)
- id change / rebind (mint): placeholder `id = s"${osmType.shortName}$osmId"` (e.g. `"r123456"`, `"w123456"`), minted in `DivisionNormalizationHelper.mainNormalization` (`:70`)
- drop: `filteredMappedWays` anti-joins out ways whose subtype matches/groups-with (`microhood`/`neighborhood`/`macrohood`) the subtype of a relation they belong to (`:246-255`)
- merge: `LocalitiesMerging.Process` merges polygon entity (way/relation) with point entity for same locality; relation's `label`-role node supplies display point; name/population/wikidata folded in under matching-subtype conditions; standalone point marked for deletion (`LocalitiesMerging.scala:46-60`)
- drop: `highway=*` ways excluded unless tagged `area=yes` (`DivisionNormalizationHelper.scala:31-33`)

### 4.5 Water subtraction / coastline splitting — `WaterSubtract`
`WaterSubtract.scala`; `--additionalDataPath` = latest published release `theme=base/type=water`
- join: coastline division areas joined against ocean geometry from latest release
- drop: small islands (holes < 1e-5 deg² ≈ 0.088 km²) removed from ocean mask before diff (`removeSmallIslandsUdf`, `:46, 77`)
- revision: ocean geometry simplified/buffered per-country (`defaultSimplificationTolerance=0.005`, `defaultShrinkValue=-0.06`, `:32-38`)
- split: division area overlapping ocean → 2 rows: original row set `is_land=false, Class="maritime"`; new row `id = "${area.id}L"`, `is_land=true, is_territorial=false` (`WaterSubtract.scala:114-123`)
- no split if `diff.isEmpty` (single row kept unchanged)

---

## 5. Match stages — assigning GERS identity

### 5.1 Primary-source matching — `theme_divisions_match_dag.py`
- gate: `validate_provider_path` checks path's `provider=` segment matches declared `provider` param (`:79-90`)
- join: `matching_task_group` called with `input.override_paths` = ingestion run's `division`/`division_area` paths, `baseline.branch="main"` (`:108-130`)

### 5.2 OSM matching — `theme_divisions_osm_match_dag.py`
- black box / write: `baseline_export` exports current `main`-branch corpus divisions to run path via `corpus_data_export_dag` (Scala/Iceberg) (`:65-81`)
- join: OSM ingest output (`.../water_subtract/theme=divisions/type=...`) matched against exported baseline (`.../corpus/theme=divisions/type=...`)

### 5.3 The matcher itself
`matching_task_group` (`src/public/overture_airflow/matching_operator.py:36-247`), shared across buildings/places/divisions
- black box: resolves `input`/`baseline` from `override_paths` or by triggering `corpus_data_export_dag`
- black box: branches to `DatabricksMatchingOperator` or `glue_matching` (`:86-127`); divisions uses Glue (`DEFAULT_COMPUTE_PLATFORM="Glue"`, `matching_utils.py:21`)
- black box: submits shaded JAR `org/overturemaps/matching/{matcher_version}/matching-{matcher_version}-shaded.jar`, package `org.overturemaps.matching`, source not in this repo (`matching_utils.py:463`)
- black box: divisions adds `spark-nlp-assembly` dependency jar (`matching_utils.py:61-63`) — NLP-based name matching, logic not visible
- id change / rebind: final GERS identity assigned inside the matcher JAR (opaque — scoring, thresholds, id-assignment rules not visible)
- black box / write: `LoadMatchingToCorpus` (`:500-665`) optionally creates corpus branch, always triggers `corpus_data_load_dag` per table — Scala/Iceberg write, keyed `IdField=id`, tagged `Source=<provider>`

---

## 6. Assemble stage — `theme_divisions_assemble_dag.py`
Chain: `FixMatcherOutput → DivisionsMerger → TrimToCountries → DivisionsParenting → Boundaries → GeoPol → StandardizeDivisions → CreateMinbarFilter (parallel) → ValidationJob`

### 6.1 Corpus fetch + `FixMatcherOutput`
- black box: `fetch_corpus_data_<table>` ×5 tables (`division`, `division_area`, `division_boundary`, `enrichment`, `patch`) via `corpus_data_export_dag` (Scala/Iceberg export) (`:107-123`)
- revision: `FixMatcherOutput.scala` parses WKB geometry columns to JTS geometry (`ST_GeomFromWKB`)
- column removed: `names_embedding` (NLP matcher embedding vector)
- column removed: `provider`
- revision: re-encoded into internal `DivisionType`/`DivisionAreaType`/`DivisionBoundaryType` case classes (`:12-36`)

### 6.2 Merge with enrichments and patches — `DivisionsMerger`
`DivisionsMerger.scala`; config: `MergeConfig.json`
- merge: `groupById` groups all sources' rows sharing the same id — grouping key: `id` (`mergeDfs`, `:45-72`)
- join: `divisionsGrouped.joinWith(enrichmentsGrouped).joinWith(patchesGrouped)` on id
- config modes per source dataset: `"default"` (OSM), `"replace"` (geoBoundaries, LINZ — with per-entity `where`/`exceptWhere`, e.g. OSM stays authoritative for Cyprus regions and some African/Asian counties), `"union"` (Dados Abertos favelas), `"enrich"` (Microsoft MEVN, attribute-only)
- id change / rebind: `takeSourceFeature` (`:179-200`) selects exactly one source feature per id per interaction/`where`/`exceptWhere` rules; throws `"Multiple feature sources available for {id}"` on collision (`getSourceFeature`, `:207-225`) — hard failure
- revision: `applyEnrichmentsForProperty` (`:256-289`) folds enrichments/patches onto chosen source feature, attribute-by-attribute
- revision: `names` field merged via `Utils.mergeNames` (partial merge, respects `overwrite` flag)
- revision: other fields replaced reflectively via `updateProperty` (`getDeclaredFields`/constructor reflection, `:295-310`); throws if >1 non-overwrite enrichment targets same column (`:249-251`)
- revision: `mergeDivisions` (`:101-156`) propagates `country`/`region`/`names` from merged division onto its `division_area` rows
- drop: `division_boundary` rows whose `division_id` no longer exists in merged division set, dropped via left-semi-join `boundariesToRemain` (`:140-153`) — cascading from parent merge

### 6.3 Clip to country boundaries — `TrimToCountries`
`TrimToCountries.scala`
- revision: every non-country division area intersected (`OverlayNGRobust.overlay(..., INTERSECTION)`) against its own country's area polygon
- drop: only biggest resulting polygon per feature kept (`GeometryUtils.extractBiggestPolygon`, `:41`); sliver fragments outside country discarded

### 6.4 Parent/child assignment — `DivisionsParenting`
`DivisionsParenting.scala`
- new column: `parent_division_id`
- aggregation: `Window.partitionBy(col("child_id")).orderBy(desc("score"), asc("parent_area"), asc("parent_id"))`; `row_number()` filtered to `rank===1` keeps single best-scoring parent per child (`:86-93`)
- revision: three-pass matching — below-region children vs. non-country parents; regions vs. countries/dependencies; unparented vs. countries/dependencies fallback (`:59-76`)
- revision: division areas inherit `country`/`region`/`admin_level` from parent division (`:102-106`)
- revision: `ParentingUtils.setInheritanceInfo` propagates inherited fields (helper not opened)

### 6.5 Boundary computation — `Boundaries`
`Boundaries.scala` + `BoundariesUtils.scala`; `--additionalDataPath` = latest published release
- aggregation: `areasToBoundaries` computes shared-edge boundary lines between adjacent same-level division areas (country-country, region-region)
- revision: `classifyBoundaries` classifies each boundary land/maritime by intersecting with ocean geometry from latest release `theme=base/type=water`
- id change / rebind (mint, content-addressed): if not already a UUID, `id = UUID.nameUUIDFromBytes(hashInput)` where `hashInput = [subtype, is_land, is_territorial, division_ids.min, division_ids.max, is_disputed, perspectives-JSON].mkString("#")` (`BoundariesUtils.scala:372-391`)
- no id change if `Utils.isUuid(boundary.id)` already true

### 6.6 Geopolitics — `GeoPol`
`GeoPol.scala`
- revision: `DisputeInfo.json` config stamps `is_disputed`/`perspectives` onto boundaries matching configured id
- gate: job fails entirely if any configured disputed-boundary id is missing from data (`:39-44`)

### 6.7 Standardization — `StandardizeDivisions`
`StandardizeDivisions.scala`
- revision: polygon winding order forced to CCW (`ST_ForcePolygonCCW`) on division areas
- revision: `capital_division_ids`/`capital_of_divisions` rebuilt from scratch via re-explode + re-join
- drop: capital references to divisions no longer present post-merge dropped via left-semi-join `filteredExplodedCapitals` (`:32-34`)
- new column: `cartography.prominence` via `LocalityProminenceScorer.assignProminence` (helper not opened)

### 6.8 Minbar filter — `CreateMinbarFilter`
`CreateMinbarFilter.scala`; reads latest published release (not current run output)
- aggregation: produces flat id list = every country/county/region + any division with `cartography.prominence >= 70` (`:38-42`), plus their division areas and country-level boundaries
- write: side-channel filter list, not part of divisions release output; consumed downstream to scope OSM normalization elsewhere

### 6.9 Validation — `ValidationJob`
`ValidationJob.scala`, `"--suite": "Divisions"`
- runs registered test classes in parallel (Scala `Future`): churn tests, id-uniqueness, geometry validity, foreign-key checks (`overture_divisions/.../jobs/validation/tests/`)
- write: results → `validation/` path in the run's own bundle (`ValidationJob.scala:22-27`); not merged into any shared cross-theme violations table

Output (`data/theme=divisions/type=division`, `type=division_area`, `type=division_boundary`) → `finalize_bundle` (`:296`) → `theme_assemble` bundle → input to `theme_promote_dag` (§0.2).

---

## 7. Raw source stage

### 7.1 geoBoundaries, Dados Abertos, LINZ, Microsoft MEVN
- external HTTP/API delivery collected directly by `dataset_divisions_collect_dag` (§1); no producing DAG in this repo; raw-read boundary = the ECS download task

### 7.2 OSM — planet history
- write: `dataset_osm_history_dag.py`/`dataset_osm_history_reset_dag.py` convert OSM full-history planet extract into Iceberg full-history table (not divisions-specific)
- write: `dataset_osm_geometry_dag.py` produces daily OSC-based geometry snapshots (`geometry_daily`) layered on the history table
- revision: `OSMGeometrySpecificTime` (§4.1) rolls `geometry_daily` forward to exact LKG instant — this is what `FilterOsmInputsForDivisions` (§4.2) reads; divisions never reads raw planet history directly

### 7.3 Wikidata
- `wiki_map` bundle (`provider="wikidata", resource="wiki_map"`) read directly by `WikidataEnrichment` (§4.3); no producing DAG in the divisions DAG set

### 7.4 Latest published Overture release
- join: read as `--additionalDataPath` by `WaterSubtract` (§4.5, ocean geometry), `Boundaries` (§6.5, ocean geometry), `CreateMinbarFilter` (§6.8, minbar id list) — prior release used as input to current run

---

## Summary table

| Stage | Operations | Id impact |
|---|---|---|
| 0.1 release_publish_dag | write | none |
| 0.2 theme_promote_dag | join, new column, drop, aggregation, write, column removed, black box (pmtiles only) | none |
| 1. Collect | write | none |
| 2.1 GeoBoundariesCreator | revision, id change/rebind | mint |
| 2.2 FavelaCreator | id change/rebind, revision, drop | mint |
| 2.3 LinzCreator | id change/rebind, new column, merge, join, split | mint + rebind (major_name_id continuity) |
| 2.4 Feed changelog | aggregation | none |
| 3.1 MevnCreator | join, id change/rebind, write | mint (patch record) |
| 4.1 OSMGeometrySpecificTime | revision | none |
| 4.2 FilterOsmInputsForDivisions | drop, revision | none |
| 4.3 WikidataEnrichment | join, new column | none |
| 4.4 OsmToDivisions | revision, drop, id change/rebind, merge | mint |
| 4.5 WaterSubtract | join, drop, revision, split | split (id + "L") |
| 5.1 Primary-source matching | gate, join | rebind (via matcher) |
| 5.2 OSM matching | black box, join | rebind (via matcher) |
| 5.3 The matcher | black box, id change/rebind | rebind (opaque) |
| 6.1 Corpus fetch + FixMatcherOutput | black box, revision, column removed | none |
| 6.2 DivisionsMerger | merge, join, id change/rebind, revision, drop | merge (group by id) |
| 6.3 TrimToCountries | revision, drop | none |
| 6.4 DivisionsParenting | new column, aggregation, revision | none |
| 6.5 Boundaries | aggregation, revision, id change/rebind | mint (content-addressed UUID) |
| 6.6 GeoPol | revision, gate | none |
| 6.7 StandardizeDivisions | revision, drop, new column | none |
| 6.8 CreateMinbarFilter | aggregation, write | none |
| 6.9 ValidationJob | write | none |
| 7.1 Vendor raw sources | write | none |
| 7.2 OSM planet history | write, revision | none |
| 7.3 Wikidata | (read only) | none |
| 7.4 Latest published release | join | none |
