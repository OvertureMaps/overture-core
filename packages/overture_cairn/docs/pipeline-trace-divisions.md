# Divisions Theme Pipeline Trace (Release → Raw Source)

This document traces the Overture **divisions** theme backward from the published
release to the earliest raw-source read. It is research material for provenance
tracking: at each stage it records what code runs, what actually happens to the
data (filters, joins, id assignment, merges, drops, splits), and flags anything
hard to see into (violation-store writes, raw SQL, black-box/cross-language calls).

Order: this document reads **forward** (source → release) for narrative clarity,
even though it was researched backward from the release-publish DAG. See the
summary at the end for the strict pipeline order.

**Headline finding, stated up front**: divisions is architecturally different
from buildings/places in one important way. The buildings/places conflation
logic (`overture_corpus`, the matcher) is a true black box — its Scala source is
not in this repo. Divisions' theme-specific transform chain (`overture_divisions`
— merge, trim, parenting, boundaries, geopolitics, standardization, ingest
normalization for every raw source) **is** Scala, but its full source **is**
readable in this repo at `overture_divisions/src/main/scala/org/overturemaps/divisions/`,
built as a Maven artifact (`overture-divisions`) and submitted to AWS Glue as a
JAR via the same `spark_agnostic_task_group` mechanism used for PySpark jobs
elsewhere. Only one piece of the divisions pipeline is a genuine external black
box: the cross-theme entity **matcher** (`matching-<version>-shaded.jar`, package
`org.overturemaps.matching`), the same shared matching engine used by
buildings/places, pulled from CodeArtifact/Maven and run via Databricks or Glue.
Its source is not in this repo. This document therefore reads the divisions
Scala jobs directly (they answer "what happens to the data" concretely) and
treats only the matcher call as opaque.

Also notable: **no violation-store write** (`entity_violations` Iceberg
`MERGE INTO`) was found anywhere in the divisions pipeline. Data-quality checks
run as a standalone `ValidationJob` test suite that writes pass/fail results to
a `validation/` path in the run bundle — not into the shared cross-theme
violations table used by buildings/places.

---

## 0. Shared stages (near-identical across all themes)

### 0.1 `release_publish_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/release_publish_dag.py`

Takes a staged release-candidate bundle (already fully assembled/promoted, all
themes present) and fans it out to production destinations. It does not
transform feature data at all — pure data movement and cataloguing:

- DataSync copies of `data/`, `changelog/`, `bridgefiles/`, `registry/` from the
  scratch bucket to the AWS release bucket, an Azure blob container, and an
  archive bucket (`:75-211`).
- A boto3 multipart copy of PMTiles to an "extras" bucket (`:327-332`).
- Runs `PublishStac` (`overture_core.stac.job`) via serverless Fargate to
  generate/update the STAC catalog scoped to this one release (`:334-351`).
- Invalidates the STAC CloudFront distribution, starts four Glue crawlers to
  refresh the release/registry/changelog/bridge Glue catalogs (`:355-376`).
- Stamps the release-candidate bundle as `released`
  (`ReleaseCandidateBundle.tag_as_released_from_uri`, `:378-380`).

No record is filtered, merged, or re-identified here.

### 0.2 `theme_promote_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_promote_dag.py`

Parameterized over all six themes (`divisions` included, `PIPELINE_CONFIG`
`:13-20`); one DAG instance per theme (`theme_divisions_promote_dag`). Takes the
theme's `theme_assemble` output bundle as input, produces the `theme_promote`
bundle that release-publish reads. Per its own `dag_doc_md` (`:24-53`) and the
shared `theme_promote_task_group` (`src/public/overture_airflow/theme_promote.py`,
not opened here — shared plumbing, not divisions logic), it:

- validates the input theme data against `overture-schema`,
- computes an internal changelog / churn statistics,
- copies theme data into the promote staging area,
- generates PMTiles for map visualization,
- generates bridge files.

Schema validation, changelog computation, and packaging — not feature-level
filtering or re-identification. From here on, everything is divisions-specific.

---

## 1. Collect stage — `dataset_divisions_collect_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/dataset_divisions_collect_dag.py`

Runs `@daily`. This is the true raw-source read for every non-OSM division
source. Per its own doc (`:32-58`): "This is the only DAG where primary and
enrichment sources are collected together. From this point onwards they are
handled separately." No Spark here — pure HTTP fetch + `ecs_download_to_s3`
(actual download work is offloaded to ECS, not the Airflow worker) into
`SourceRawBundle` paths.

| Source | Provider | Mechanism |
|---|---|---|
| geoBoundaries | `geoboundaries` | Per-country/ADM-level API call (`resolve_geoboundaries`, `divisions/utils.py:104-110`) returns a GeoJSON download URL and a `buildDate`-derived version. |
| Dados Abertos (favelas, núcleos) | `dados_abertos` | Static URL, versioned by HTTP `Last-Modified` header (`resolve_last_modified_version`). |
| Microsoft MEVN (enrichment) | `microsoft` | Static URL, versioned by `Last-Modified`. |
| LINZ suburbs/localities + major-name table | `linz` | No permanent file URL — goes through Koordinates' async Exports API: POST export job → poll every 10s → follow a 302 to a pre-signed S3 URL (`resolve_linz_download_url`, `divisions/utils.py:127-195`). Uses an API key from Secrets Manager (`/managed-secrets/koordinates/api_key`). Two separate layers are downloaded (main table + major-name lookup) because LINZ discontinued the single combined TSV that used to carry both. |

Nothing is transformed here — this is fetch-and-land. Each `should_download`
`ShortCircuitOperator` compares versions to skip re-downloading unchanged data.

**OSM's raw read is separate and further upstream** — see §7, since OSM divisions
data is derived from the shared OSM planet history/geometry infrastructure, not
collected by this DAG.

---

## 2. Ingest / normalization stage (primary sources) — `dataset_divisions_ingest_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/dataset_divisions_ingest_dag.py`

Converts each raw vendor drop into Overture-shaped `division`/`division_area`
rows. All normalization runs as the same Scala JAR
(`org.overturemaps.divisions.JobRunner`, built from `overture_divisions/` in
this repo) submitted to AWS Glue, one `--jobClass` per source. No PySpark exists
at this layer; the only Python is Airflow wiring (bundle resolution, changelog,
skip-if-already-ingested checks).

### 2.1 geoBoundaries — `GeoBoundariesCreator`

Task: `geoboundaries.ingest.<country>_<adm_level>.normalize`
(`dataset_divisions_ingest_dag.py:145-167`), running
`org.overturemaps.divisions.jobs.dataprocessors.GeoBoundariesCreator`
(`overture_divisions/.../jobs/dataprocessors/GeoBoundariesCreator.scala`).

Reads a country/ADM-level-specific GeoJSON, maps `ADM1`→`region`,
`ADM2`→`county` (`boundaryToSubtypeMapping`, `:33-34`), and applies a
hand-maintained `nameOverrides` map (`:36-73`) for ~40 specific shapes across
Japan, Colombia, and New Zealand where the source GeoJSON's `shapeName` is
missing or wrong — a manual, entity-specific data-quality patch baked directly
into the ingest code rather than expressed as a config-driven patch record.
Division ids here are derived from the source's own `shapeID`/`shapeGroup`
composite (not shown above, but consistent with the “each source stamps its own
prefixed id” pattern used across all division ingest sources — see §2.2–2.4).

### 2.2 Dados Abertos (favelas + núcleos) — `FavelaCreator`

Task: `dados_abertos.normalize_dados_abertos`
(`dataset_divisions_ingest_dag.py:239-261`), running
`org.overturemaps.divisions.jobs.dataprocessors.FavelaCreator`
(`FavelaCreator.scala`). One job processes both GeoJSON inputs at once
(`--inputPaths favelas,nucleos`, a comma-joined dual path — an unusual
convention worth noting for provenance since it's not the normal single-path
bundle shape).

```scala
private def transformToDivisions(favelas: Dataset[Favela], sourcePrefix: String, stamp: SourceStamp) =
  favelas
    .map(fav => fav.copy(id = sourcePrefix + "_" + fav.id))(Encoders.product)
    .map(_.toDivisionAndAreaTuple(stamp))(Encoders.product)
```
(`FavelaCreator.scala:120-125`)

Each vendor record's `gid` becomes `id = "favela_FA_<gid>"` or
`"favela_NU_<gid>"` (source prefix `"FA"`/`"NU"` distinguishes the two GeoJSON
inputs before the shared `favela_` division prefix, `:19-20, 120`) — this is the
placeholder pre-match id. Geometry is reprojected from EPSG:31983 (SIRGAS 2000 /
UTM 23S) to WGS84 (`GeometryUtils.transformCrs`, `:29-30`). Rows with an empty
`names.primary` are dropped (`FavelaCreator.scala:99-100`) — the only filter in
this job.

### 2.3 LINZ — `LinzCreator`

Task: `linz.normalize_linz` (`dataset_divisions_ingest_dag.py:328-353`), running
`org.overturemaps.divisions.jobs.dataprocessors.LinzCreator`
(`LinzCreator.scala`). Two distinct entity types come out of one job:

- **Suburb/locality divisions**: `record_id = id` from the main CSV
  (`LinzCreator.scala:45`), `id = divisionId` derived directly from the source
  row (`:70`), plus a synthesized `division_area` (`id = s"area_$divisionId"`,
  `:82`).
- **City-level localities**: built by *unioning geometries of every suburb that
  shares the same `major_name`* (`LocalitiesMerging`-style aggregation is not
  used here; this is LinzCreator's own grouping). The synthesized city's id is
  deterministic from the name string itself —
  `s"Linz#city_${major_name.toLowerCase.replaceAll("\\s+", "_")}"`
  (`LinzCreator.scala:106`) — but its `record_id` (used for corpus/provenance
  history) comes from a **separate** lookup table
  (`nz_suburb_locality_major_name`, passed as `--additionalDataPath`) keyed by
  `major_name_id` (`:109`). The DAG's own comment explains why:

  > "LINZ does not provide a permanent file URL... To preserve parity with the
  > old data we now join... major_name_id, needed to maintain stable
  > city-level record IDs across dataset refreshes."
  > (`dataset_divisions_ingest_dag.py:294-299`)

  This is a deliberate identity-stability workaround: without pinning
  `record_id` to the lookup table's stable key, every LINZ refresh would look
  like a brand-new entity to the matcher, breaking corpus history for
  city-level divisions.

Single-suburb "cities" are filtered out of the city-aggregation branch and left
as plain suburb divisions (`LinzCreator.scala:212-222`) — a group-by-then-branch
split, not a drop.

### 2.4 Feed changelog (all primary sources)

Task: `<source>.feed_changelog_division[_area]`
(`feed_changelog_task_group`, shared plumbing, not divisions-specific — same as
other themes). Compares the new normalized version against the previous one and
fails the run if churn exceeds configured thresholds (manually overridable to
proceed). No record content changes here; it's a gate, not a transform.

---

## 3. Enrichment ingest stage — `dataset_divisions_enrichment_ingest_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/dataset_divisions_enrichment_ingest_dag.py`

Per its own doc: "Enrichment sources provide additional attributes... for
existing division entities identified by their GERS ID. They do not go through
the matcher." This is architecturally distinct from every other source in this
trace — it never touches the matcher or the corpus branch/load flow at all.

### 3.1 Microsoft MEVN — `MevnCreator`

Task: `microsoft.normalize_mevn_data` (`:113-134`), running
`org.overturemaps.divisions.jobs.dataprocessors.MevnCreator`
(`MevnCreator.scala`). Takes `--additionalDataPath` = the latest published
Overture release (via `get_latest_release_via_stac`).

```scala
val gersToOsmId = ExtractGersOsmIdMap(divisions)   // GERS id -> OSM id, from sources[0].record_id of the released divisions
...
mevnInput.col("osmId") === gersToOsmId.col("osmId")   // join MEVN's own OSM id to the released GERS id
```
(`MevnCreator.scala:46-56`, condensed)

Each matched MEVN row becomes a **patch record** — `pid = s"msft_${id}_names"`,
`id` = the *existing GERS id* (not a new one), `record_id` = the MEVN row's OSM
id (`:56-65`). This is a direct deterministic join against already-published
identity (extracted straight out of the release's OSM `record_id`), not a
spatial/fuzzy match. Divisions with no OSM-derived GERS id in the current
release, or whose OSM id doesn't appear in the MEVN table, simply get no
enrichment — nothing is dropped, the join just produces no patch for them.
These patch records feed directly into `DivisionsMerger` (§6.2) as
"enrichments", never into the corpus/matcher flow.

---

## 4. OSM ingest stage — `dataset_divisions_osm_ingest_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/dataset_divisions_osm_ingest_dag.py`

Doc: "Takes data from provided path, it will normalize it and create Corpus
branch to put it in." This is the OSM-specific ingest chain, run manually (not
scheduled), anchored to an OSM Nightwatch "last known good" (LKG) timestamp.

### 4.1 Point-in-time geometry build — `OSMGeometrySpecificTime` (PySpark)

Task group `build_specific_time_geometry` (`:212-230`), module
`omf.osm.osm_geometry_specific_time.OSMGeometrySpecificTime` — this is the one
**PySpark** (not Scala) step in the whole divisions ingest chain, shared
multi-theme OSM infrastructure (also used by buildings/base/transportation, not
opened in depth here). Rolls the `geometry_daily` snapshot forward by
OSC-changeset replay filtered to `timestamp < cut_off`, producing a
geometry state consistent with the exact LKG instant
(`get_osm_geometry_partition` docstring explains the off-by-one-day base
selection, `:78-88`).

### 4.2 Filter to division-relevant entities — `FilterOsmInputsForDivisions`

Task group `filter_osm_inputs` (`:236-251`), running
`org.overturemaps.divisions.jobs.FilterOsmInputsForDivisions`
(`FilterOsmInputsForDivisions.scala`). Its own docstring: created to speed up
local dev by shrinking a 1TB+ OSM extract to ~10GB of division-relevant rows.
It filters:

```scala
val filteredRelationsDF = osmDF.filter(
  col("type") === "relation" && !ST_IsEmpty(geometry) &&
  (map_contains_key(tags, "admin_level")
   || tags["place"].isin("borough","neighbourhood","quarter","square","suburb","city","hamlet","town","township","village")
   || tags["type"] === "boundary"
   || id.isInCollection(CountryInfoList.MapOfCountriesOsmId.keys ++ Resources.RelationsSubtypeOverrideList.keys)))
```
(`FilterOsmInputsForDivisions.scala:33-42`, condensed)

Ways get an equivalent filter restricted to `POLYGON` geometry; nodes are kept
only if tagged `place=<locality-ish>` or explicitly overridden. It also pulls
in any node referenced as a relation's `label` member even if the node itself
has no qualifying tags (`nodesFromRelationLabels`, `:62-70`) — those nodes carry
the display point/name used later. A configurable bbox filter and a
`SampleFilterWkts.txt`-based sample-country filter (dev/test only) can further
restrict output (`:74-107`). Geometry is snapped through
`ST_ReducePrecision(ST_MakeValid(...), 7)` before write (`:110`).

**Data effect**: everything not tagged as an administrative/populated-place
candidate is dropped at this stage — a coarse pre-filter, not the final
subtype classification (that happens in §4.4).

### 4.3 Wikidata enrichment — `WikidataEnrichment`

Task group `enrich_osm_with_wikidata` (`:253-268`), running
`org.overturemaps.divisions.jobs.preprocesing.WikidataEnrichment`
(`WikidataEnrichment.scala`). For any OSM entity with a `wikipedia` tag but no
`wikidata` tag, looks up the Wikidata ID by joining on Wikipedia page title
against a Wikidata dump (`wiki_map` bundle, ingested by a separate
`wikidata`-provider pipeline not opened in this trace) and injects a synthetic
`wikidata` tag (`:55-66`). Backfills a missing cross-reference; doesn't drop or
merge rows.

### 4.4 OSM → Overture normalization — `OsmToDivisions`

Task group `osm_normalization` (`:270-290`), running
`org.overturemaps.divisions.jobs.OsmToDivisions` (`OsmToDivisions.scala`, 332
lines — the largest single job in the divisions Scala codebase). Its own
docstring: "This is main Divisions script, it takes OSM data as input and
converts/normalises it to Overture format, it does conversion 1:1 from Osm
Nodes/Ways/Relations."

Key transform points:

- **Country assignment via point-in-polygon / max-overlap**: builds an
  in-memory STR-tree of every country polygon (with antimeridian-crossing
  countries indexed twice, split at the dateline, `CreateStrTree`,
  `:308-331`) and assigns each node/way/relation the country whose polygon
  contains it (nodes) or has maximum area overlap (ways/relations,
  `:138-235`). A relation/way with no qualifying country match gets no country
  and is effectively dropped downstream.
- **Country geometry manipulation overrides**: `CountryInfo.json` /
  `GeometryManipulations.json` define per-country `AddOsm`/`RemoveOsm`/`AddWkt`/
  `RemoveWkt` operations — e.g. a disputed territory relation gets unioned onto
  or subtracted from a country's OSM-sourced polygon before it's used as the
  country boundary for point-in-polygon tests (`:77-108, 188-206`). This is a
  config-driven correction of raw OSM geometry, not something visible in OSM
  tags — a country's *effective* territory for this pipeline can differ from
  its literal OSM relation boundary.
- **Placeholder id**: `id = s"${osmType.shortName}$osmId"` (e.g. `"r123456"`,
  `"w123456"`) minted per entity in `DivisionNormalizationHelper.mainNormalization`
  (`:70`) — pre-match, replaced later by the matcher if a GERS match is found
  (same pattern as buildings' random-uuid placeholder, except here it's
  deterministic from the OSM type+id rather than random).
- **Way suppressed if its containing relation already produced the same
  subtype**: `filteredMappedWays` anti-joins out any way whose `subtype`
  matches (or is grouped with, for `microhood`/`neighborhood`/`macrohood`) the
  subtype of a relation it's a member of (`:246-255`) — avoids double-counting
  a boundary represented both as a standalone way and as a relation member.
- **`LocalitiesMerging.Process`** (`LocalitiesMerging.scala`, 229 lines):
  merges a polygon entity (way/relation) with the point entity representing the
  same real-world locality — a relation's `label`-role node supplies the
  display point (overriding centroid) and, under matching-subtype conditions,
  its name/population/wikidata get folded in and the standalone point entity is
  marked for deletion so it doesn't ship as a duplicate feature
  (`LocalitiesMerging.scala:46-60`, condensed). This is the OSM-specific
  duplicate-collapse step — two raw OSM entities (a boundary way/relation and a
  point node) become one Overture division.
- **`highway=*` ways explicitly excluded** unless tagged `area=yes`
  (`DivisionNormalizationHelper.scala:31-33`) — a defensive filter matching a
  legacy tool's ("OMDP") behavior per an inline `TODO: Remove this if...`
  comment, i.e. a known-temporary compatibility shim.

Output feeds `WaterSubtract` directly; there is no separate ID-assignment pass
beyond the placeholder above until the matcher runs (§8).

### 4.5 Water subtraction / coastline splitting — `WaterSubtract`

Task group `water_subtract` (`:294-310`), running
`org.overturemaps.divisions.jobs.WaterSubtract` (`WaterSubtract.scala`). Takes
`--additionalDataPath` = the latest published Overture release's
`theme=base/type=water`. Its own docstring: subtracts ocean geometry from
coastline divisions, removes small islands, simplifies coastlines.

The concrete identity-changing moment — a single input division area can become
**two output rows**:

```scala
case (AreaWithoutIslands(area: DivisionAreaType, _), Row(_, oceanGeometry: Geometry, _)) =>
  val diff = area.geometry.difference(oceanGeometry)
  if (diff.isEmpty) Seq(area)
  else {
    val newArea = area.copy(id = s"${area.id}L", geometry = diff, is_land = true, is_territorial = false)
    Seq(area.copy(is_land = false, `Class` = "maritime"), newArea)
  }
```
(`WaterSubtract.scala:114-123`)

A coastline division area that overlaps ocean is **split** into the original
(now `is_land = false`, `Class = "maritime"`, representing the maritime/water
portion) and a new sibling row with an `"L"`-suffixed id (the land-only
portion, `is_territorial = false`). This is a genuine one-to-two identity split
performed purely by this job, not by the matcher or corpus. Small islands
(holes below `1e-5` deg² ≈ 0.088 km²) are dropped from the ocean mask before the
difference is computed (`removeSmallIslandsUdf`, `:46, 77`), and the ocean
geometry itself is simplified/buffered per-country before use
(`defaultSimplificationTolerance = 0.005`, `defaultShrinkValue = -0.06`,
`:32-38`) — a deliberate approximation to avoid slivers along noisy coastlines.

Output of this stage (`water_subtract/theme=divisions/type=division[_area]`) is
what the OSM match DAG (§8.2) reads as its matcher input.

---

## 5. Match stages — assigning GERS identity

Divisions matching is split into two DAGs by provider, but both call the same
underlying mechanism.

### 5.1 Primary-source matching — `theme_divisions_match_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_divisions_match_dag.py`

Manually triggered per source (`provider` param: LINZ, geoBoundaries, Dados
Abertos). Validates the path's `provider=` segment matches the declared
`provider` param (`validate_provider_path`, `:79-90`), resolves a run id/output
path (`resolve_match_run_params`), then calls:

```python
match = matching_task_group(
    dag=dag, scenario="overture", theme="divisions", provider="{{ params.provider }}",
    input=json.dumps({"branch": "", "version": "", "override_paths": {
        "division": "{{ params.ingestion_run_path }}/data/theme=divisions/type=division",
        "division_area": "{{ params.ingestion_run_path }}/data/theme=divisions/type=division_area",
    }}),
    baseline=json.dumps({"branch": "main", "version": "", "override_paths": ""}),
    ...
    output_corpus_branch="{{ params.output_corpus_branch }}",
)
```
(`theme_divisions_match_dag.py:108-130`, condensed)

### 5.2 OSM matching — `theme_divisions_osm_match_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_divisions_osm_match_dag.py`

Doc: "Takes the output of the OSM ingest run, matches it against the current
Corpus baseline, and loads the results into a Corpus branch." First exports the
current `main`-branch corpus divisions to the run path
(`baseline_export` task group, `corpus_data_export_dag`, `:65-81` — a black-box
Scala/Iceberg export, same mechanism documented for buildings/places), then
matches the OSM ingest output (`.../water_subtract/theme=divisions/type=...`)
against that exported baseline (`.../corpus/theme=divisions/type=...`), using
`matcher_env`/`output_corpus_branch` the same way as §5.1.

### 5.3 The matcher itself: a genuine external black box

`matching_task_group` (`src/public/overture_airflow/matching_operator.py:36-247`)
is **shared plumbing across buildings, places, and divisions** — not divisions
code. It:

1. Resolves `input`/`baseline` either from `override_paths` (as both divisions
   DAGs use) or by triggering `corpus_data_export_dag` for a named branch.
2. Branches on `compute_platform` to either `DatabricksMatchingOperator` or
   `glue_matching` (`:86-127`) — divisions uses Glue in this environment
   (`DEFAULT_COMPUTE_PLATFORM = "Glue"`, `matching_utils.py:21`).
3. Either path resolves and submits a **separate** shaded Maven JAR:

   ```python
   matcher_path = f"org/overturemaps/matching/{matcher_version}/matching-{matcher_version}-shaded.jar"
   ```
   (`matching_utils.py:463`, inside `get_matcher_jar_paths_task`)

   This is package `org.overturemaps.matching` — **not**
   `org.overturemaps.divisions`. Its source is not in this repo. Divisions adds
   one extra dependency jar to this job, `spark-nlp-assembly` (John Snow Labs
   NLP, version pinned in `THEME_JARS["divisions"]`, `matching_utils.py:61-63`),
   implying the divisions matcher configuration uses NLP-based name matching in
   addition to spatial matching — but the matching logic itself (spatial
   thresholds, name-similarity scoring, id assignment rules) is entirely inside
   that external JAR and is **not visible from this repo**, exactly as
   documented for buildings' `BuildingMatcher`-adjacent corpus matching and
   places' matcher in the sibling traces.
4. On success, `LoadMatchingToCorpus` (`:500-665`) optionally creates a corpus
   branch (`corpus_create_branch_dag`) and always triggers
   `corpus_data_load_dag` per table — another black-box Scala/Iceberg write,
   loading the matched (GERS-identified) rows into the named corpus branch,
   keyed by `IdField=id`, tagged `Source=<provider>`.

**What crosses the visibility boundary here**: input paths, baseline paths,
provider/theme/scenario strings, and the resulting output paths are all visible
Python. What happens *inside* the matching JAR — how a candidate division is
scored against corpus candidates, what threshold constitutes a match, how ids
are actually assigned to matched vs. unmatched entities — is opaque. This
mirrors exactly the corpus/matcher boundary already documented for buildings
(`BuildingMatcher` reads a corpus export and writes matched output, but the
underlying spatial-conflation identity assignment inside the shared matcher/
corpus stack is external) and places.

---

## 6. Assemble stage — `theme_divisions_assemble_dag.py`

File: `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_divisions_assemble_dag.py`

Doc: "Exports matched divisions data from Corpus, runs the full transform
chain, and writes the final dataset to the theme stage output bundle." Every
step is the same `org.overturemaps.divisions.JobRunner` Scala JAR as ingest,
chained: `FixMatcherOutput → DivisionsMerger → TrimToCountries →
DivisionsParenting → Boundaries → GeoPol → StandardizeDivisions →
CreateMinbarFilter (parallel) → ValidationJob`.

### 6.1 Corpus fetch + `FixMatcherOutput`

Task group `fetch_corpus_data_<table>` × 5 tables (`division`, `division_area`,
`division_boundary`, `enrichment`, `patch`, `:107-123`) — each a black-box
`corpus_data_export_dag` trigger (Scala/Iceberg export), same mechanism as
buildings' `fetch_corpus`.

`FixMatcherOutput` (`format_data` task group, `:128-144`, running
`FixMatcherOutput.scala`) is a thin reshape: reads WKB geometry columns, parses
to JTS geometry (`ST_GeomFromWKB`), drops `names_embedding` (the NLP matcher's
embedding vector — not needed downstream) and `provider` columns, and re-encodes
into the internal `DivisionType`/`DivisionAreaType`/`DivisionBoundaryType`
case classes (`FixMatcherOutput.scala:12-36`). No filtering or id changes — a
structural normalization of whatever shape the corpus export/matcher left the
data in.

### 6.2 Merge with enrichments and patches — `DivisionsMerger`

Task group `merge_data` (`:147-168`), running `DivisionsMerger.scala` (324
lines — the most complex piece of config-driven merge logic in the theme).
Config comes from `MergeConfig.json` (`overture_divisions/src/main/resources/`),
which declares, per source dataset, an `interaction` mode:

- `"default"` (OSM) — used unless a higher-priority source claims the entity.
- `"replace"` (geoBoundaries, LINZ) — wins over the default in the countries/
  subtypes it covers (with per-entity `exceptWhere`/`where` conditions, e.g. OSM
  stays authoritative for Cyprus regions and several African/Asian counties
  even though geoBoundaries is generally set to replace).
- `"union"` (Dados Abertos favelas) — added alongside, not competing for the
  same slot.
- `"enrich"` (Microsoft MEVN) — never a source feature on its own; only patches
  attributes onto whichever source won.

Core algorithm, per entity id (`mergeDfs`, `:45-72`):

```scala
val divisionsGrouped = groupById[T](divisions)         // group all sources' rows with the same id
...
divisionsGrouped.joinWith(enrichmentsGrouped, ...).joinWith(patchesGrouped, ...)
  .flatMap { case ((division, enrichment), patch) => merge(division.entities, enrichment.entities, patch.entities, mergeConfig) }
```

`takeSourceFeature` (`:179-200`) picks exactly one "source feature" for the id
based on `interaction`/`where`/`exceptWhere` matching; if more than one
candidate claims the same `replace`/`default`/`union` slot, it throws
(`getSourceFeature`, `:207-225` — "Multiple feature sources available for
{id}"), meaning a misconfigured merge or an unexpected match collision is a
hard pipeline failure, not a silently-resolved ambiguity. Enrichments/patches
are then folded onto the chosen source feature attribute-by-attribute
(`applyEnrichmentsForProperty`, `:256-289`) — special-cased for `names`
(partial merge via `Utils.mergeNames`, respecting `overwrite`) and reflective
field-by-field replacement for everything else (`updateProperty` uses
`getDeclaredFields`/constructor reflection, `:295-310`). More than one
non-overwrite enrichment on the same column is also a hard failure (`:249-251`).

After the per-table merge, `mergeDivisions` (`:101-156`) propagates
`country`/`region`/`names` from the merged division down onto its
`division_area` rows and drops any `division_boundary` row whose referenced
`division_id` no longer exists in the merged division set (`boundariesToRemain`
left-semi-join, `:140-153`) — a cascading delete driven by the parent merge
outcome, not an independent filter.

### 6.3 Clip to country boundaries — `TrimToCountries`

Task group `trim_to_country` (`:170-185`), running `TrimToCountries.scala`.
Every non-country division area is geometrically intersected
(`OverlayNGRobust.overlay(..., INTERSECTION)`) against its own country's area
polygon; only the biggest resulting polygon per feature survives
(`GeometryUtils.extractBiggestPolygon`, `:41`). This clips a division whose
source geometry extends slightly outside its stated country (a common vendor
data-quality issue) down to the country's actual extent — sliver fragments
outside the country are discarded, not kept as extra rows.

### 6.4 Parent/child assignment — `DivisionsParenting`

Task group `parenting` (`:187-203`), running `DivisionsParenting.scala`. Builds
child/parent candidate pairs via spatial intersection plus a subtype-compatible
scoring UDF (`ParentingUtils.canBeParent`/`getParentScore`, not opened in
depth — a helper module, not a job), then for each child keeps only the
**highest-scoring** parent (tie-broken by smallest parent area, then
`parent_id`):

```scala
def windowSpec = Window.partitionBy(col("child_id")).orderBy(desc("score"), asc("parent_area"), asc("parent_id"))
val bestRankedParents = allParented.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") === 1)
```
(`DivisionsParenting.scala:86-93`)

Matching runs in three passes to avoid combinatorial blowup: children below
region level against non-country parents, then regions against
countries/dependencies, then anything still unparented against
countries/dependencies as a fallback (`:59-76`). This assigns
`parent_division_id`; nothing is dropped, but every division's place in the
administrative hierarchy is decided here, and `ParentingUtils.setInheritanceInfo`
propagates inherited fields (not opened) once parents are fixed. Division
areas then inherit `country`/`region`/`admin_level` back from their parent
division (`:102-106`).

### 6.5 Boundary computation — `Boundaries`

Task group `boundaries` (`:205-222`, note: also consumes the **latest published
Overture release** via `--additionalDataPath` for ocean geometry). Running
`Boundaries.scala` + `BoundariesUtils.scala`. Computes shared-edge boundary
lines between adjacent same-level division areas (country-country,
region-region, `areasToBoundaries`), classifies each boundary as land/maritime
by intersecting with ocean geometry from the current release's
`theme=base/type=water` (`classifyBoundaries`), and mints a **deterministic
id** for every newly-created boundary:

```scala
if (Utils.isUuid(boundary.id)) boundary
else {
  val hashInput = List(
    boundary.subtype, boundary.is_land, boundary.is_territorial,
    boundary.division_ids.min, boundary.division_ids.max,
    boundary.is_disputed.toString,
    if (boundary.perspectives == null) "" else mapper.writer().writeValueAsString(boundary.perspectives),
  ).mkString("#")
  boundary.copy(id = UUID.nameUUIDFromBytes(hashInput.getBytes(StandardCharsets.UTF_8)).toString)
}
```
(`BoundariesUtils.scala:372-391`)

This is a **content-addressed id**: two boundary rows with the same subtype,
land/territorial flags, sorted division-id pair, dispute flag, and perspectives
JSON always hash to the same UUID, regardless of which run produced them —
distinct from every other id-minting mechanism seen in this trace (OSM's
type+id placeholder, vendor-source prefixed ids, the matcher's corpus-inherited
ids). It's re-derivable and stable across runs without needing corpus lookup,
as long as none of the hashed fields change.

### 6.6 Geopolitics — `GeoPol`

Task group `geopol` (`:224-239`), running `GeoPol.scala`. Reads a
hand-maintained `DisputeInfo.json` and stamps `is_disputed`/`perspectives` onto
boundaries matching a config'd id. **Fails the entire job** if any configured
disputed-boundary id doesn't appear in the data (`:39-44`) — i.e. every known
geopolitical dispute must be represented in the output, treated as a hard
correctness invariant rather than a best-effort annotation.

### 6.7 Standardization — `StandardizeDivisions`

Task group `standardize` (`:241-256`), running `StandardizeDivisions.scala`.
Final schema cleanup pass:
- Forces polygon winding order to CCW (`ST_ForcePolygonCCW`) on division areas.
- Rebuilds `capital_division_ids`/`capital_of_divisions` from scratch by
  re-exploding and re-joining the capital relationships, **filtered to capitals
  that still exist post-merge** (`filteredExplodedCapitals` left-semi-join,
  `:32-34`) — a capital reference to a division that got merged away or dropped
  earlier in the chain is silently dropped here rather than left dangling.
- Runs `LocalityProminenceScorer.assignProminence` (not opened — a scoring
  helper) to compute the `cartography.prominence` value consumed later by
  `CreateMinbarFilter`.

### 6.8 Minbar filter — `CreateMinbarFilter`

Task group `create_minbar` (`:258-273`), running `CreateMinbarFilter.scala`.
Reads from the **latest published release** (not this run's output) and
produces a flat list of ids meeting a "minimal bar" — every country/county/
region, plus any division with `cartography.prominence >= 70`
(`:38-42`), plus their division areas and country-level boundaries. This
output is not part of the divisions release data itself; it's a filter list
consumed downstream (per the file's own docstring) to restrict OSM
normalization scope elsewhere in the platform.

### 6.9 Validation — `ValidationJob`

Task group `run_tests` (`:275-294`), running `ValidationJob.scala` against the
Divisions test suite (`"--suite": "Divisions"`). Dynamically loads and runs
(in parallel, via Scala `Future`) every test class registered for the suite —
churn tests, ID-uniqueness, geometry validity, foreign-key checks, etc.
(`overture_divisions/.../jobs/validation/tests/`, not individually opened —
this is a test harness, not a data transform). **Results are written to a
`validation/` path in the run's own bundle** (`ValidationJob.scala:22-27`), not
merged into any shared cross-theme table — this is the point in the trace
where "no violation-store write" is most notable: buildings/places accumulate
violation history in a shared Iceberg table across runs; divisions' equivalent
check is self-contained per run and doesn't persist history the same way.

Output of this chain (`data/theme=divisions/type=division`,
`type=division_area`, `type=division_boundary`) is what `finalize_bundle`
(`:296`) ships as the `theme_assemble` bundle — input to `theme_promote_dag`
(§0.2).

---

## 7. Raw source stage

### 7.1 geoBoundaries, Dados Abertos, LINZ, Microsoft MEVN

All four are external HTTP/API deliveries collected directly by
`dataset_divisions_collect_dag` (§1) — no upstream producing DAG in this repo;
this pipeline's raw-read boundary is the ECS download task itself.

### 7.2 OSM — planet history

OSM divisions data traces back through the same shared multi-theme OSM ingest
infrastructure documented in the buildings trace:

1. **`dataset_osm_history_dag.py` / `dataset_osm_history_reset_dag.py`**
   (`airflow/dags/osm/`) convert OSM's full-history planet extract into an
   Iceberg full-history table. Not divisions-specific; not re-opened here.
2. **`dataset_osm_geometry_dag.py`** produces daily OSC-based geometry
   snapshots layered onto the history table (`geometry_daily`).
3. **`omf.osm.osm_geometry_specific_time.OSMGeometrySpecificTime`** (§4.1) rolls
   that daily snapshot forward to an exact "last known good" instant for this
   divisions ingest run — the actual point where OSM history becomes a
   divisions-usable geometry snapshot.

The output of step 3 is what `FilterOsmInputsForDivisions` (§4.2) reads —
divisions never reads the raw planet history table directly; it always goes
through the shared specific-time geometry build.

### 7.3 Wikidata

`wiki_map` bundle (`provider="wikidata", resource="wiki_map"`), read directly
by `WikidataEnrichment` (§4.3) — no producing DAG for this bundle exists in the
divisions DAG set; it's ingested by a separate wikidata pipeline not opened in
this trace.

### 7.4 Latest published Overture release

Both `Boundaries` (§6.5, for ocean geometry) and `CreateMinbarFilter` (§6.8,
for the minbar id list) and `WaterSubtract` (§4.5, for ocean geometry) read the
**previous released** Overture dataset (`get_latest_release_via_stac`) as an
input to the *next* release. This is a real inter-release dependency: divisions
boundary/water-subtraction quality and the minbar filter both depend on
whatever the base theme's water polygons looked like in the last release, not
on anything computed fresh in the current run.

---

## Summary: linear pipeline order (raw source → release)

1. **Vendor raw drops** (geoBoundaries API, Dados Abertos HTTP, LINZ Koordinates export API, Microsoft MEVN HTTP) — collected by `dataset_divisions_collect_dag` via ECS download. External delivery.
2. **OSM planet history + daily geometry** (`dataset_osm_history_dag`, `dataset_osm_geometry_dag`) — shared multi-theme infra, raw source read.
3. **`OSMGeometrySpecificTime`** (PySpark) — rolls daily geometry to an exact last-known-good instant. The one non-Scala transform step in the divisions pipeline.
4. **`GeoBoundariesCreator` / `FavelaCreator` / `LinzCreator`** (Scala, `dataset_divisions_ingest_dag`) — per-vendor schema normalization; each mints its own placeholder id (source-prefixed or deterministic-from-name); LINZ pins `record_id` to a stable lookup key for identity continuity.
5. **`FilterOsmInputsForDivisions`** — coarse tag-based filter cutting 1TB+ OSM extract to admin/place-tagged candidates.
6. **`WikidataEnrichment`** — backfills missing `wikidata` tags via Wikipedia-title join.
7. **`OsmToDivisions`** — 1:1 OSM entity → division conversion; point-in-polygon country assignment with config-driven geometry overrides; way/relation dedup; `LocalitiesMerging` collapses polygon+point pairs representing one locality; mints OSM-type+id placeholder ids.
8. **`WaterSubtract`** — subtracts ocean geometry from coastline divisions; **splits** overlapping areas into a maritime row and an `"L"`-suffixed land-only row.
9. **`MevnCreator`** (enrichment path, `dataset_divisions_enrichment_ingest_dag`) — joins Microsoft name variants directly onto already-published GERS ids via OSM-id lookup; produces patch records; never touches the matcher.
10. **Matching** (`theme_divisions_match_dag` for primary sources, `theme_divisions_osm_match_dag` for OSM) — **black box**: external shaded JAR (`org.overturemaps.matching`, shared with buildings/places, plus a Spark-NLP dependency for divisions), run via Glue/Databricks. Assigns final GERS identity; source not in this repo.
11. **Corpus branch create + load** (`corpus_create_branch_dag`, `corpus_data_load_dag`) — **black box** Scala/Iceberg write, per source/provider, into a named corpus branch.
12. **Corpus export** (`corpus_data_export_dag`, ×5 tables: division, division_area, division_boundary, enrichment, patch) — **black box** Scala/Iceberg export feeding the assemble run.
13. **`FixMatcherOutput`** — reshapes corpus export into internal case classes; drops the matcher's NLP embedding column.
14. **`DivisionsMerger`** — config-driven (`MergeConfig.json`) cross-source merge: one source wins per entity (`replace`/`default`/`union`), enrichments/patches folded on; cascading drop of orphaned division_area/division_boundary rows.
15. **`TrimToCountries`** — clips division areas to their country's actual extent; discards slivers outside the country.
16. **`DivisionsParenting`** — spatial+subtype scoring assigns each division exactly one best parent.
17. **`Boundaries`** — computes shared-edge boundary lines, classifies land/maritime against the latest release's water data, mints **content-addressed deterministic ids** by hashing boundary attributes.
18. **`GeoPol`** — stamps dispute/perspective info from config; hard-fails if any configured dispute is missing from the data.
19. **`StandardizeDivisions`** — final schema cleanup; drops dangling capital references; computes cartography prominence.
20. **`CreateMinbarFilter`** — produces a side-channel id filter list from the *previous* release (not part of the divisions release output itself).
21. **`ValidationJob`** — runs the Divisions test suite; writes results to the run's own `validation/` path (no shared violation-store write, unlike buildings/places).
22. **`theme_divisions_promote_dag`** (shared `theme_promote_dag`) — schema validation, changelog computation, PMTiles/bridge-file generation, staging copy.
23. **`release_publish_dag`** (shared) — DataSync/S3 copy to release/archive/Azure destinations, STAC publish, Glue crawler refresh, tag bundle as released.
