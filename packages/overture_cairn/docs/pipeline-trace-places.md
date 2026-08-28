# Places Pipeline Trace (Release → Raw Source)

Research material for overture-cairn (provenance/lineage tracker). This document traces the
PLACES theme's data flow **backward**: starting at the published release and walking back to
the earliest raw-source reads, one stage at a time. For each stage it identifies the DAG/task
that triggers it, the actual PySpark job that does the work, and — with real code excerpts —
what the job does to the data: what's filtered, dropped, merged, re-identified, or otherwise
changed.

Places has the richest DAG chain of any theme, largely because it fans out across many
third-party data providers (Foursquare, Meta, Microsoft, AllThePlaces, PinMeTo, Krick,
BrightQuery, RenderSEO, DAC, an LLM-toolkit feed, and manual "patches"), each of which must be
independently collected, Overturized, matched against a shared baseline, and only then merged
into one deduplicated places dataset. Places is also the theme with explicit **per-field
attribute merging** — for a single real-world place seen by five different providers, the
pipeline has to decide, field by field, which provider's value wins. That logic lives in the
merge stage (Stage 4 below) and is the hardest part of this pipeline to see into.

---

## Stage 1 — Release Publish (shared across all themes)

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/release_publish_dag.py`

This DAG is identical for every theme (buildings, transportation, places, etc.) — it operates
on the whole multi-theme release bundle, not on places specifically, so it's summarized briefly
here (the buildings and transportation traces will describe the same stage).

It takes a staged **release candidate** bundle (produced by `theme_promote_dag`, see Stage 2)
sitting under `s3://<scratch bucket>/staging/release_candidate/...` and publishes it as a
numbered public release:

- Verifies a `success` marker exists at the RC path (`check_release_candidate_success_file`).
- DataSyncs `data/`, `changelog/`, `bridgefiles/`, and `registry/` from the scratch bucket to
  the AWS release bucket, an Azure blob container, and an archive bucket (`create_datasync_task_group`,
  `release_publish_dag.py:314-332`).
- Copies PMTiles separately via boto3 multipart copy (large files, DataSync too slow) —
  `release_publish_dag.py:327-332`.
- Runs `PublishStac` (`overture_core.stac.job`) in single-release mode to generate/mirror STAC
  items scoped to this one release, then invalidates the STAC CloudFront distribution
  (`release_publish_dag.py:334-353`).
- Starts four Glue crawlers in the Distribution AWS account to refresh the release data catalog
  (`release_publish_dag.py:355-376`).
- Tags the RC bundle as `released` and stamps `released_at` in its `metadata.json`
  (`ReleaseCandidateBundle.tag_as_released_from_uri`, `release_publish_dag.py:378-380`) — this
  is what lets downstream jobs later call `resolve_latest_released()` to find "the current
  release" (used, for example, by the places ingest job to resolve spatial-filter datasets — see
  Stage 6).
- Updates published attribution docs (`update_docs_for_release`).

No record-level transformation happens here — this stage is pure data movement (copy/sync) plus
cataloging/metadata bookkeeping. Nothing is filtered, merged, or re-identified.

---

## Stage 2 — Theme Promote (shared across all themes, parametrized per theme)

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_promote_dag.py`
**Task group:** `theme_promote_task_group` in
`/Users/adam/OMF/tf-data-platform/airflow/dags/src/public/overture_airflow/theme_promote.py`

Also shared across all six themes (`theme_places_promote_dag`, `theme_buildings_promote_dag`,
etc. are all generated from the same `PIPELINE_CONFIG` loop in `theme_promote_dag.py:13-20`).
It reads the theme's **assembled** bundle (`ThemeAssembleBundle`, output of Stage 3 below — the
places-specific "theme stage" data) and produces a `ThemePromoteBundle`, which is what
`release_publish_dag` later consumes:

- `validate_data` — runs `overture_cdp.validate_data.ValidateDataJob` against the
  `overture-schema` package version resolved by `schema_version` (`theme_promote.py:297-316`).
  This is a real schema-validation gate: it can flag/fail records that don't conform to the
  published Overture schema for the theme.
- `compute_internal_changelog` / `compute_public_changelog` — diff this run's data against the
  previous release to produce changelog and churn statistics
  (`overture_cdp.compute_internal_changelog`, `overture_cdp.compute_public_changelog`,
  `theme_promote.py:193-198, 234-252`).
- `validate_churn` — fails the run if churn stats exceed configured thresholds
  (`validate_churn_thresholds`, `theme_promote.py:318-321`).
- `process_data` — runs `overture_cdp.process_data.ProcessDataJob` per `type=` partition
  (`theme_promote.py:219-232`). This is the actual "copy from theme-stage path to promote-stage
  path" step, and is also where theme-agnostic finishing touches (e.g. bbox/version columns) get
  applied — it's a shared cross-theme job, not places-specific, so it isn't traced in this
  document.
- `pmtiles_task` — generates PMTiles for map visualization via AWS Batch
  (`generate_theme_pmtiles`, `theme_promote.py:291-295`).
- Bridge files — for theme types configured in `THEME_BRIDGE_FILE_TYPES`, triggers
  `bridge_file_create_dag` which (for places) pulls straight from **corpus** rather than from
  the assembled data path (`use_corpus=True` path, `theme_promote.py:264-289`) — see the corpus
  discussion in Stage 5.
- `validate_final` / `cleanup_hidden_files` — final output-shape check and S3 tidy-up.

No provider-level attribute merging happens at this stage — that already happened upstream in
Stage 4. This stage's job is schema conformance, changelog/churn computation, and packaging.

---

## Stage 3 — Theme Places Assemble

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_assemble_dag.py`
**Triggers:** `corpus_data_export_dag` (twice: `TableName="place"` and `TableName="patch"`),
then `theme_places_merge_dag`

This is where the places-specific chain begins (working backward). Its job is almost entirely
orchestration glue: pull matched data and patch data back out of **corpus** (see Stage 5 for what
corpus is), then hand both to the merge job.

```python
# theme_places_assemble_dag.py:120-144
trigger_corpus_match_data_export = TriggerDagRunOperator(
    task_id="corpus_match_data_export",
    trigger_dag_id="corpus_data_export_dag",
    conf={"ThemeName": "places", "TableName": "place", "BranchName": "main"},
    wait_for_completion=True,
    ...
)
trigger_corpus_patch_data_export = TriggerDagRunOperator(
    task_id="corpus_patch_data_export",
    trigger_dag_id="corpus_data_export_dag",
    conf={"ThemeName": "places", "TableName": "patch", "BranchName": "main"},
    ...
)
...
merge_config = {
    "merge_input": merge_input,
    "patches_uri": patch_input,
    "merge_type": "non_attribute",
    "run_id": get_run_id(context),
    "match_metadata_path": match_metadata_path,
    "patch_metadata_path": patch_metadata_path,
}
```

Plain-English: corpus holds the current "main" branch of matched place records (written by the
matching stage, Stage 5) and a separate "patch" table (written directly by
`theme_places_ingest_patch_dag`, Stage 8, bypassing matching entirely). Assemble exports both out
of corpus as flat data, then triggers `theme_places_merge_dag` with `merge_type="non_attribute"`
— a specific merge mode explained in Stage 4. `use_patches` is a DAG param (default `True`); if
`False`, the patch export is skipped and `patches_uri` is `None`. No record-level transformation
happens in this DAG itself — it is pure plumbing between corpus and the merge job.

---

## Stage 4 — Theme Places Merge (attribute merging)

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_merge_dag.py`
**Job:** `PlacesMergeJob`, module `overture_places.places_merge`
(`/Users/adam/OMF/tf-data-platform/overture_places/overture_places/places_merge.py`), plus
`overture_places/overture_places/merge/*.py`

This is the stage the whole research effort is really about: for a cluster of source records that
the matcher has already decided are "the same real-world place" (Stage 5, working backward — but
chronologically this runs *after* matching), it decides field by field which source's value
becomes the final Overture record. **The merge job never mints a new id** — it groups by the
`id` column that the matcher already assigned to every record in a cluster, and simply keeps that
id on the output row.

### Entry point and mode selection

```python
# places_merge.py:10-53
class PlacesMergeJob(SparkSedonaJob):
    def execute_job(self):
        matched_uri = self.get_param("matched_uri")
        patches_uri = self.get_param("patches_uri", None)
        merged_uri = self.get_param("merged_uri")
        run_id = self.get_param("run_id")
        merge_type = self.get_param("merge_type")
        ...
        if merge_type == "non_attribute":
            NonAttributeMerger().process_feeds(...)
        elif merge_type == "basic":
            BasicAttributesMerger().process_feeds(...)
        else:
            raise ValueError(f"Invalid merge type - {merge_type}")
```

`matched_uri` is a parquet table of matched source records — one row per source-provider claim,
already carrying a shared candidate `id` from the matcher. `patches_uri` is optional. Both
arguments accept either a raw S3 path or a bundle-manifest JSON blob
(`{"data_export": [{"exportPath": ...}]}`) — `get_uri()` tries to parse it as JSON and silently
falls back to treating it as a raw path on `JSONDecodeError`. `merge_type` selects between two
very different fusion strategies (`"basic"` is the DAG's own default; `theme_places_assemble_dag`
explicitly overrides it to `"non_attribute"` for the main production line — see Stage 3).

### Grouping and "who becomes the base record" (`base_properties_feeds_merger.py:118-146`)

```python
def _merge(self, spark_session, matched_feed_df, license_priorities) -> DataFrame:
    grouped_df = matched_feed_df.groupBy("id").agg(
        collect_list(struct(*matched_feed_df.columns)).alias("grouped_data")
    )

    def merge_gers_id_matches(row):
        sorted_data = self._sort_mergeable_data(row.grouped_data, license_priorities)
        highest_ranked = sorted_data[0]
        lower_ranked = sorted_data[1:]
        return self._merge_row(highest_ranked, lower_ranked)

    processed_rdd = grouped_df.rdd.map(merge_gers_id_matches)
    return spark_session.createDataFrame(processed_rdd, schema=matched_feed_df.schema)
```

Every group (all records sharing one matcher-assigned `id`) gets ranked, and the **top-ranked
record's row becomes `base`** — both merge modes build the output starting from `base`'s own
`properties` dict and then selectively fold in the lower-ranked records. The sort key
(identical in both mergers, `basic_attributes_merger.py:16-36` / `non_attribute_merger.py:10-29`):

```python
return sorted(
    grouped_data,
    key=lambda x: (
        license_priorities[x["sources"][0]["dataset"]],
        -x["confidence"],
    ),
)
```

**Primary sort key is license permissiveness, not confidence.** From `sources/sources/places.json`,
`license_priority = {"CDLA-Permissive-2.0": 0, "CC0-1.0": 0, "Apache-2.0": 1}`. Foursquare is the
only major provider on `Apache-2.0` — meaning **any** CDLA/CC0 source (Meta, Microsoft, PinMeTo,
Krick, RenderSEO, DAC, BrightQuery, AllThePlaces) outranks Foursquare as the base record,
*regardless of confidence score*, purely because Overture's output license terms constrain which
data can be used as the unattributed "authoritative" seed. Only within the same license tier does
descending confidence break the tie. The dataset lookup (`sources[0]["dataset"]`) is positional —
it assumes each record's first `sources` entry is always its own whole-POI attribution, which is a
convention, not something enforced here.

### Per-field "which source wins" — `BasicAttributesMerger._merge_row` (`basic_attributes_merger.py:38-94`)

```python
properties_to_ignore = {"version", "confidence", "brand", "addresses"}

def _merge_row(self, base, lower_ranked):
    properties = {k: getattr(base, k) for k in base.asDict().keys() if k not in {"id", "lowerRankedFeaturesProperties"}}
    if lower_ranked is None:
        return Row(id=base.id, **properties)

    new_confidence = 1 - properties["confidence"]
    for feature_properties in lower_ranked:
        new_confidence *= 1 - feature_properties["confidence"]
        for key, value in feature_properties.asDict().items():
            if key not in self.properties_to_ignore:
                if key == "sources" and value is not None:
                    properties[key] = SourcesMerger.merge(properties.get(key), value)
                elif isinstance(value, list):
                    properties.setdefault(key, [])
                    properties[key] = ListMerger.merge(properties[key], value)
                elif key == "categories" and value is not None:
                    properties[key] = CategoryMerger.merge(properties.get(key), value)
                elif key == "names" and value is not None:
                    properties[key] = NamesMerger.merge(properties.get(key), value)

    properties["sources"].append(self.confidence_source)
    properties["confidence"] = 1 - new_confidence
    return Row(id=base.id, **properties)
```

The result is seeded entirely from `base` (the highest license/confidence-ranked record), then
selectively overwritten as lower-ranked records fold in, in ranked order:

| Field | Winner logic |
|---|---|
| `geometry` | **Base only, verbatim.** Not a list, not one of the special-cased keys — never touched after seeding from `base`. |
| `websites`, `emails`, `socials`, `phones` | `ListMerger.merge` — **union, de-duplicated**, base's items first, order-preserving. Everything survives; no "best wins" here. |
| `categories` (`{primary, alternate[]}`) | `CategoryMerger.merge` — **base's `primary` always wins.** A differing lower-ranked `primary` is demoted into `alternate` (if not already present); the lower-ranked record's own `alternate` entries are folded in too, de-duped. No category information is discarded, only demoted. |
| `names` (`{primary, common{}, rules[]}`) | `NamesMerger.merge` — **base's `primary` wins.** A differing lower-ranked primary is demoted into `rules` as `{"variant": None, "value": <other_primary>}`. `common` (localized names) merges **gap-filling only** — a lower-ranked entry for a language key is used only if base doesn't already have a non-null value for that key. `rules` lists are unioned with exact-match dedup. |
| `sources` | `SourcesMerger.merge` — **only the lower-ranked record's own whole-POI source entry** (the one where `property` is `None`/`""`) is appended, and only the *first* such match (`break` after one hit) — attribute-level provenance entries from lower-ranked records are not carried forward by this path. |
| `brand`, `addresses` | **`properties_to_ignore` — base only, always.** Notably `addresses` is schema-typed as a list and would otherwise be eligible for `ListMerger`'s union, but the ignore-set check runs before the list-type check, so addresses are silently **never unioned** across sources — easy to miss from the schema alone. |
| `version` | **`properties_to_ignore` — base only**, left untouched by this job. |
| `confidence` | Explicitly ignored in the per-record loop, then recomputed once at the end (see below) — not a per-source "winner," a derived value. |
| `operating_status`, `basic_category`, `taxonomy` | No special-case branch and not a list type, so these fall through untouched — **base wins, silently, by omission** rather than by explicit rule. |

### Confidence recombination and provenance stamping (both merge modes)

```python
# basic_attributes_merger.py / non_attribute_merger.py
new_confidence = 1 - properties["confidence"]
for feature_properties in lower_ranked:
    new_confidence *= 1 - feature_properties["confidence"]
properties["confidence"] = 1 - new_confidence
```

Treats each source's confidence as an independent probability the place exists, and combines
complements: `merged_confidence = 1 - Π(1 - confidence_i)` across the whole cluster — more
agreeing sources push confidence up, regardless of which source contributed which attribute
value. Every merged record also gets a synthetic **"Overture confidence calculation" source**
appended to `sources`, dated to the pipeline run (`base_properties_feeds_merger.py:33-44`):

```python
self.confidence_source = {
    "property": "/properties/confidence", "dataset": "Overture",
    "license": "CDLA-Permissive-2.0", "record_id": None,
    "update_time": date_from_run_id, "provider": "overture",
    "resource": "confidence_calculation", "version": confidence_source_version,
}
```

This is the provenance trail documenting that the confidence value was *derived*, not sourced
from any single provider.

### `NonAttributeMerger` — the cheap, "existence-only" mode (`non_attribute_merger.py:9-62`)

Used by the actual production line (`theme_places_assemble_dag` sets `merge_type="non_attribute"`).
It keeps the highest-ranked record's data **wholesale, with no per-field fusion at all**:

```python
def _merge_row(self, base, lower_ranked):
    properties = {k: v for k, v in base.asDict().items() if k not in ("id", "lowerRankedFeaturesProperties")}
    if lower_ranked is None:
        return Row(id=base.id, **properties)
    new_confidence = 1 - properties["confidence"]
    for prop in lower_ranked:
        new_confidence *= (1 - prop["confidence"])
    properties["sources"] = SourcesMerger.change_source_prop_to_empty_string(properties["sources"])
    properties["sources"].append(self.confidence_source)
    properties["confidence"] = 1 - new_confidence
    return Row(id=base.id, **properties)
```

Lower-ranked records contribute **only their confidence value** — names, categories, geometry,
socials, everything else from every other provider in the cluster is discarded entirely, not even
demoted to alternates. This mode exists to record "N sources agree this place exists" without
doing real attribute fusion. Note: the module's own docstring claims a `/properties/existence`
source is added per lower-ranked feature, but the code only ever appends the shared
`confidence_source` (`/properties/confidence`) — **a documentation/behavior mismatch**; no
`/properties/existence` source is actually created anywhere in this file.

### Patches — a separate overlay applied *after* merge (`properties_patcher.py`)

Patches are joined onto the already-merged output purely by the merged record's `id`
(`base_properties_feeds_merger.py:148-182`, `merged_places.id == patches.id`), a left join so
places with no patches pass through unchanged. Patches are never applied to individual pre-merge
source records — they always land on top of the finished merge result. Every patch's `type` must
be `UPSERT` or `DELETE`; anything else hard-fails the job (`_validate_patch_types`).

Conflict resolution differs by attribute kind (`sort_patches_by_attribute`,
`properties_patcher.py:94-197`):

- **Single-value attributes** (`geometry`, `confidence`, `brand.wikidata`, `brand.names.primary`,
  `version`, `names.primary`, `operating_status`) — **only the single newest UPSERT wins**
  (newest by `update_time` across the patch's own `sources`). If there's no UPSERT at all, the
  first DELETE applies instead. This is last-writer-wins by source timestamp, not by patch
  arrival order.
- **Everything else** (multi-value/list-like attributes) — **all patches for that attribute are
  kept and replayed in sequence**, ordered oldest-source-first with DELETE before UPSERT on ties
  — so a signal pipeline can cleanly retract an old value and add a new one for the same
  attribute in a single pass.

Every applied patch (except confidence/existence patches) has its own `sources` normalized and
appended to the record's `sources` array — so a patched record's `sources` accumulates: the
original merge's whole-POI attributions, the synthetic confidence source, and one source entry
per applied patch. This is the second (and final) place attribute-level provenance gets recorded
on a places record before assembly.

### Dedup / drop / filter logic in this stage

**None.** No cluster-size threshold, no "drop clusters below N sources," no required-field
`dropna` exists anywhere in the merge code — the only validation is the patch-type hard-fail
above. Every `id` group present in `matched_uri`, including singleton clusters (a single
unmatched source record), produces exactly one output row. Any completeness/size filtering for
places must happen upstream in the matcher (Stage 5) or downstream in assembly/promote validation
(Stages 2-3).

### Notable hard-to-trace items

- `get_uri()`'s JSON-or-raw-path parsing silently swallows `JSONDecodeError` but would let any
  other exception (e.g. a missing `data_export` key) propagate uncaught — an implicit contract
  with whatever upstream bundle producer populates `matched_uri`/`patches_uri`.
- `SourcesMerger.merge`'s `break` after the first whole-POI match means if a lower-ranked record
  somehow carries multiple whole-POI-attribution source entries, only one survives into the
  merged output.
- `SparkSedonaJob` (the shared base class) can optionally wrap `execute_job()` in
  `overture_cairn` provenance tracking when `cairn_run_id`/`cairn_provenance_uri` params are
  present — `theme_places_merge_dag.py` does not appear to pass either, so cairn coverage of this
  specific stage is likely not active today.
- No raw SQL anywhere in the merge code — all transforms are DataFrame API (`groupBy`, `agg`,
  `collect_list`, `join`) plus RDD `.map()` callbacks running plain-Python merge logic per group.
  The one Sedona spatial function call, `expr("ST_GeomFromWKB(geometry)")` in the final write
  step, is not a query.
- No dynamic/plugin dispatch — attribute mergers (`CategoryMerger`, `NamesMerger`,
  `SourcesMerger`, `ListMerger`) are a fixed, statically-imported `if/elif` chain inside
  `BasicAttributesMerger._merge_row`, easy to trace but requiring a direct edit to add any new
  special-cased attribute.

---

## Stage 5 — Theme Places Match, and the Corpus store

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_match_dag.py`
**Task group:** `matching_task_group` in
`/Users/adam/OMF/tf-data-platform/airflow/dags/src/public/overture_airflow/matching_operator.py`
(implementation detail in the sibling `matching_utils.py`)

This is the stage where the pipeline decides **which candidate records across providers are the
same real-world place**. It runs once per provider per ingest cycle (triggered from
`theme_places_ingest_orchestrator_dag`, see Stage 6), matching that provider's freshly-Overturized
feed against a baseline (normally the current "main" branch of corpus).

### What "corpus" is

Corpus (`/Users/adam/OMF/tf-data-platform/overture_corpus/`) is a separate Scala/Spark service
(package `org.overturemaps.store`, entry point `JobRunner`) that manages Apache Iceberg tables as
a versioned, **branchable** data store, sitting behind an AWS Glue Iceberg REST catalog
(`spark.sql.catalog.<cat>.uri = https://glue.us-west-2.amazonaws.com/iceberg`). Airflow talks to
it only through a handful of job "verbs" invoked as Spark jobs: `RegisterThemeType`, `DataLoad`,
`DataExport`, `CreateBranch`, `GetBranches`, `GetBranchVersions`.

**This is a black box from the Python pipeline's point of view.** The DAGs know only the
parameter surface (`ThemeName`, `TableName`, `BranchName`, `Version`, `InputPath`, `IdField`,
`Source`) and whatever paths/JSON the Scala job hands back on completion. How corpus diffs a load
against a branch's prior state, how it computes its own internal changelog side-table, and how
branches/versions are represented internally are all opaque Scala logic this trace did not (and,
from the Airflow layer, cannot) inspect.

### The matcher call itself is also a black box

`matching_task_group` (`matching_operator.py:62-246`) does, in order:

1. **`parse_inputs` / `parse_baseline`** — resolve the input/baseline data paths. If explicit
   `override_paths` aren't given, it triggers `corpus_data_export_dag` (a pure read/export, no
   transform — see below) to pull the current branch/version state out of corpus.
2. **`choose_matching_platform`** branches to either a Databricks job or a Glue job.
3. **The actual match/cluster decision — entirely external to this repo:**

   - *Databricks path* calls a **pre-registered Databricks job by name** (e.g.
     `"[places] Matching Pipeline"`) via the Databricks Jobs API and pulls the output of its
     `"assign"` task:
     ```python
     # matching_utils.py:226-236
     job_run_id = db_operator.run_now(job_parameters=job_parameters, job_name=self.job_name)
     output = db_operator.get_job_output(
         task_key_contains="assign", job_run_id=job_run_id, deserialize=True,
     )
     output = output["resultDataPaths"]
     ```
     The notebook/task graph that does the actual candidate clustering lives entirely on the
     Databricks workspace, not in this repo.

   - *Glue path* (the one actually wired into places matching) resolves a **separately-versioned
     Scala matcher JAR** from CodeArtifact (repo `overture-matchers`) and runs it as a Spark job:
     ```python
     # matching_utils.py:445-465
     matcher_version = Variable.get("matching_scala_version", ...).strip()
     matcher_path = f"org/overturemaps/matching/{matcher_version}/matching-{matcher_version}-shaded.jar"
     matcher_url = f"https://aws:{codeartifact_token}@{DOMAIN}-{ACCOUNT}.d.codeartifact.us-west-2.amazonaws.com/maven/{REPO}/{matcher_path}"
     ```
     ```python
     # matching_utils.py:511-522
     matching = spark_agnostic_task_group(
         group_id="matching",
         class_name="org.overturemaps.matching.Main",
         parameters=matcher_conf,   # --scenario --theme --provider --input --baseline
                                     # --outputPath --runId --embeddingsCacheTable
                                     # --matchHistory --outputDebug
         spark_cluster_desired_worker_cores="980",  # places-specific
         spark_cluster_desired_workers="31",
     )
     ```

   **In both paths, the candidate-clustering / same-place decision logic is entirely opaque to
   tf-data-platform.** `overture-matchers` is a separate Scala codebase published as a versioned
   artifact; the Databricks notebook graph is likewise external. This repo only ever supplies a
   flat parameter dict (including the embeddings cache table built in Stage 6's ingest job, used
   for candidate generation) and receives back a map of output paths per table, plus records that
   already carry a decided `id` field.

4. **`write_match_output_metadata`** stamps a `metadata.json` recording input/baseline/result
   paths (bundle pattern).
5. **`load_corpus`**: gated to run only when `scenario == "overture"` and an
   `output_corpus_branch` is set. If the target branch isn't `main`, it first triggers
   `corpus_create_branch_dag` to fork a branch off whatever baseline was matched against, then
   loads results via `corpus_data_load_dag`:
   ```python
   # matching_operator.py:643-651
   trigger = TriggerDagRunOperator(
       task_id="load_corpus",
       trigger_dag_id="corpus_data_load_dag",
       conf={
           "ThemeName": theme, "TableName": table, "BranchName": branch,
           "InputPath": data_path, "IdField": "id", "Source": provider,
       },
       wait_for_completion=True,
   )
   ```
   **`IdField` is hardcoded to `"id"`** — this is the field corpus treats as the stable record
   identity going forward. Corpus does not resolve identity itself; it is a pass-through
   versioned store keyed on whatever `id` the matcher already assigned to each output record.
   Inside `corpus_data_load_dag`, this becomes a call to the Scala `DataLoad` job
   (`class_name="org.overturemaps.store.JobRunner"`, `parameters={"job-class": "DataLoad",
   "matchedFeedInputPath": InputPath, "idField": IdField, ...}`) which, with
   `computeChangelog=True` (default), also appends entity-level changelog rows to an Iceberg
   side table as part of the load — again opaque Scala logic.

### `corpus_data_export_dag` — the read side (pure export, no transform)

Per its own docstring: validates the table exists, collects source metadata from an Iceberg
metadata side table, exports the requested branch/version to S3 as parquet (optionally splitting
active vs. deleted records into `corpus/` and `corpus_deletes/`, or combining them with a
`status` field when `CombineOutput=True`). No matching/merge logic happens here — this is the
exact DAG `theme_places_assemble_dag` calls twice to pull `place` and `patch` data back out for
merge (Stage 3/4), and the same DAG `matching_task_group` calls to pull its own input/baseline.

### Matcher telemetry (side branch)

`emit_matcher_telemetry` runs `overture_telemetry.matcher_telemetry_job.MatcherTelemetryJob`,
which reads a metrics parquet file (written matcher-side by a Scala `MetricsOrchestrator`) and
forwards it to a centralized metrics store tagged `stage="matching"`. Pure metrics relay — not
part of the matching decision path.

### Plain-English summary of Stage 5

```
per-provider Overturized feed (Stage 6 output)
        │
        ▼
matching_task_group (per provider)
  ├─ parse_inputs / parse_baseline → corpus_data_export_dag (read, no transform)
  ├─ BLACK BOX: overture-matchers (Scala JAR) or a Databricks notebook graph
  │     decides which candidate records across providers are "the same place"
  │     and assigns/preserves an `id` on each
  ├─ write_match_output_metadata
  └─ load_corpus
        ├─ corpus_create_branch_dag (fork a branch off the baseline, if not main)
        └─ corpus_data_load_dag (TableName="place", IdField="id") → WRITE into corpus Iceberg table
        │
        ▼ (later, separate trigger)
theme_places_assemble_dag → corpus_data_export_dag ×2 (place, patch) → PlacesMergeJob (Stage 4)
```

The one and only point where "same real-world place" gets decided is inside the external matcher
job. None of that decision logic is visible in tf-data-platform; the Airflow layer supplies
input/baseline paths and provider/theme parameters and receives back paths plus an `id` per
record, which it loads verbatim into corpus. This lines up with the OMF pipeline-architecture
tenet "GERS assignment happens at Match → Store → Merge, never Match → Merge → Store."

**Black-box / external calls flagged in this stage:**
- Databricks Jobs API call to a named external job (candidate clustering logic not in this repo)
- `overture-matchers` Scala JAR pulled from CodeArtifact at a pinned version (Airflow Variable
  `matching_scala_version`) — a separate codebase entirely
- `overture_corpus` Scala JAR (`org.overturemaps.store.JobRunner`) for `DataLoad`/`DataExport`/
  `CreateBranch` — opaque Iceberg diff/versioning/changelog logic
- AWS Glue Iceberg REST catalog — branch/version semantics are native Iceberg branching features
  managed entirely inside AWS Glue, invisible to Python
- CodeArtifact auth token is embedded directly into the JAR download URL
  (`matching_utils.py:456-465`) — a build-artifact fetch, not a data secret, but worth noting for
  credential-handling review

No raw SQL was found anywhere in the matching/corpus DAG code — all Iceberg/Glue interaction goes
through Spark catalog configuration and boto3 Glue API calls, never literal SQL text.

---

## Stage 6 — Theme Places Ingest Orchestrator

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_ingest_orchestrator_dag.py`

Scheduled weekly (Tuesdays 00:00), this DAG is pure orchestration — it contains no record-level
transform code itself, but it is the hub that decides, per provider, whether there's new data to
process and drives the whole Collect → Ingest → Match sequence:

1. For each configured provider (Meta, Microsoft, Foursquare, PinMeTo, AllThePlaces, Krick,
   BrightQuery, RenderSEO, DAC, Patches — `theme_places_ingest_orchestrator_dag.py:60-110`),
   `check_provider_changes` compares the provider's raw S3 last-modified time against stored
   ingestion metadata (`s3://<managed_bucket_feeds_ingest>/ingestion_metadata/<provider>/places/`).
   If unchanged and `force_ingest` is false, that provider is skipped entirely for this run.
2. For providers with new data (non-patch), triggers `theme_places_collect_dag` (Stage 9), then
   `theme_places_ingest_dag` (Stage 7), then builds a per-provider matcher config and triggers
   `theme_places_match_dag` (Stage 5).
3. For the special `Patches` "provider," collection is skipped (patches are collected directly by
   `PatchesIngestJob` from a fixed S3 URI) and it goes straight to `theme_places_ingest_patch_dag`
   (Stage 8), which bypasses matching entirely.
4. On success, updates the provider's stored ingestion metadata (`last_ingestion_time`,
   `ds_partition`) so the next weekly run can detect whether there's anything new.
5. Also threads through the four optional spatial-filter dataset paths
   (`places_country_boundaries_path`, `places_overture_water_path`, `places_disputed_areas_path`,
   `places_h3_index_cache_prefix`) and `enable_spatial_filters`, used by Stage 7's spatial
   country/water filter.

No filtering/merging of place records happens in this DAG — it is scheduling and change-detection
logic only.

---

## Stage 7 — Theme Places Ingest ("Overturize")

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_ingest_dag.py`
**Job:** `PlacesFeedIngestJob`, module `overture_places.places_feed_ingest`
(`/Users/adam/OMF/tf-data-platform/overture_places/overture_places/places_feed_ingest.py`)

This is the biggest single transform stage in the whole places pipeline: it takes one provider's
raw collected data and reshapes it into Overture's places attribute schema — the "Overturize"
step — filtering out bad records along the way. It runs **once per provider**, independently; no
cross-provider merging happens here (that's Stage 4).

### Entry point and pipeline shape

`PlacesFeedIngestJob.execute_job()` (`places_feed_ingest.py:1-149`) is a thin wrapper: it reads
job parameters, looks up the provider implementation class from a static registry keyed by
provider name (`places_data_providers/providers.py`, e.g. `PROVIDERS = {"Meta": MetaPlacesProvider,
"Foursquare": FoursquarePlacesProvider, ...}`), and calls `provider.ingest(...)`. All the actual
pipeline logic lives in the shared base class `PlacesDataProvider.ingest()`
(`places_data_providers/places_data_provider.py:537-674`), which every provider subclasses:

```
raw_df                = self._load(...)                           # provider-specific reader
normalized_df         = self._normalize(...)                      # provider-specific → Overture schema
downsized_df          = self._filter_by_bbox(normalized_df, bbox) # optional geo clip (silent drop)
categorized_df        = self._categorize(...)                     # taxonomy matching + candidate ledger
operating_status_ckd  = self._check_operating_status(...)         # default-fill only, never drops
patched_df            = self._apply_patches(...)                  # provider hook, no-op by default
pre_geocode_filtered  = self._pre_geocode_filter(...)              # provider hook, no-op by default
legacy_category_df    = self._get_legacy_category(...)            # back-fill deprecated `categories`
geocoded_df           = self._geocode(...)   (if requires_geocoding)
filtered_df           = self._filter(...)  -> PlacesFilterChain.apply_filters()   # DROP / FLAG
post_processed_df     = self._post_process(...)                   # cosmetic null-normalization
validated_df          = self._validate(...)                       # schema-shape assertion, raises on mismatch
validated_df.write.parquet(s3_output_prefix_uri, mode="overwrite")
```

### Raw → Overture schema mapping (provider-specific `_normalize`)

Each provider hand-rolls its own `_load`/`_normalize`. Example, Foursquare
(`impl/foursquare_places_provider.py:107-203`):

```python
raw_fsq_category = element_at(col("fsq_category_labels"), 1)
fsq_category = when(
    raw_fsq_category.isNotNull(),
    trim(element_at(split(raw_fsq_category, ">"), size(split(raw_fsq_category, ">")))),
).otherwise(None)
...
df = input_df.select(
    col("fsq_place_id").alias("id"),
    expr("ST_AsBinary(ST_Point(longitude, latitude))").alias("geometry"),
    lit(FOURSQUARE_DEFAULT_CONFIDENCE).cast(DoubleType()).alias("confidence"),
    fsq_category.alias(self.source_category),
    ...
)
```

Before `_normalize` even runs, Foursquare applies a hand-fit **logistic regression**
(hardcoded coefficients, not a loaded model artifact) scoring attribution quality from
website/phone/social presence and data freshness, silently dropping records with
`attribution_score <= 0.65`, records in an excluded-category list, and closed places — this drop
happens *before* the filter chain and is **not** logged to the invalid-features/violation store.

Meta (`impl/meta_places_provider.py:16-78`) is close to a pure column rename with no
pre-filtering. AllThePlaces maps OSM tags to a curated taxonomy CSV and overrides `_categorize`
to hard-drop any record whose taxonomy never resolved (`.filter(col("taxonomy").isNotNull())`).
BrightQuery (`requires_geocoding=True`) never produces a `geometry` column in `_normalize` at
all — only lat/lon "bias hints" — because geometry is populated later, entirely by the TomTom
geocoding step.

**ID note:** no new id is minted at this stage. `places_feed_schema.py:84`: *"we require providers
to include their own record id or GERS"* — every provider comment confirms it's carrying forward
the provider's own native id (`# Not GERS, Meta's FB Page ID`, etc.). GERS-id assignment happens
later, at Match → Store → Merge.

### Filters that DROP a record (`PlacesFilterChain`, `places_filter_chain.py`)

Orchestrated by `apply_filters()` (`places_filter_chain.py:107-249`), which runs cheap filters
first and the expensive spatial join last, against only the records that survived so far
(`left_anti` join, lines 154-171):

- **Duplicate provider id** — one row per `id` kept, via a window function ordered by
  `names.primary` (`places_filter_chain.py:124-139, 693`).
- **Duplicate by attributes** — groups on `(wkt_geometry, name_primary, category_primary)`;
  any group with `count > 1` is entirely flagged (`places_filter_chain.py:746`).
- **Missing required fields** — driven by `get_required_fields()` walking the schema for
  non-nullable leaf fields (`id`, `geometry`, `sources`, plus nested fields like
  `categories.primary` only when their parent struct is itself non-null) (`places_filter_chain.py:825`).
- **Invalid geometry** — out-of-range lat/lon or exactly `(0, 0)`:
  ```python
  valid_conditions = (
      expr("x").between(-180, 180) & expr("y").between(-90, 90)
      & (expr("x") != 0) & (expr("y") != 0)
  )
  ```
  (`places_filter_chain.py:524`)
- **Invalid country boundary** — a Sedona spatial join against Overture Divisions
  (country/dependency polygons buffered 300m), a disputed-areas GeoJSON, and Overture water
  tiles, with an explicit decision hierarchy (`spatial_filter_mixin.py:211-437, 413-421`):
  declared-country-matches → keep; declared-but-mismatched-in-disputed-area → keep;
  declared-but-mismatched → reject; declared-but-unrecognized-country-code → reject;
  no-declared-country-and-not-in-water → keep; no-declared-country-and-in-water → reject. Country
  codes get aliased first (`UK`→`GB`, `AN`→`AW/BQ/CW/SX`, `PS`→`XG/XW`) since Overture Divisions
  doesn't recognize some legacy codes.
- **Invalid categories** — a *soft* filter: checked against a broadcast CSV allowlist, but per an
  explicit design comment the record is **kept** with `categories`/`taxonomy` nulled out rather
  than dropped (`places_filter_chain.py:574`, docstring: *"records are kept with their categories
  and taxonomy nullified instead of being excluded"*).
- **Name matches address component** — AllThePlaces-only (conditionally registered,
  `places_filter_chain.py:75-78`): drops records where `names.primary` equals the normalized
  address locality, "a known AllThePlaces data-quality issue that can create bad place-match
  decisions downstream" (`places_filter_chain.py:620`).
- **Bbox clipping** — applied earlier, right after `_normalize`, as a Sedona `ST_Intersects`
  predicate (`places_data_provider.py:120-152`) — a silent geographic crop, not part of the
  filter chain and not logged as a violation.

### Category matching (`category_matching/category_matcher.py`)

Escalating match strategy per unique raw category string (`_match_single_category`, lines
289-352): (1) source-specific curated mapping in `overture_taxonomy.json`, (2) direct match
against taxonomy leaf names, (3) reuse of a mapping learned from a *different* provider, (4)
semantic match via `sentence-transformers/multi-qa-mpnet-base-cos-v1` embeddings with cosine
similarity threshold `0.85`, (5) no match → `None`. A no-match is **not dropped here** — it flows
downstream to the soft category filter above (nulled, kept), except for AllThePlaces which
hard-drops it. Every genuinely new mapping is applied immediately (deterministic henceforth) *and*
appended to an append-only Iceberg audit ledger (`CategoryCandidateLedger.append()`) consumed only
by a separate weekly human-review workflow — per its own docstring, *"Matching is deterministic,
so this is audit-not-gate."*

### Geocoding — TomTom API (black-box third-party call)

Only for providers with `requires_geocoding=True` (currently BrightQuery, also Krick per the
orchestrator config). `TomTomGeocoder` (`util/tomtom_geocoder.py`) pulls its API key from **AWS
Secrets Manager** (`/managed-secrets/tomtom/geocoder_api_key`), then calls out from Spark
executors:

```python
url = f"https://api.tomtom.com/search/2/geocode/{urllib.parse.quote(query)}.json"
params = {"key": api_key}
...
r = self._session().get(url, params=params, timeout=self.timeout_secs)
```

with per-partition thread pools, QPS throttling, and retry/backoff on `429/500/502/503/504`.
Only `"Point Address"` results with `score >= 0.7` are accepted. Results are cached in an
Iceberg table keyed by a SHA-256 hash of normalized address fields + rounded bias coordinates
(anti-join for cache misses, `MERGE INTO` to write results back) — this is the one external
network dependency in the whole ingest job.

### Invalid/rejected-features side output → feeds the entity-violations store

`PlacesFilterChain._write_invalid_df` always writes (even if empty) to
`places_invalid_features_repository_uri`, using a shared schema (`get_feature_violations_schema()`,
`overture_spark/overture_spark/entity_violations.py:14-37`): `dataset` (prefixed
`places/{provider}`), `violation_name`, `id`, `version`, `severity`, `geometry`, `context` (a
per-filter JSON blob), and `counterpart` (linking a dropped duplicate to the record kept for it).

**This is the hand-off into the violation-store write** (see below). When `unfiltered_ingest=True`
(dev-only), the filter chain is skipped entirely and an empty violations frame is written instead.

#### Side write: entity violations store

**Task:** `update_entity_violations` in `theme_places_ingest_dag.py:536-561`, running
`omf.entity_violations.entity_violations_update.EntityViolationsUpdate`
(`/Users/adam/OMF/tf-data-platform/omf/omf/entity_violations/entity_violations_update.py`).

```python
# theme_places_ingest_dag.py:544-557
parameters={
    "input_path": provider_params["places_invalid_features_repository_uri"],
    "violations": ",".join([
        "duplicate_provider_id", "missing_required_fields", "duplicate_by_attributes",
        "invalid_geometry", "invalid_country_boundary", "invalid_categories",
        "name_matches_address_component",
    ]),
    "entity_violations_table": get_entity_violation_table(),
}
```

This job filters the invalid-features parquet down to only these seven violation names (any
other tag is silently dropped, not persisted), then performs an Iceberg **`MERGE INTO`** keyed on
`(id, violation_name, version, dataset, counterpart)` — existing rows get `severity`/`geometry`/
`context` refreshed, new ones inserted. **Flag: this is a violation-store write, and it's a
terminal sink** — nothing here re-injects, retries, or routes rejected records back into the
places feed, corpus, or merge. It never deletes stale rows either (a separate maintenance job
handles Iceberg snapshot/compaction). It exists purely for audit/observability; the rejected
records themselves are already gone from the pipeline the moment `PlacesFeedIngestJob` wrote them
to `invalid_features/`.

#### Side write: places embeddings cache

**DAG:** `theme_places_embed_dag.py`, triggered from `theme_places_ingest_dag.py:578-588`
immediately after `places_feed_ingest` completes (running in parallel with
`update_entity_violations`, `feed_changelog`, `ingest_attribute_completeness`).
**Job:** `PlacesEmbedJob`, module `overture_places.places_embed`
(model logic shared from `overture_places_embeddings/overture_places_embeddings/core.py`).

This is a **side-branch cache-builder, not a stage on the main record stream** — its output isn't
read again until the separate Match stage (Stage 5), where it's passed to the matcher as
`embeddings_cache_table` to aid candidate lookup.

- Model: a pinned SentenceTransformer, `jinaai/jina-embeddings-v5-text-nano-text-matching` at a
  fixed revision, defined once as the single source of truth shared between this job and the
  Scala matcher (so vectors are byte-identical between "build" and "serve" — no train/serve
  drift, per the module's own docstring).
- Text embedded: `names.primary` (lowercased/trimmed) and `taxonomy.primary` (underscores →
  spaces) from the just-ingested feed.
- Cache key: SHA-256 hash of the normalized text, paired with `field_type` (`"name"` or
  `"taxonomy"`) — a **global content-addressed cache** shared across all providers; any place
  with the same normalized name/taxonomy text shares one cached embedding regardless of provider
  or record identity.
- Processing: left-anti join against the existing Iceberg cache to find only new
  `(hash, field_type)` pairs, embeds only the misses, L2-normalizes then int8-quantizes vectors
  to shrink storage, appends new rows only (`writeTo(table).append()`).

Downstream, the matcher's Airflow wiring passes this exact table name through as
`--embeddingsCacheTable`, used purely as a candidate-lookup/scoring aid (cosine-similarity
feature) — not as a source of truth for any record field.

### Feed changelog and attribute-completeness telemetry (side branches)

`feed_changelog_task_group` (`overture_cdp` changelog jobs) diffs this provider's new feed
version against the previous one for changelog/churn stats; `ingest_attribute_completeness` runs
`overture_telemetry.attribute_completeness_job.AttributeCompletenessJob` to record per-field
completeness metrics. Neither modifies the record stream — both are metrics side-branches.

### Explicit flags for this stage

- **Raw SQL** (Iceberg DDL/DML via `spark.sql(...)`): `CREATE TABLE IF NOT EXISTS ... USING
  iceberg` for both the category-candidate ledger and the TomTom geocoder cache table, and a
  `MERGE INTO {cache_table} ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT *` to
  merge freshly geocoded rows into that cache.
- **Black-box third-party calls**: the TomTom Geocoding API (only for `requires_geocoding=True`
  providers); the `sentence-transformers/multi-qa-mpnet-base-cos-v1` pretrained model used for
  semantic category matching (loaded locally, not an API call, but still an opaque third-party
  artifact).
- **Provider dispatch**: `PROVIDERS` in `places_data_providers/providers.py` is a static dict, not
  true dynamic/reflective plugin loading — but it is a runtime string-keyed dispatch
  (`provider_name` param from Airflow), so tracing "what code actually ran for provider X" always
  requires this one extra hop.
- **Unfiltered-mode divergence**: `unfiltered_ingest=True` (dev-only) changes behavior *inside
  individual providers* (Foursquare skips its ML attribution filter, AllThePlaces skips dedup and
  keeps taxonomy-unmatched rows, BrightQuery skips its pre-geocode filter) rather than being
  centralized in `PlacesFilterChain` — provenance tracking for "what got dropped" under
  unfiltered mode requires checking each provider individually.
- Several provider-embedded pre-filters (Foursquare's logistic-regression attribution score,
  AllThePlaces's per-id dedup-by-tag-richness) drop records **before** the shared filter chain
  runs, and are never logged to the entity-violations store — they're invisible to that audit
  trail.

---

## Stage 8 — Theme Places Ingest Patch

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_ingest_patch_dag.py`
**Job:** `PatchesIngestJob`, module `overture_places.patches_ingest`
(`/Users/adam/OMF/tf-data-platform/overture_places/overture_places/patches_ingest.py`)

Patches are a separate, matching-bypassing lane: a "patch" is a single proposed edit to one
attribute of one *already-known* Overture place (identified directly by `pid`), not a full place
record, so there's nothing to cluster/match — it goes straight into corpus.

Schema (`patches_ingest.py:25-34`):

```python
PATCHES_SCHEMA = StructType([
    StructField("pid", StringType(), True),        # place ID being patched
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("attribute", StringType(), True),   # which field to patch
    StructField("value", StringType(), True),       # proposed new value
    StructField("sources", ArrayType(RAW_SOURCE_SCHEMA), True),
])
```

**Input:** `PATCHES_RAW_DATA_URI = s3://3ppp-output-places-omf/` (a third-party "signals"
producer bucket), read as Spark **structured streaming**:

```python
# patches_ingest.py:49-53
source_df = (
    spark.readStream.schema(self.PATCHES_SCHEMA)
    .option("recursiveFileLookup", "true")
    .parquet(input_uri)
)
```

**Transform:** derives a `version` from the source file's own path
(`.../(manual|scheduled)__<version>/...`, falling back to the job's `version` param), hard-failing
if neither yields a parseable date — every patch must land with a resolvable, date-versioned
provenance, no silent "unknown" fallback. It then rewrites each `sources[]` entry, stamping
`provider="overture"` and `resource=f"{attribute}_signal"` onto every source struct — this is the
provenance record that survives into the corpus patch table. No filtering, no dedup, no id
assignment: this is a pure pass-through/reshape plus version-stamping.

**Output:** appended (streaming, checkpointed) to a `SourceIngestBundle` data path, then loaded
directly into corpus:

```python
# theme_places_ingest_patch_dag.py:110-124
corpus_load = TriggerDagRunOperator(
    task_id="trigger_corpus_patch_data_load",
    trigger_dag_id="corpus_data_load_dag",
    conf={
        "ThemeName": "places", "TableName": "patch", "BranchName": "main",
        "InputPath": output_bundle.data_uri + "/*.parquet",
        "IdField": "pid", "Source": "Overture-signals",
    },
)
```

Note `IdField="pid"` here vs. `IdField="id"` for matched place records (Stage 5) — patches key
directly by the existing place id they're patching, confirming they never go through the matcher.
`properties_patcher.py` (documented in Stage 4) is what later reads this patch table at merge
time and applies `attribute`/`value` onto the matching `pid`'s merged record.

---

## Stage 9 — Theme Places Collect

**DAG:** `/Users/adam/OMF/tf-data-platform/airflow/dags/theme_places_collect_dag.py`

Pure data movement, no transform: uses AWS DataSync (`DataSyncOperator`,
`theme_places_collect_dag.py:111-130`) to copy a provider's raw data from wherever it landed
(`omf-places-data-providers/<provider>/ds=<version>/` or `meta-overture-staging/...`) into the
internal source-data bucket, under a `SourceRawBundle` path
(`s3://<managed_bucket_source_data>/datasets/...`). `Options: {"PreserveDeletedFiles": "REMOVE"}`
means the destination mirrors the source exactly (files removed at source are removed at
destination too). No records are filtered, mapped, or re-identified — this is the boundary where
raw provider bytes cross from an external-facing bucket into Overture's own managed pipeline.

---

## Stage 10 — Source-Specific Collectors (earliest raw reads)

These are the true starting points of the places pipeline — the first place any Overture code
touches external, third-party data. Each is a `SparkSedonaJob` subclass that runs on a Glue/Spark
worker purely for IAM access to S3/Secrets Manager; none does real distributed Spark
transformation — they're single-node file-transfer/filter jobs wrapped in the job framework.
**None assigns any Overture/GERS id** — records keep whatever identifier the source system used
natively. Each writes NDJSON/parquet to `s3://omf-places-data-providers/<provider>/ds=<date>/`
plus a `metadata.json` marker, which `theme_places_ingest_orchestrator_dag` (Stage 6) later
detects as "new provider data."

### AllThePlaces

**DAG:** `source_places_alltheplaces_collect_dag.py`
**Job:** `AllThePlacesCollectJob`, module `overture_places.alltheplaces_collect`

**External system (black box):** `alltheplaces.xyz`, a community scraping-aggregator project —
no Overture code produced this data. Fetches `latest.json` via a bare `urlopen`
(`alltheplaces_collect.py:71-83`) to find the current run's output zip URL, then downloads the
full zip of per-spider `.geojson` files (`alltheplaces_collect.py:97-111`).

**Filters — drop records before they even become "raw" Overture data:**

```python
# alltheplaces_collect.py:12-13
ACCEPTABLE_LICENSES = {"creative commons zero", "cc0"}
# alltheplaces_collect.py:15-16
FILTER_OUT_SPIDERS = {"moneygram", "little_free_library", "gbfs"}
```
```python
# alltheplaces_collect.py:137-150
if dataset_attr.get("@spider") in FILTER_OUT_SPIDERS:
    skipped_spider += 1
    del geojson
    continue
license_str = dataset_attr.get("license")
if license_str and license_str.lower() not in ACCEPTABLE_LICENSES:
    skipped_license += 1
    del geojson
    continue
```

Only CC0-licensed spiders survive, plus a hardcoded spider exclusion list. (Files lacking
`dataset_attributes` entirely bypass both checks.) Output: one NDJSON file per run, each feature
stamped with `atp_run_end_datetime`, uploaded to
`s3://omf-places-data-providers/alltheplaces/ds=<run_date>/<run_date_compact>_alltheplaces.json`.

### Foursquare

**DAG:** `source_places_foursquare_collect_dag.py`
**Job:** `FoursquareCollectJob`, module `overture_places.foursquare_collect`

**External system (black box):** the HuggingFace Hub dataset
`datasets/foursquare/fsq-os-places/release`, accessed via `HfFileSystem`, authenticated with a
token pulled from **AWS Secrets Manager** (`/managed-secrets/huggingface_api_token`) —
consistent with boto3+Secrets Manager for credentials.

**No filter/license/quality logic at all** — per the job's own docstring: *"Downloads the latest
Foursquare OS Places parquet files from HuggingFace and uploads them as-is to S3. No Spark
transformations — just file transfers."* It finds the latest `dt=` partition, skips entirely if
that release's `metadata.json` already exists in S3 (idempotency), then parallel-copies each
`.parquet` file byte-for-byte. Rows keep Foursquare's native `fsq_place_id` untouched.

### LLM Toolkit

**DAG:** `source_places_llm_toolkit_collect_dag.py`
**Job:** `LLMToolkitCollectJob`, module `overture_places.llm_toolkit_collect`

**External system (black box):** an HTTP API at `https://places-llm-api-test.ds.io` ("Places LLM
Toolkit API") — the one collector where an LLM produced the underlying data, via a separate,
independently-run upstream extraction system out of scope for this pipeline's own code. This job
only consumes already-published results from that toolkit; it does not itself call an LLM.

**Filter/quality logic, applied server-side via query params — before any record is even
downloaded:**

```python
# llm_toolkit_collect.py:33-42
min_quality_score = float(self.get_param("min_quality_score", "0.7"))
is_open_license = self.get_param("is_open_license", "true") == "true"
commercial_use_allowed = self.get_param("commercial_use_allowed", "true") == "true"
```
```python
# llm_toolkit_collect.py:92-97
if is_open_license:
    params["is_open_license"] = "true"
if commercial_use_allowed:
    params["commercial_use_allowed"] = "true"
if min_quality_score > 0:
    params["min_quality_score"] = min_quality_score
```

Defaults mean anything below a 0.7 quality score, non-open-license, or non-commercial-use is
dropped by default — entirely by the external API, invisible to any code in this repo. The job
fails loudly if the filters match nothing (`RuntimeError("No results matched the configured
filters...")`). Each matching result's `output.parquet` is downloaded via a redirect-following GET
to a presigned S3 URL (another external hop, owned by the LLM Toolkit service). Output: one
parquet per published "result" at `s3://omf-places-data-providers/llm_toolkit/ds=<today>/
<dataset_name>_<result_id[:8]>.parquet`. No id assignment — whatever row-level ids the LLM
Toolkit produced pass through untouched.

---

## Side pipelines noted but not traced

Per scope, `airflow/dags/places/` contains several evaluation/quality/feature-enrichment DAGs that
read from places data (mainly from corpus) but do not feed back into the main
Collect → Ingest → Match → Merge → Assemble → Promote → Publish line traced above:

- `places_eval_coverage_dag.py`, `places_eval_precision_dag.py`, `places_eval_stats_dag.py` —
  run `placeeval` subcommands on ECS (coverage/precision/duplicates/churn stats) against
  published data, for internal quality monitoring.
- `feature_places_quality_compute_dag.py` — runs `QualityScoreJob`, reading `corpus_places.place`
  from the Glue REST Iceberg catalog and scoring each place with a bundled XGBoost model; writes
  a quality score, doesn't alter the places records themselves.
- `feature_places_quality_cross_theme_dag.py` — computes spatial/textual features between places
  and other Overture themes.
- `feature_places_quality_threeppp_dag.py` — runs a "3PPP" confidence-feature producer against
  raw 3PPP patch parquet (the same `3ppp-output-places-omf` bucket patches are sourced from).
- `feature_places_website_resolve_dag.py` — a five-stage ECS pipeline that resolves place website
  URLs (discovery → resolution → merge back).

These are quality-metric/enrichment side branches, not part of the data that ends up in a
release.

---

## Summary: full stage list, raw source to release

In pipeline (forward) order — reverse of how this document was researched:

1. **Source-specific collectors** (`source_places_alltheplaces_collect_dag.py`,
   `source_places_foursquare_collect_dag.py`, `source_places_llm_toolkit_collect_dag.py`, plus
   manual drops for Meta/Microsoft/PinMeTo/Krick/BrightQuery/RenderSEO/DAC) — pull raw data from
   external third-party systems (alltheplaces.xyz, HuggingFace, an LLM Toolkit API, provider
   uploads) into `omf-places-data-providers`, with source-specific license/quality filters.
2. **`theme_places_collect_dag`** — DataSync copy of a provider's raw data into Overture's
   internal source-data bucket. Pure movement, no transform.
3. **`theme_places_ingest_orchestrator_dag`** — weekly scheduler; detects new provider data and
   drives Collect → Ingest → Match per provider.
4. **`theme_places_ingest_dag` → `PlacesFeedIngestJob`** ("Overturize") — maps raw provider
   records to Overture's places schema; runs dedup, geometry/country-boundary/category/
   name-vs-address filters; geocodes address-only providers via TomTom; writes rejected records
   to an invalid-features sink.
   - **Side: `update_entity_violations`** — merges rejected records into the `entity_violations`
     Iceberg table (audit sink, terminal, no feedback into the pipeline).
   - **Side: `theme_places_embed_dag` → `PlacesEmbedJob`** — builds a content-addressed name/
     taxonomy embeddings cache, consumed later by the matcher.
5. **`theme_places_ingest_patch_dag` → `PatchesIngestJob`** — ingests third-party "signal"
   patches (single-attribute edits keyed by existing place id `pid`) and loads them directly to
   corpus's patch table, bypassing matching.
6. **`theme_places_match_dag` → `matching_task_group`** — matches a provider's feed against a
   corpus baseline; the actual candidate-clustering/same-place decision runs in an external Scala
   matcher JAR (`overture-matchers`) or Databricks job, entirely opaque to this repo; loads
   results into corpus (Iceberg, versioned/branchable, itself opaque Scala) keyed by `id`.
7. **`theme_places_assemble_dag`** — exports the matched `place` table and the `patch` table back
   out of corpus, then triggers merge.
8. **`theme_places_merge_dag` → `PlacesMergeJob`** — groups matched records by their
   matcher-assigned id; for each cluster, ranks records by license permissiveness then confidence
   and picks a base record; in `basic` mode blends names/categories/sources/contact lists
   field-by-field (base's `primary` wins, others demoted to alternates; lists unioned; geometry/
   brand/addresses/version taken from base only) while `non_attribute` mode (the production
   default) keeps only the base record's data and folds in just the confidence signal; applies
   patches as a final id-keyed overlay with last-write-wins-by-timestamp for single-value fields
   and full replay for multi-value fields.
9. **`theme_promote_dag`** (shared) — schema-validates the assembled data, computes changelog/
   churn stats, generates PMTiles and bridge files, packages the promote bundle.
10. **`release_publish_dag`** (shared) — copies the release-candidate bundle to AWS/Azure/archive
    buckets, publishes STAC items, refreshes Glue catalogs, tags the bundle as released.
