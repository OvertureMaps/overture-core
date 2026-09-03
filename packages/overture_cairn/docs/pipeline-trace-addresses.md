# Addresses Theme: Data Flow Trace (Release → Raw Source)

This document traces the ADDRESSES theme backward from release publish to the
earliest raw-source read. It is research material for provenance-tracking
design: every section identifies the DAG/task and job that runs, shows the
concrete transform code, and explains in plain English what happens to the
data — what comes in, what changes, what leaves, and whether records are
dropped, merged, split, or given a new identity.

## Architecture finding, up front

**There are two parallel, non-connected implementations of "collect address
data" in this repo today:**

1. **The legacy `omf` pipeline** (`omf/omf/addr/...`), fed by
   `source_addresses_collect_dag.py` (ECS Fargate + a monolithic
   `address_ingestion` CLI). This is the pipeline `theme_addresses_stage_dag.py`
   actually reads from, and is therefore the one that currently produces the
   released `addresses` theme.
2. **The new `overture_addresses` pipeline** (`overture_addresses/overture_addresses/...`),
   fed by `dataset_addresses_collect_dag.py` (serverless Python `CollectionJob`)
   and normalized by `dataset_addresses_ingest_dag.py` (`BatchIngestJob`). Per
   `airflow/dags/addresses/README.md`, this is the "Active" collect → ingest
   half of a planned **collect → ingest → match** pipeline. Its final stage,
   `theme_addresses_match_dag`, is listed as **"Planned"** — it does not exist
   yet. The ingest output schema (`overture_addresses/overture_addresses/ingest/schema.py`)
   explicitly documents this: `id` is `null` at ingest, with the comment
   *"id is populated by the match stage (post-match UUID)."*

Both pipelines are documented below, in the order the task instructions listed
them (which is also the practical dependency order: theme_stage lists
`source_addresses_collect_dag` output as its own input; the ingest DAG is a
separate, not-yet-wired-up branch building toward the future match-based
architecture). The **id-assignment boundary** the user's prior investigation
flagged is confirmed here: for the new pipeline, GERS/stable-ID assignment
happens in a downstream match job that is not present in this codebase yet
(only "Planned" in the README) — that is a real, current gap, not something to
chase further. The legacy pipeline (which is what actually ships today) does
its own internal ID stabilization in a "stage 2" job, described below, which
is NOT GERS assignment — it's a same-pipeline UUID-stability step.

---

## 1. Release Publish (shared across all themes)

**File**: `airflow/dags/release_publish_dag.py`

This DAG does not transform theme data — it distributes an already-assembled
release candidate bundle. Given `input_source_path` (a release-candidate
bundle produced by an earlier, separate stage) and `output_release_version`,
it:

- DataSyncs `data/`, `changelog/`, `bridgefiles/`, `registry/` from the scratch
  bucket to the managed release bucket (AWS), an Azure storage account, and an
  archive bucket.
- Copies PMTiles via boto3 multipart S3 copy (bypasses DataSync — files are
  25–150GB).
- Runs `PublishStac` (`overture_core.stac.job`) on Fargate to generate/publish
  STAC items scoped to this one release.
- Invalidates the STAC CloudFront distribution.
- Starts four Glue crawlers (`Overture`, `Overture Changelogs`,
  `Overture bridge files`, `Overture registry`) to refresh the release catalog.
- Writes a `released` marker + `released_at` timestamp on the RC bundle's
  `metadata.json` via `ReleaseCandidateBundle.tag_as_released_from_uri`.

No per-record logic touches addresses data here; it is pure movement/publishing
of files that were already finalized upstream. Not addresses-specific.

---

## 2. Theme Promote (shared across all themes)

**File**: `airflow/dags/theme_promote_dag.py`, task group builder:
`airflow/dags/src/public/overture_airflow/theme_promote.py`

This DAG takes the addresses `theme_stage` bundle (see §3 below,
`ThemeAssembleBundle`, `ROOT_PATH = "theme_stage"`) as input and produces the
`theme_promote` bundle that release_publish later ships. It is shared
plumbing across all six themes (`addresses`, `base`, `buildings`, `divisions`,
`places`, `transportation`) — the same generic `overture_cdp` Spark jobs run
per theme, addresses included. Key steps (from
`_theme_promote_task_group` in `theme_promote.py`):

```python
process_data = spark_agnostic_mapped_task_group(
    group_id="process_data",
    ...
    module_name="overture_cdp.process_data",
    class_name="ProcessDataJob",
    ...
)
...
validate_data = spark_agnostic_mapped_task_group(
    group_id="validate_data",
    ...
    module_name="overture_cdp.validate_data",
    class_name="ValidateDataJob",
    ...
)
...
compute_public_changelog = spark_agnostic_mapped_task_group(
    group_id="compute_public_changelog",
    ...
    module_name="overture_cdp.compute_public_changelog",
    class_name="ComputePublicChangelogJob",
    ...
)
```

In plain English: for each `type=` partition discovered under the input
theme_stage data (for addresses, this is `type=address`), it validates schema
conformance (`ValidateDataJob`, ignoring `bbox` and `version` fields),
computes an internal changelog and public changelog/churn stats against the
previous release, copies/rewrites the data into the promote bundle
(`ProcessDataJob` — this is the actual theme data copy, with optional bbox
clipping), generates PMTiles for map visualization (`AWS Batch`), and
optionally generates bridge files (config-driven per theme via
`THEME_BRIDGE_FILE_TYPES`; not deeply examined here since it's shared/generic).
`validate_churn_thresholds` can fail the run if churn between releases exceeds
configured thresholds. None of this is addresses-specific — it is documented
in more depth elsewhere in the repo (`overture_cdp` package). No IDs are
reassigned here; it is schema validation, changelog diffing, and format
packaging on data that already has whatever IDs the theme_stage step gave it.

---

## 3. Theme Addresses Stage — the pipeline that actually produces the release

**File**: `airflow/dags/theme_addresses_stage_dag.py`

Two sequential Spark jobs, both in the legacy `omf.addr.scripts.glue` module,
run against the addresses theme:

### 3a. Stage 1 — `OvertureAddresses` (normalize + candidate ID)

**Task**: `stage_1_no_ids` task group (`theme_addresses_stage_dag.py:91-105`)
**Job**: `omf/omf/addr/scripts/glue.py:24` `class OvertureAddresses(SparkSedonaJob)`

```python
class OvertureAddresses(SparkSedonaJob):
    def execute_job(self):
        storage_root = self.get_param("storage_root")
        fs = fsspec.filesystem("s3")
        self.repo = SimpleRawSourceRepo(fs=fs, storage_root=Path(storage_root))
        output = self.get_param("s3_output_path")
        session = OvertureSession(self.spark, self.repo)
        df = session.build_dataset(Planet())
        df.withColumn("geometry", ST_AsBinary(col("geometry"))).write.parquet(
            output, mode="overwrite"
        )
```

`storage_root` is the raw data written by `source_addresses_collect_dag`
(§4 below) — the DAG param pattern is literally
`^s3://.+/collection/addresses/run=[^<>]+/$`.

`Planet` (`omf/omf/addr/sources/planet.py`) is a `UnionedDataset` of ~38
per-country/source `AddressDataset` classes (Austria, Australia, Belgium,
Brazil, Czechia, Canada, Switzerland, Chile, Colombia, Denmark, Germany,
Estonia, Spain, Finland, Faroe Islands, France, Greenland, Hong Kong, Croatia,
Iceland, Italy, Japan, Liechtenstein, Lithuania, Luxembourg, Latvia, Mexico,
Netherlands, Norway, New Zealand, Poland, Portugal, Serbia, Singapore,
Slovenia, Slovakia, Taiwan, **US**, Uruguay). `US` (`omf/omf/addr/sources/us.py`)
is itself a large union of NAD (national fallback) plus ~150 OA per-county/
per-state datasets. Each dataset's `to_df` is called and the results are
`unionAll`'d:

```python
def to_df(self, spark, input_source_paths):
    dfs = [dataset.to_df(spark, input_source_paths) for dataset in self.datasets]
    df = union_all(dfs)
    return df
```

Every concrete dataset subclasses `BasicAddressDataset`
(`omf/omf/addr/base.py:50`), whose `to_df` is the common per-source transform:

```python
def to_df(self, spark, input_source_paths):
    df = self.load(spark, input_source_paths)
    df = self.preprocess(df)
    df = df.select(
        self.id_col().alias("id"),
        self.version_col().alias("version"),
        self.street_col().alias("street"),
        self.number_col().alias("number"),
        self.unit_col().alias("unit"),
        self.postcode_col().alias("postcode"),
        self.postal_city_col().alias("postal_city"),
        self.address_levels_col().cast(address_levels_type).alias("address_levels"),
        self.country_col().alias("country"),
        self.geometry_col().alias("geometry"),
        self.sources_col().cast(sources_type).alias("sources"),
    )
    ... # strip whitespace/control chars on street/number/unit/postcode/postal_city
    df = self.postprocess(df)
    return df
```

**What happens to the data**: each source's raw records (OA GeoJSON, NAD CSV,
various country-specific shapefiles/CSVs — country modules `be.py`, `br.py`,
`cz.py`, `de.py`, `isl.py`, `jp.py`, `nl.py`, `no.py`, `pl.py`, `si.py`,
`tw.py` weren't individually read in this pass, but follow the same
`BasicAddressDataset` contract) are mapped field-by-field into the Overture
address shape (`street`, `number`, `unit`, `postcode`, `postal_city`,
`address_levels`, `country`, `geometry`, `sources`). Each record gets a **new,
random UUID as its id** — `omf/omf/addr/overture_id.py`:

```python
def overture_id():
    return expr("uuid()")
```

This is a **candidate id, not a GERS id** — it is not stable across runs at
this point; stage 2 fixes that. Per-source `preprocess`/`postprocess` hooks
apply filters: e.g. `OpenAddressDataset` (`omf/omf/addr/sources/open_addresses.py`)
nulls out disallowed placeholder tokens (`"unknown"`, `"n/a"`, `"s/n"`, etc.)
per field via `none_if_matches_any`, several countries (`CA`, `CL`, `EE`,
`FI`, `PT`, `SK`) drop rows with no street/number/unit at all
(`filter_empty_street_number_unit`), Italy and Mexico bbox-filter stray
geocoding errors, Australia filters out `unit` containing "carspace", Mexico
filters "calle ninguno"/"sn" placeholder rows, New Zealand wraps
out-of-range longitudes, France strips embedded newlines from `number`.

**US-specific merge/dedup logic** (`omf/omf/addr/sources/us.py`, class `US`):
NAD (national fallback coverage) is loaded, then two `left_anti` spatial joins
remove NAD records that fall inside regions where a trusted, better OA source
exists — NYC boroughs and ~150 TIGER counties/states (California, Minnesota,
Mississippi, Wisconsin, Georgia, Colorado, Florida, Massachusetts, Oregon):

```python
nad_cut = nad.alias("nad").join(
    broadcast(nyc_borough_boundaries).alias("nyc_borough_boundaries"),
    expr("ST_Contains(nyc_borough_boundaries.geometry, nad.geometry)"),
    how="left_anti",
)
nad_cut = nad_cut.alias("nad").join(
    broadcast(us_county_boundaries).alias("us_county_boundaries"),
    expr("ST_Contains(us_county_boundaries.geometry, nad.geometry)"),
    how="left_anti",
)
```

The two Mississippi statewide OA files are themselves `left_anti` clipped by
the same county boundaries so the 25 MS per-county OA files win in those
counties (no double-counting). The clipped NAD plus ~150 per-county/per-state
OA datasets are then unioned into one US dataframe. This is a real
geometry-driven precedence/dedup decision: OA wins over NAD wherever OA has
per-county coverage; NAD only fills gaps.

**Output**: one Overture-address-schema Parquet dataset at
`{run}/stage_1/theme=addresses`, with a random per-row UUID `id` and no
cross-release ID stability yet.

### 3b. Stage 2 — `AddressIDAssignment` (dedup + baseline ID stabilization)

**Task**: `stage_2_id_assignment` task group (`theme_addresses_stage_dag.py:107-123`)
**Job**: `omf/omf/addr/scripts/glue.py:42` `class AddressIDAssignment(SparkSedonaJob)`

This is the **stable-ID-assignment step for this theme**. It is explicitly
*not* GERS assignment — it is a same-pipeline mechanism to keep the same UUID
across releases for records that look unchanged.

**Step 1 — exact-duplicate collapse within the candidate set** (raw SQL, run
via `self.spark.sql(...)`):

```sql
SELECT
    candidate.*
FROM candidate
JOIN (
    SELECT MIN(id) AS id
    FROM candidate
    WHERE geometry IS NOT NULL
    GROUP BY address_levels, country, postcode, street, number, unit, postal_city, geometry
) AS unique_candidate
ON candidate.id = unique_candidate.id
WHERE candidate.geometry IS NOT NULL
```

**What happens**: any two stage-1 rows that are byte-identical across
`address_levels, country, postcode, street, number, unit, postal_city,
geometry` are collapsed to one row (the one with the lexicographically
smallest UUID survives — an arbitrary but deterministic tie-break). Rows with
null geometry are dropped entirely here. This is a genuine **dedup that
destroys record identity** — if two source datasets independently reported
the exact same address, only one row survives and there's no trace in the
output of which one "won" beyond its `sources` array.

**Step 2 — baseline join for ID stability** (H3-indexed left join against the
previous release):

```sql
SELECT
    c.*,
    COALESCE(b.id, c.id) as result_id
FROM candidate_unique c
LEFT JOIN baseline_unique b
ON
    c.h3 = b.h3
    AND c.address_levels <=> b.address_levels
    AND c.country <=> b.country
    AND c.postcode <=> b.postcode
    AND c.street <=> b.street
    AND c.number <=> b.number
    AND c.unit <=> b.unit
    AND c.postal_city <=> b.postal_city
    AND ST_X(...) = ST_X(...) AND ST_Y(...) = ST_Y(...)
```

Both candidate and baseline (previous release, `id_assignment_baseline` param)
are first deduped the same way and indexed by an H3 level-10 cell for the join.
The matcher is exact-field equality — "the initial address 'matcher' just sees
if all fields are identical... If any of them change at all we end up with new
IDs" (comment in source). Where a candidate matches a baseline row, it inherits
the baseline's `id`; otherwise it keeps its stage-1 random UUID
(`COALESCE(b.id, c.id)`). This is the entire "matching" logic for addresses
today — no fuzzy/approximate matching, no cross-dataset conflation beyond
exact-field equality.

**Output**: `{run}/data/theme=addresses/type=address/` — this becomes the
`data/` partition of the `theme_stage` bundle (`ThemeAssembleBundle`,
`ROOT_PATH = "theme_stage"`), which is what `theme_promote_dag` reads (§2).

---

## 4. Source Addresses Collect (ECS, legacy) — feeds Stage 1 directly

**File**: `airflow/dags/source_addresses_collect_dag.py`
**Task group**: `data_import`, one `EcsRunTaskOperator` per source
(lines 90-179)

This DAG registers one ECS Fargate task definition, then runs one
container invocation per source (in parallel), each invoking the
`address_ingestion` console script (`omf/omf/scripts/address_ingestion.py`,
registered in `omf/pyproject.toml`) with `--sources <name>`:

```python
EcsRunTaskOperator(
    task_id=source,
    ...
    overrides={"containerOverrides": [{
        "name": CONTAINER_NAME,
        "command": ["address_ingestion", "--storage-uri",
                    f"{{{{ params.output_path }}}}/run={RUN_ID}",
                    "--sources", source],
    }]},
    ...
)
```

Sources: `flanders`, `brussels`, `wallonia`, `br_ibge`, `czechia`,
`stadfangaskra`, `byggoastofnun_postcity`, `norway`, `si_gurs`,
`nyc_borough_boundaries`, `us_county_boundaries`, `oa`, `us_nad`.

**Job** (`omf/omf/scripts/address_ingestion.py:main`): each source object
(e.g. `us.UsNadSource()`, `oa.OpenAddressesSource.with_discovered_paths()`,
`be.flanders`) is handed to `SimpleRawSourceRepo.ingest_source(source)`, which
calls the source's own `ingest()` method to fetch bytes from the external
provider and write them, unmodified, to S3 under a per-source path. This is a
**black-box third-party network call per source** — no data-shape logic runs
here; it's pure byte-for-byte retrieval:

- OA: downloads `v2.openaddresses.io/batch-prod/collection-global.zip`
  (requester-pays S3 bucket) and extracts each per-country/county GeoJSON
  member to its own path.
- NAD: downloads a ~GB-scale ZIP from `data.transportation.gov`, shells out to
  `7z` to extract, and finds/copies the largest `NAD_r*.txt` file (picking the
  largest is itself a heuristic: "the full national extract dominates any
  supplementary file by orders of magnitude").
- Other countries (`be`, `br`, `cz`, `isl`, `nl`, `no`, `si`) each have their
  own `RawSource`/`HttpRawSource` definition (not read in depth here — same
  fetch-and-store pattern).

**Output**: `s3://<bucket>/<...>/collection/addresses/run=<ts>/` — this is
exactly the `input_path` that Stage 1 (`OvertureAddresses`,
`SimpleRawSourceRepo(storage_root=...)`) reads via
`self._raw_repo.path_for_source(source_name)`. No theme-schema transform
happens in this DAG at all — it's collection only.

---

## 5. The parallel "new" pipeline: collect → ingest (not yet wired to a release)

This branch is architecturally separate from §3–4 above. It is documented for
completeness because it's the direction the addresses theme is moving, and
because its `id`-null contract is the clearest evidence of the deferred-match
boundary mentioned in prior investigation.

### 5a. Dataset Addresses Collect

**File**: `airflow/dags/dataset_addresses_collect_dag.py`
**Job**: `overture_addresses/overture_addresses/collect/job.py`, `class CollectionJob`

Runs on a **serverless Python runtime — no Spark, no ECS**. Three strategies,
selected per resource by a resolver `@task` (`resolve_and_init_oa`,
`_resolve_and_init`, `_gurs_resolve_and_init`) that also does version
detection (OA batch job ID, HTTP `Last-Modified`, Socrata `rowsUpdatedAt`,
signed-API TTL, or run date) and bundle initialization
(`SourceRawBundle`/`Bundle.finalize`):

```python
def execute_job(self) -> None:
    strategy = self.get_param("strategy")
    if strategy == "s3_copy":
        self._s3_copy_all()          # OA: server-side S3→S3 copy, 16 parallel workers
    elif strategy == "http_download":
        self._http_download(url=self.get_param("url"))   # stream URL -> S3, no disk buffer
    elif strategy == "http_signed_api":
        ...                          # GURS: resolve short-lived signed URL, then stream
```

**What happens to the data**: nothing — this is byte-for-byte replication
into a bundle-shaped S3 layout
(`datasets/provider=<p>/resource=<r>/version=<v>/run=<run>/data/`). Any
archive extraction is deferred to ingest time (kept explicitly out of collect
"to keep collect I/O-only and disk-free"). Covers OpenAddresses (~165
resources via `s3_copy`), NAD (US, Socrata), NYC DCP + US Census/TIGER
boundary references (`http_static`), Byggðastofnun (IS), Stadfangaskrá (IS,
HEAD-probe versioned), PDOK/BAG (NL, HEAD-probe versioned), ČÚZK (CZ,
monthly-URL probe), GURS (SI, signed API).

The resolver tasks (`resolve_and_init_oa`, `_resolve_and_init`, etc., in
`dataset_addresses_collect_dag.py`) decide **whether** to (re)collect at all —
`SourceRawBundle.needs_processing`/`needs_processing_batch` skip resources
whose current upstream version was already collected (unless
`force_collect=True`). This is a scheduling/idempotency filter, not a data
filter.

### 5b. Dataset Addresses Ingest — normalize to Overture schema, id left null

**File**: `airflow/dags/dataset_addresses_ingest_dag.py`
**Job**: `overture_addresses/overture_addresses/ingest/job.py`, `class BatchIngestJob`
**Resolvers**: `airflow/dags/addresses/utils.py` (`resolve_oa_per_source_params`,
`resolve_non_oa_params`) — decide which raw bundles need (re)ingestion by
diffing S3 listings of raw bundles vs. already-ingested bundles.

`BatchIngestJob` loops over a resolved resource list in one Spark session and
dispatches each to a provider normalizer:

```python
_NORMALIZERS = {
    "open_addresses": lambda params: OANormalizer(...),
    "nad": lambda params: NadNormalizer(...),
    "stadfangaskra": lambda params: StadfangaskraNormalizer(...),
    "pdok": lambda params: PdokNormalizer(...),
    "gurs": lambda params: GursNormalizer(...),
    "cuzk": lambda params: CuzkNormalizer(...),
}
...
df = normalizer.normalize(self.spark, input_uri, temp_uri=temp_uri)
df.write.parquet(f"{output_uri}/type=address/", mode="overwrite")
```

Each normalizer subclasses `AddressNormalizer`
(`overture_addresses/overture_addresses/ingest/normalizer.py`), which
provides the same disallowed-token nulling, whitespace stripping, and
`address_levels` array construction as the legacy pipeline (ported values —
same disallowed lists, same "sources" struct shape, now extended with
`provider`/`resource`/`version` provenance fields the legacy pipeline left
null).

**`OANormalizer`** (`ingest/providers/oa.py`): reads
`source.geojson.gz`, filters null geometry/properties, nulls disallowed
tokens, wraps out-of-range longitudes (NZ Chatham Islands case), applies
`unit_excludes` editorial filters from JSON config (e.g. AU "carspace"),
and — for the two Mississippi statewide OA files only — `left_anti` clips
against TIGER counties so per-county OA takes precedence:

```python
selected = df.select(
    F.lit(None).cast("string").alias("id"),   # id is assigned by the match stage
    ...
).filter(
    F.col("geometry").isNotNull()
    & (F.col("street").isNotNull() | F.col("number").isNotNull())
)
for token in self.unit_excludes:
    selected = selected.filter(
        ~F.lower(F.coalesce(F.col("unit"), F.lit(""))).contains(token)
    )
if self.us_county_boundaries_uri:
    selected = self._apply_us_county_clip(spark, selected, temp_uri)
```

**`NadNormalizer`** (`ingest/providers/nad.py`): extracts `NAD_r*.txt` from
`source.zip`, maps columns, and reproduces the same NYC-boroughs +
TIGER-counties `left_anti` clip logic as the legacy `US` class — but the
filter lists now live in `airflow/dags/addresses/us_clip_regions.py`
(`COUNTY_NS_FILTERS`, `STATE_FP_FILTERS`, `CLIP_BY_COUNTY_OA_SOURCES`),
explicitly "ported from `omf/omf/addr/sources/us.py`" per that file's
docstring, with a unit test enforcing that every filter entry maps to a real
OA source.

```python
clipped = clipped.alias("nad").join(
    broadcast(nyc).alias("bnd"),
    ST_Contains(F.col("bnd.geometry"), F.col("nad._geom")),
    how="left_anti",
)
```

**Every normalizer's final schema cast** goes through `ADDRESS_SCHEMA`
(`overture_addresses/overture_addresses/ingest/schema.py`):

```python
ADDRESS_SCHEMA = StructType([
    # id is populated by the match stage (post-match UUID); null at ingest.
    StructField("id", StringType(), True),
    ...
])
```

**This is the id-assignment boundary.** Unlike the legacy pipeline (which
assigns a candidate UUID immediately in stage 1 and stabilizes it in stage 2,
all within the addresses theme's own DAGs), this pipeline ships `id = NULL`
all the way to its output and depends on a **"Planned" (not yet built)**
`theme_addresses_match_dag` to assign IDs. That DAG does not exist in this
repo — `airflow/dags/addresses/README.md`'s DAG table lists it as `Planned`
with description "Deduplication and GERS ID assignment." This confirms the
prior finding: **GERS/stable-ID assignment for this new pipeline is deferred
to a downstream match job that is not present/traceable in this codebase.**
Per the org's pipeline-architecture-tenets ("GERS assignment happens at
Match → Store → Merge, never Match → Merge → Store"), this is presumably
intended to route through the shared Match/Store/Merge infrastructure used by
other themes once built — but that wiring doesn't exist yet for addresses.

**Output**: `s3://<feeds bucket>/.../type=address/` per resource — written to
what the DAG doc calls "the feeds bucket for downstream matching." This
output is not currently read by `theme_addresses_stage_dag` or any other DAG
in this repo; it terminates at "feeds" pending the match DAG's implementation.

### Supporting files in `airflow/dags/addresses/`

- **`collect.py`**: one function, `fetch_all_oa_jobs`, calls the OA batch API
  (`GET {api_base}/data?layer=addresses`) to resolve every OA resource's
  current job ID in a single HTTP call (used by the collect DAG's OA
  resolver). No data transform.
- **`loaders.py`**: builds typed resource-config objects
  (`OAResourceConfig`, `HttpResourceConfig`, `HttpHeadProbeConfig`,
  `SocrataResourceConfig`, `SIResourceConfig`, `CZResourceConfig`) from the
  JSON files under `configs/datasets/`, dispatching by provider label via a
  registry (`_HTTP_LOADER_BY_PROVIDER`). Pure config plumbing, no records
  touched.
- **`resource_configs.py`** (not read in full — config dataclasses matching
  the loader table above; each encodes one provider's fetch strategy
  metadata, not row-level transforms).
- **`us_clip_regions.py`**: pure data — the county/state filter lists used by
  both `OANormalizer` and `NadNormalizer` for the US left_anti clips (see
  above). Explicitly ported from the legacy pipeline's `us.py` constants.
- **`utils.py`**: DAG-side resolvers (`resolve_oa_per_source_params`,
  `resolve_non_oa_params`, and one resolver function per non-OA provider) that
  decide *which* resources need (re)ingestion by diffing S3 listings —
  scheduling/idempotency logic, not data transforms — plus
  `finalize_batch_bundles`, which writes bundle metadata/success markers.

---

## Notes on provenance-relevant hazards found during this trace

- **Raw SQL blocks**: `AddressIDAssignment` (§3b) runs two full `spark.sql(...)`
  string queries for dedup and baseline matching — this is the one place in
  the whole addresses pipeline where record identity is created, merged, or
  discarded via SQL rather than DataFrame API calls, and provenance tracking
  needs to understand this join to explain why two input rows became one
  output row.
- **UUID generation as identity**: `overture_id()` (`expr("uuid()")`) is a
  non-deterministic identity source. Two runs of stage 1 alone (without stage
  2's baseline join) would give the same input data two different ID sets —
  provenance tracking must treat stage-1 IDs as ephemeral/candidate, not
  final.
- **No violation-store writes were found anywhere in this trace.** Neither
  the legacy nor the new pipeline writes to a violation store for addresses;
  data that fails a filter is silently dropped (e.g. null-geometry rows,
  disallowed-token rows, unit-exclude rows, US NAD/OA overlap clips).
- **Black-box third-party calls**: `source_addresses_collect_dag` (§4) and
  `dataset_addresses_collect_dag` (§5a) both call external, non-Overture
  services directly (OpenAddresses' S3 bucket, US DOT's NAD download, GURS's
  signed-URL API, PDOK/ČÚZK/Stadfangaskrá HTTP endpoints, Census TIGER, NYC
  DCP). None of these upstream services' internal processing is visible to
  this repo; from a provenance standpoint they are opaque sources whose
  output is trusted as-is (aside from the OA-vs-NAD precedence rule).
- **Exact-match-only conflation**: the only "matching" logic in this entire
  theme (legacy stage 2, §3b) is exact-field-equality after H3 binning. There
  is no fuzzy/probabilistic address matching anywhere in this codebase today.
- **Config-as-code for editorial decisions**: `unit_excludes` (JSON config,
  new pipeline) is explicitly a design choice to keep per-source editorial
  filtering auditable outside of code — worth preserving as a distinct
  provenance category (editorial exclusion vs. structural filter).

---

## Summary: linear stage list, raw source → release

1. **Raw source fetch** (`source_addresses_collect_dag.py`, ECS Fargate,
   `address_ingestion` CLI) — downloads OA, NAD, and per-country files
   byte-for-byte from external providers into
   `collection/addresses/run=<ts>/`.
2. **Stage 1 normalize + candidate ID** (`theme_addresses_stage_dag.py` →
   `omf.addr.scripts.glue.OvertureAddresses`) — maps every source's raw
   records into the Overture address schema via `Planet`'s ~38 per-country
   `AddressDataset`s (including the US NAD/OA `left_anti` precedence clips),
   assigns each record a random UUID `id`.
3. **Stage 2 dedup + baseline ID stabilization**
   (`theme_addresses_stage_dag.py` →
   `omf.addr.scripts.glue.AddressIDAssignment`) — collapses exact-duplicate
   rows (raw SQL `GROUP BY` + `MIN(id)`), then H3-joins against the previous
   release's addresses to inherit stable IDs for unchanged records
   (`COALESCE(baseline.id, candidate.id)`). Output is the `theme_stage`
   bundle's `data/theme=addresses/type=address/`.
4. **Theme Promote** (`theme_promote_dag.py`, shared `overture_cdp` jobs) —
   validates schema (`ValidateDataJob`), computes changelog/churn
   (`ComputePublicChangelogJob`), copies/packages data
   (`ProcessDataJob`), generates PMTiles and bridge files. Output is the
   `theme_promote` bundle.
5. **Release Publish** (`release_publish_dag.py`) — DataSyncs the promoted
   bundle to the managed release/archive/Azure buckets, publishes STAC,
   refreshes Glue crawlers, tags the bundle as released.

Parallel, not-yet-connected track (does not currently feed a release):

A. **Serverless collect** (`dataset_addresses_collect_dag.py` →
   `overture_addresses.collect.CollectionJob`) — byte-for-byte fetch into
   bundle-shaped `datasets/provider=.../resource=.../version=.../run=.../data/`.
B. **Batch ingest, id left null** (`dataset_addresses_ingest_dag.py` →
   `overture_addresses.ingest.job.BatchIngestJob`, per-provider normalizers)
   — same schema-mapping/filtering pattern as stage 1 above, writes to a
   "feeds" bucket "for downstream matching," with `id = NULL` by contract.
C. **Match (Planned, not built)** — `theme_addresses_match_dag`, described in
   `addresses/README.md` as "Deduplication and GERS ID assignment." Does not
   exist in this repository; this is the confirmed deferred-match boundary.
