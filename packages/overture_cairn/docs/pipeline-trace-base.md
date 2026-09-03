# Base Theme Pipeline Trace

Research document for `overture-cairn` (provenance tracking). Traces the Overture
**base** theme (`infrastructure`, `land_use`, `land`, `water`, `land_cover`,
`bathymetry`) end to end, starting at release publish and working backward to
the earliest raw source reads, per stage: what comes in, what changes, what
leaves, and whether records are dropped, merged, split, or given a new
identity.

A note on scope: `overture_base` was used as an early prototyping target for
`overture-cairn`. `overture_base/overture_base/base_land.py` and
`base_common.py` import from a file called `_cairn_stub.py`
(`overture_base/overture_base/_cairn_stub.py`). That file is throwaway scratch
work — its `track`/`drop`/`keep_best`/`rebind` wrappers exist only so
`base_land.py` could be rewritten as if a real `overture-cairn` API already
existed, to see how the calls would read. It is not part of the design and its
instrumentation (the `print()` calls) is not real provenance capture. However,
the underlying Spark logic each wrapper runs (a `.filter()`, a
window-function "keep one row per group", a same-dataframe id comparison) *is*
the real, currently-shipping transform — `dedup()` in `base_common.py` calls
`keep_best()` for real deduplication of `base_land.py`/`base_water.py` output.
Where this document describes what `drop`/`keep_best`/`rebind` do, it is
describing that real underlying behavior, not the stub's printout.

---

## 1. Release Publish (shared across all themes)

**File**: `airflow/dags/release_publish_dag.py`

This is the last stage for every theme, not base-specific. It takes a
already-assembled release-candidate bundle (produced by `theme_promote_dag`
for every theme, then presumably combined into `staging/release_candidate/...`
by a separate assembly step outside this trace) and moves it into public
distribution. Concretely it:

- DataSyncs `data/`, `changelog/`, `bridgefiles/`, and `registry/` from the
  scratch bucket to the managed AWS release bucket, an Azure blob container,
  and an archive bucket (three destinations, same bytes, no transform).
- Copies PMTiles via boto3 multipart copy instead of DataSync (files are
  25-150GB; DataSync is too slow for them).
- Runs `PublishStac` (`overture_core.stac.job`) in single-release mode on
  Fargate, then invalidates the STAC CloudFront distribution.
- Starts four Glue crawlers in the Distribution AWS account to refresh the
  Athena/Glue catalog over the newly published release.
- Writes a `released` marker and `released_at` timestamp into the RC bundle's
  `metadata.json` (`ReleaseCandidateBundle.tag_as_released_from_uri`, line
  378-380), which is how `resolve_latest_released()` finds "the latest
  release" elsewhere in the platform.

No feature-level data transform happens here — this stage is byte-for-byte
replication plus catalog/metadata bookkeeping. It is documented in more depth
elsewhere; included here only so the trace has an unbroken chain to the public
release.

---

## 2. Theme Promote (shared across all themes)

**File**: `airflow/dags/theme_promote_dag.py`, task group builder
`airflow/dags/src/public/overture_airflow/theme_promote.py`

Takes a `theme_stage`/`theme_assemble` bundle (for base: the output of
`theme_base_stage_dag`, see §3) and turns it into the versioned, changelogged,
schema-validated `theme_promote` bundle that release candidates are built
from. This is shared plumbing across `addresses`, `base`, `buildings`,
`divisions`, `places`, `transportation` — but it does perform real per-record
data changes, so it's worth tracing here rather than skipping.

Task order (`theme_promote.py:336-363`): `validate_data` (schema check against
input) → `compute_internal_changelog` → `validate_churn` (fails the run if
churn stats look wrong) → `process_data` → `compute_public_changelog` →
`generate_metrics`, with `pmtiles_task` and bridge-file generation branching
off `process_data`, and a final `validate_final` + hidden-file cleanup gate.

### 2a. `compute_internal_changelog` — version assignment

**File**: `overture_cdp/overture_cdp/compute_internal_changelog.py`,
`ComputeInternalChangelogJob.get_changelog_df` (lines 83-176)

This is where a feature's public `version` number actually gets decided. It
full-outer-joins the new theme data against the previous release's data on
`(id, type)`, hashes every non-excluded column (geometry included, via
`ST_AsText` then `sha2`) on both sides, and compares the hashes:

```python
changelog_df: DataFrame = old_df.alias("old").join(
    new_df.alias("new"),
    (col("old.id") == col("new.id")) & (col("old.type") == col("new.type")),
    "full_outer",
)
...
when(col("old.id").isNull(), lit(ChangeType.ADDED.value))
.when(col("new.id").isNull(), lit(ChangeType.REMOVED.value))
.otherwise(
    when(size(raw_changes_array) > 0, lit(ChangeType.DATA_CHANGED.value))
    .otherwise(lit(ChangeType.UNCHANGED.value))
)
.alias("change_type"),
...
when(
    (col("new.id").isNotNull()) & (col("old.id").isNotNull())
    & (size(raw_changes_array) > 0),
    col("old.version") + 1,
)
.otherwise(coalesce(col("old.version"), lit(1)))
.alias("version"),
```

In plain English: a feature present in both old and new data with identical
hashed columns keeps its old version number unchanged. A feature whose
columns hashed differently gets `version + 1`. A brand new `id` gets version
1. Nothing is dropped by this step itself — it produces a side-table
(`changelog_df`, keyed by `id`) that `process_data` (§2b) later joins back
onto the main dataframe to attach `version` and `bbox`. This is the point in
the whole base pipeline where "version" — as GERS understands it — first
comes into existence; everything upstream of this (all of §3-§11 below) emits
`version = 0` as a placeholder.

### 2b. `process_data` — block-list filter, bbox filter, spatial repartition

**File**: `overture_cdp/overture_cdp/process_data.py`, `ProcessDataJob` (lines
842-995)

```python
def _filter_block_list(self, df: DataFrame) -> DataFrame:
    """Remove blocked IDs from the dataset before writing public output."""
    if not BLOCKED_IDS:
        return df
    filtered = df.filter(~F.col("id").isin(BLOCKED_IDS))
    ...

df = df.join(changelog_df.select("id", "version", "bbox"), on="id", how="left")
df = self._filter_by_bbox(df, bbox_str)
df = self._filter_block_list(df)
```

Three real changes to the record set happen here: (1) `version`/`bbox` are
joined on from the changelog computed in §2a — this is the definitive
assignment of public version numbers; (2) if a `bbox` param was supplied
(used for dev/test runs, see `DEFAULT_TEST_BBOX`), rows whose bbox doesn't
intersect it are dropped outright; (3) a hardcoded block list
(`overture_cdp.block_list.BLOCKED_IDS`) removes specific GERS ids from public
output unconditionally — a manual takedown mechanism, not something derived
from the data itself. After filtering, the job spatially repartitions
(KDB-tree over bbox centroids) and rewrites GeoParquet with
Hilbert-curve-ordered row groups (`write_spatial_parquet`) — this changes
file layout, not feature content.

`validate_data` (`overture_cdp/overture_cdp/validate_data.py`) runs
`overture.schema.pyspark.validate` against the input data and reports/fails on
violations, but does not itself drop or alter rows — it's a gate, not a
transform.

---

## 3. Theme Base Stage (base-specific orchestrator)

**File**: `airflow/dags/theme_base_stage_dag.py`

This is where base-specific processing begins. It resolves four independent
input bundles and fans out to six parallel Spark jobs, one per base
sub-type, each reading a different combination of those inputs and writing
directly into `type=<subtype>` partitions of one shared output bundle:

```python
overture_rc_bundle = SourceIngestBundle(provider="osm", resource="overture_rc", version="{{ params.overture_rc_ds }}")
coastline_bundle   = SourceRawBundle(provider="osm", resource="coastlines", version="{{ params.coastline_ds }}", ...)
land_cover_bundle  = SourceRawBundle(provider="esa", resource="esa_worldcover", version="{{ params.land_cover_ds }}")
bathymetry_bundle  = SourceRawBundle(provider="ncei", resource="etopo_globathy", version="{{ params.bathymetry_ds }}")
```

| sub-theme | job class | reads |
|---|---|---|
| infrastructure | `overture_base.base_infrastructure.BaseInfrastructure` | `overture_rc_bundle` |
| land_use | `overture_base.base_land_use.BaseLandUse` | `overture_rc_bundle` |
| land | `overture_base.base_land.BaseLand` | `overture_rc_bundle` + `coastline_bundle` |
| water | `overture_base.base_water.BaseWater` | `overture_rc_bundle` + `coastline_bundle` |
| land_cover | `overture_base.base_land_cover.BaseLandCover` | `land_cover_bundle` (ESA) |
| bathymetry | `overture_base.base_bathymetry.BaseBathymetry` | `bathymetry_bundle` (NCEI) |

The DAG's own comment on land_cover/bathymetry (lines 218-220) states the
intent plainly: *"Land cover is copied through as-is. Bathymetry is
inverted: raw ETOPO depth=d polygons cover everything shallower than d
(including land); the job converts them to grid-chipped polygons covering
everything deeper than d (schema#311)."*

All six jobs run in parallel after `setup_parameters` and converge on
`finalize_bundle`, which stamps the output `ThemeAssembleBundle` (the input
that `theme_promote_dag`, §2, later consumes). Four independent raw-source
chains feed this fan-out — OSM (§5-§8), OSM coastlines (§9), ESA WorldCover
(§10), and NCEI ETOPO/GLOBathy (§11) — each traced separately below.

---

## 4. Base sub-theme jobs — identity, filtering, dedup

**Package**: `overture_base/overture_base/base_land.py`, `base_water.py`,
`base_land_use.py`, `base_infrastructure.py`, `base_land_cover.py`,
`base_bathymetry.py`, shared helpers in `base_common.py`

All four OSM-derived jobs (`land`, `water`, `land_use`, `infrastructure`)
share the same shape: read `overture_rc_bundle` at
`theme=base/type=<subtype>`, drop rows with null/empty/invalid geometry,
promote OSM "common" names to "primary" when no primary name exists, re-mint
`id` for any row that doesn't already carry a schema-valid UUIDv3, select
down to the final column set, and dedup on `id`.

### `promote_common_to_primary_name` (`base_common.py:27-47`)

```python
def promote_common_to_primary_name():
    return (
        F.when(F.col("names.primary").isNotNull(), F.col("names"))
        .when(F.col("names.common").isNotNull(), F.struct(
            F.map_values(F.col("names.common"))[0].alias("primary"),
            F.col("names.common"), F.col("names.rules"),
        ))
        .when(F.col("names.rules").isNotNull(), F.struct(
            F.col("names.rules")[0]["value"].alias("primary"),
            F.col("names.common"), F.col("names.rules"),
        ))
        .otherwise(F.lit(None))
    )
```

If a feature has no primary name but does have a "common"-variant name (or a
name from a naming rule), that becomes the primary name. This changes what a
downstream consumer sees as *the* name of a feature — it's a real content
decision, not just a null-fill.

### Identity re-minting — `base_land.py:109-132` (representative of all four)

```python
df = df.withColumn(
    "_new_id",
    F.when(
        F.col("id").rlike(
            "^[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        ),
        F.col("id"),
    ).otherwise(
        generate_uuid3(
            F.lit("LAND"),
            F.expr("subtype || class || SPLIT(sources[0].record_id,'@')[0]"),
        ),
    ),
)
df = rebind(df, old="id", new="_new_id", reason="minted a content-derived id because the record didn't already carry a valid one")
df = df.drop("id").withColumnRenamed("_new_id", "id")
```

Every row already got a UUIDv3 `id` upstream in the OSM-to-Overture converter
(§6), keyed on `subtype + class + OSM record id`. This step is a safety net:
if the incoming `id` isn't a syntactically valid v3 UUID (regex-checked, not
namespace-checked), it's re-derived from the same formula. In steady state
this is a no-op; it only fires for malformed/legacy input.

### Dedup — `base_common.py:97-103`

```python
def dedup(df):
    return keep_best(
        df, per="id",
        by=F.col("sources")[0].getItem("update_time").desc(),
        reason="superseded by a newer duplicate for the same id",
    )
```

`keep_best` (in the `_cairn_stub.py` prototype, but running real Spark logic)
partitions by `id`, orders by `sources[0].update_time` descending, and keeps
only the top row per partition — i.e. if two rows end up with the same
minted `id` (e.g. two different OSM features that happen to hash to the same
`subtype+class+record_id`, or an OSM-derived and coastline-derived row
colliding), the one with the freshest source timestamp wins and the other is
dropped. Every one of the six base jobs calls this before writing its final
output.

### `base_land.py` — coastline union

`land_from_coastline()` (lines 20-59) turns each OSM-coastline "land" polygon
into a base `land`/`land` feature with a freshly-minted id
(`generate_uuid3("LAND", "'landland' || ST_ASTEXT(geometry))")`) and a
synthetic `sources` entry (`coastline_sources()`, `base_common.py:67-94`)
pointing at `provider=osm, resource=coastlines`. `combine_osm_and_coastline_land`
(lines 62-69) is a plain `.union()` — the comment is explicit that this step
by itself changes no identities; the real merge (two rows landing on the same
`id`) is deferred to `dedup()` above.

### `base_water.py` — same coastline pattern, plus a geometry rewrite

Same coastline-union pattern as land, minting `oceanocean`-keyed ids for
coastline-derived ocean polygons. One extra transform not present in the
other jobs, in the OSM branch's geometry column (`water_from_osm.py:86-91`,
described in §6): canal/drain/ditch features that came through as `Polygon`
have their geometry rewritten to just the exterior ring — i.e. a closed area
feature is turned into a line, because a linear waterway shouldn't be
represented as a filled polygon.

### `base_land_cover.py` — pass-through with provenance stamping

```python
df = (
    df.select(["id", "version", "subtype", "cartography", "geometry"])
    .filter(F.col("geometry").isNotNull())
    .filter(F.expr("ST_ISEmpty(ST_GeomFromWKB(geometry)) = false"))
    .filter(F.expr("ST_ISVALID(ST_GeomFromWKB(geometry)) = true"))
    .withColumn("sources", F.array(F.struct(
        F.lit("ESA WorldCover").alias("dataset"),
        F.lit("CC-BY-4.0").alias("license"),
        F.lit(update_time).alias("update_time"),
        F.lit(provider).alias("provider"), F.lit(resource).alias("resource"),
        F.lit(land_cover_version).cast("string").alias("version"),
        ...
    ))),
)
```

No id is re-minted and no rules classify anything — the comment in the DAG
(§3) says it plainly: this is a copy-through. The only additions are the
three geometry sanity filters (shared with every other base job) and a
`sources` struct that stamps ESA WorldCover's dataset name, license, and pull
version onto every row, so downstream consumers can tell this data came from
ESA and not OSM.

### `base_bathymetry.py` — the one geometric inversion in base

Already summarized structurally in §3; the mechanics (lines 63-140): the
raw ETOPO/GLOBathy input is a set of *stacked coverage* polygons per depth
level `d`, where the `d` polygon covers everything shallower than `d`
(land included). The job subdivides each depth polygon into small pieces for
efficient spatial joins (`ST_SubDivide`), builds a 1°x1° world grid, computes
per-cell-per-depth "covered" geometry via `ST_Intersection` +
`ST_Union_Aggr`, then inverts:

```python
CASE
    WHEN c.covered IS NULL THEN g.cell
    ELSE ST_CollectionExtract(ST_Difference(g.cell, c.covered), 3)
END AS geom
```

A grid cell with no recorded coverage at depth `d` is assumed entirely deeper
than `d` (kept whole); a partially-covered cell keeps only the uncovered
remainder; a fully-covered cell becomes empty and is dropped by the
subsequent `NOT ST_IsEmpty` filter. This is a genuine geometric
transformation of what each record *means* — "shallower than d" input
becomes "deeper than d" output — done for schema conformance
(OvertureMaps/schema#311), not a formatting change. IDs are freshly minted
per output polygon (`generate_uuid3("BATHYMETRY", "'bathymetry' || depth || WKT")`)
since the output polygons no longer correspond 1:1 to input polygons (they've
been diced onto a grid and clipped).

---

## 5. OSM Adjudicator — `overture_rc` bundle, violation-store merge

**Files**: `airflow/dags/osm/dataset_osm_adjudicator_dag.py`,
`omf/omf/adjudicator/osm_adjudicator.py`

This is the stage that produces the `overture_rc_bundle` consumed in §3-§4.
**It reads and writes managed Iceberg violation-store tables** —
`entity_violations_table` and `entity_fast_forward_table`
(`src/iceberg.py: get_entity_violation_table`, `get_entity_fast_forward_table`)
— and runs raw Spark SQL against them
(`osm_adjudicator.py:69-189`). Flagging this explicitly per the trace
brief: this is exactly the kind of violation-store read/write this
provenance work needs to model.

On a **sprint reset day**, this stage is a pure format copy
(`ResetCopyFormats`, `omf/omf/adjudicator/reset_copy_formats.py`): read
`osm_in_overture` parquet for a theme/type, drop two internal debug columns
(`ext_osm_id`, `ext_debug`), write both a plain-Parquet and a GeoParquet copy.
No adjudication logic runs.

On a **regular day**, `OSMAdjudicator.execute_job` (`osm_adjudicator.py:30-198`)
runs one big multi-CTE SQL query joining three sources:

- `entity_fast_forward` — Iceberg rows where a human reviewer marked an OSM
  entity `fixed`/`already_fixed` between the last sprint reset and today.
- `entity_violations` — Iceberg rows for OSM entities with an active
  violation whose name is in a hardcoded `CRITICAL_VIOLATION_NAMES` list
  (`dataset_osm_adjudicator_dag.py:52-68` — e.g. `building_spiky`,
  `suspicious_name_changes`, `important_feature_deletion`).
- `osm_geometry` — the raw OSM geometry table as it stood at the last sprint
  reset (not today).

The query identifies every OSM entity that either (a) has a critical
violation today that it didn't have at the reset date, or (b) was
fast-forwarded (reviewer-approved) since the reset, and emits one row per
`id` recording `version_at_reset` and `violations_at_reset`. This result
(`adjudicator_output`) is not itself the final data — it's a routing table
consumed by the per-theme/type merge step:

```python
# OSMAdjudicatorMerge.execute_job, osm_adjudicator.py:219-288
df_reset = spark.sql(f"""
    SELECT {cols_sql} FROM osm_reset
    WHERE split(sources[0].record_id, '@')[0] NOT IN (SELECT id FROM adjudicator_ops)
""")
df_today = spark.sql(f"""
    SELECT {cols_sql} FROM osm_today
    WHERE split(sources[0].record_id, '@')[0] IN (SELECT id FROM adjudicator_ops)
""")
merged_df = df_reset.unionByName(df_today, allowMissingColumns=True)
```

In plain English: for every base/buildings/transportation type
(`OVERTURE_OSM_THEMES_AND_TYPES`, line 70-79), an entity flagged by the
adjudicator query gets **today's** (fixed) version of the feature; every
other entity gets the **sprint-reset** (pre-vetted) version, discarding
today's version of that entity entirely. This is a genuine per-record
provenance fork: two different OSM snapshot dates are interleaved into one
output dataset depending on a violation-store lookup, and the losing side of
each `id` is dropped. Output is written as both Parquet (`legacy_data/`,
for `theme_base_stage_dag`) and GeoParquet (`data/`).

---

## 6. OSM-to-Overture conversion (`osm_in_overture` bundle)

**Files**: `airflow/dags/osm/dataset_osm_ingest_dag.py` (task group
`convert_osm_to_overture` → `base`), job classes in
`overture_base/overture_base/{land,water,land_use,infrastructure}_from_osm.py`,
shared SQL helpers in `overture_osm/overture_osm/osm_common.py`, declarative
tag rules in `overture_base/overture_base/tag_classification.py` +
`{land,water,land_use,infrastructure}_rules.py`

This is the stage that first turns raw OSM node/way/relation records into
Overture-shaped `base` records, reading from `geometry_daily` (§7). Each of
the four sub-type jobs follows the same shape: SQL `WHERE` pre-filter on
relevant OSM tags → SQL `SELECT` computing names/sources/surface/height/etc.
via shared helpers → a declarative rules table assigns `subtype`/`class` →
drop anything the rules didn't classify → mint an id → select final columns.

### Pre-filter (tag existence gate) — `land_from_osm.py:34-46`

```python
where_filter = f"""
(
    element_at(tags, 'natural') IS NOT NULL
    OR element_at(tags, 'surface') IS NOT NULL
    OR element_at(tags, 'landcover') IS NOT NULL
    OR element_at(tags, 'landuse') IN ('forest')
    OR element_at(tags, 'place') IN ('archipelago','island','islet')
    OR element_at(tags, 'geological') IN ('meteor_crater','volcanic_caldera_rim')
)
AND element_at(tags, 'highway') IS NULL
AND element_at(tags, 'building') IS NULL
AND element_at(tags, 'golf') IS NULL
AND element_at(tags, 'leisure') IS NULL"""
```

Every sub-type job (`water_from_osm.py:32-48`, `land_use_from_osm.py:33-38`,
`infrastructure_from_osm.py:36-39`) has its own tag-existence gate like this.
This is the first point where the full OSM planet dataset gets split into
theme-relevant subsets — an OSM way tagged `building=house` never reaches the
land/water/land_use/infrastructure converters at all (it goes to the
buildings converter instead, out of scope for this trace).

### Classification rules — declarative, first-match-wins

**File**: `overture_base/overture_base/tag_classification.py`,
`rules_to_column()` (lines 235-262)

```python
def rules_to_column(rules: Sequence[Rule]) -> Column:
    col = None
    for rule in rules:
        cond, res = ...  # each rule's condition() and result()
        col = F.when(cond, res) if col is None else col.when(cond, res)
    return col.otherwise(F.lit(None).cast("struct<subtype:string,class:string>"))
```

Rule tables (e.g. `land_rules.py`, `water_rules.py`) are lists of
`TagRule`/`NestedTagRule`/`CompoundRule` dataclasses, each mapping an OSM tag
condition (and optionally a geometry-type constraint) to an Overture
`(subtype, class)` pair. Example, `water_rules.py:13-25`:

```python
WATER_RULES = [
    TagRule(tag="waterway", equals="stream", subtype="stream", class_from_tag="waterway"),
    TagRule(tag="water", equals="stream", subtype="stream", class_from_tag="water"),
    TagRule(tag="waterway", equals="river", subtype="river", class_from_tag="waterway"),
    ...
]
```

The first matching rule wins; a feature that matches none of them gets a
`NULL` `overture` struct and is dropped by the very next line in every job:

```python
result_df = (
    base_df.withColumn("overture", overture_col)
    .filter(F.col("overture.subtype").isNotNull())
    ...
```

So the tag pre-filter (loose, existence-based) admits a superset of
candidates, and the rules table is the actual fine-grained classifier that
decides both *whether* a feature becomes an Overture `base` feature at all,
and *which* `subtype`/`class` it gets.

### Identity assignment — `overture_osm/osm_common.py:72-136`

```python
def uuid_v3_sql(namespace: str, value_expr: str) -> str:
    ns_uuid = OvertureNameSpace[namespace].value
    ...  # md5(namespace_bytes || value_expr), formatted as a v3 UUID

def osm_to_overture_sources_sql(osm_id="id", osm_type="type", osm_version="version",
                                 updated_at="updated_at", *, pull_date: str) -> str:
    ...
    return f"""
    array(named_struct(
        'dataset', 'OpenStreetMap', 'license', 'ODbL-1.0',
        'record_id', concat(substring({osm_type},1,1), cast({osm_id} as string), '@', cast({osm_version} as string)),
        'update_time', {update_time},
        'provider', 'osm', 'resource', 'planet',
        'version', greatest({pull_ts}, {update_time})
    ))"""
```

Every base feature's `id` is a UUIDv3 of `namespace(LAND|WATER|LAND_USE|INFRASTRUCTURE)`
+ `subtype || class || <OSM type-letter><OSM id>` (each job's
`selectExpr`, e.g. `land_from_osm.py:71-76`). This means the id is fully
**content-derived**: it depends on the OSM feature's id *and* which
subtype/class the rules table assigned it. If a later re-run of the rules
table reclassifies a feature (a tag rule changes), that feature gets a
**different id** — there is no persistent, rules-independent identity
carried from OSM into Overture. `sources[0].record_id` (`<type-letter><osm
id>@<osm version>`) is the traceable link back to the specific OSM
node/way/relation version this feature was derived from.

### `water_from_osm.py` — polygon-to-line geometry rewrite

```python
"""CASE
    WHEN ST_GeometryType(ST_GeomFromWKB(geometry)) = 'ST_Polygon'
         AND element_at(tags, 'waterway') IN ('canal', 'drain', 'ditch')
        THEN ST_AsEWKB(ST_ExteriorRing(ST_GeomFromWKB(geometry)))
    ELSE geometry
END AS geometry"""
```

An OSM canal/drain/ditch mapped as a closed polygon area has its geometry
replaced by just the exterior ring (turning it into a line), because
Overture's water schema wants linear waterways represented as lines even if
OSM mapped the channel as an area.

---

## 7. OSM daily geometry construction (`geometry_daily`)

**Files**: `airflow/dags/osm/dataset_osm_geometry_dag.py`,
`omf/omf/osm/osm_geometry_osc.py`, `omf/omf/osm/osm_osc_collect.py`, real
logic in `omf/omf/utilities/osm_geometry.py`

This is one level further back: before OSM tags can be classified into base
features, OSM's raw node/way/relation primitives have to be resolved into
actual geometries (nodes → points, ways → linestrings/polygons, relations →
multipolygons), and kept up to date daily. `geometry_daily` is *not* filtered
or classified for Overture at all — it's a general-purpose OSM geometry
table that base, buildings, and transportation ingest (§6) all read from
independently.

### Raw ingest: OSM's own replication feed (external, black-box HTTP)

**File**: `omf/omf/utilities/osm_geometry.py`, class `OSCData` (lines
909-1069), function `download_daily_osc_data` (lines 1073-1106)

```python
base_url = "https://planet.openstreetmap.org/replication"
state_url = urljoin(base_url, frequency, "state.txt")
...
response = requests.get(state_url, timeout=10)
...
osc_url = self._build_osc_url(base_url, frequency, sequence_number)
response = requests.get(osc_url, stream=True)
xml = GzipFile(fileobj=response.raw)
self._iterparse_xml(xml)
```

This is the earliest raw ingest for the daily OSM update path: a plain HTTP
`GET` against `planet.openstreetmap.org`'s public replication endpoint,
outside any AWS/Overture infrastructure — a genuine external black-box
call with no schema contract beyond OSM's own OSC XML format. The XML is
parsed into `create`/`modify`/`delete` buckets per OSM type
(`_iterparse_xml`, lines 992-1059) and written to parquet
(`OSMOscCollect` job, `omf/omf/osm/osm_osc_collect.py`).

### Applying the diff on top of yesterday's geometry

**File**: `omf/omf/utilities/osm_geometry.py`,
`build_osm_geometry_with_cache` (lines 670-891)

For each OSM type (node, way, relation), the day's OSC changes are
deduplicated to the latest version per id (`osc_dedup`, lines 18-37, a
`GROUP BY id, type` with `max_by(..., version)` on every field), split into
create/modify vs. delete, and merged onto yesterday's `geometry_daily` table
via `apply_osc_with_geometry` (lines 545-572):

```python
SELECT a.id, ..., a.geom
FROM (
    SELECT IF(b.id IS NULL, a.id, b.id) AS id, ..., IF(b.id IS NULL, a.geom, b.geom) AS geom
    FROM {base_data} a
    FULL OUTER JOIN {updated_data} b ON a.id = b.id
) a
LEFT OUTER JOIN {deleted_data} b ON a.id = b.id
WHERE b.timestamp IS NULL
```

A full outer join lets a new/modified row from today's OSC override
yesterday's row for the same id; a subsequent left-anti-join against the
day's delete list drops any id OSM reports as deleted. Geometry construction
itself cascades node → way → relation: `build_node_geometry` turns
lat/lon into a `ST_Point`; `build_linestring_for_ways` collects each way's
member node points in position order into a linestring
(`ST_LineFromMultiPoint`); `build_ways_geometry_from_linestring`
(lines 225-276) turns a closed linestring into a polygon *only* if the way
carries one of a specific allow-list of area-implying tags (`building`,
`landuse`, `natural`, `amenity`, etc. — lines 242-266) — otherwise it stays a
line even if closed. Relations follow a heavier multi-step process
(`merge_ways_into_multipolygon`, `_merge_lines`, `aggregate_polygons_for_relation`)
that reconstructs multipolygons from member way rings via `ST_SymDifference`
aggregation, including custom cycle-detection Python code
(`_get_all_cycles`, lines 316-344) for self-intersecting merged rings.
Invalid way geometries are repaired, not dropped
(`fix_invalid_geometries`, lines 285-312): `ST_MakeValid` followed by
`ST_ForcePolygonCCW`. Relations exceeding `MAXIMUM_RELATION_GEOMETRY_SIZE`
(30MB of WKB) are simplified via `ST_SimplifyPreserveTopology` rather than
rejected (`save_geo_parquet_osm`, lines 659-666) — this is a real,
lossy geometric approximation applied silently to a small number of
enormous relations (some country/coastline boundaries).

---

## 8. OSM planet bootstrap (`geometry_planet`) — earliest raw OSM source

**Files**: `airflow/dags/osm/dataset_osm_geometry_reset_dag.py`,
`omf/omf/osm/osm_geometry_planet.py`

The daily OSC chain (§7) always applies changes on top of a `base_table_path`
— which for the very first run, or a periodic full reset, is not yesterday's
`geometry_daily` but a **planet snapshot**. This is the true earliest raw
read in the entire base pipeline:

```python
# dataset_osm_geometry_reset_dag.py:52-73
_osm_dataset = Dataset.from_name("osm", "planet")
_planet_download_url = _osm_dataset.collection["data_download"][0]
...
PBF_KEY = _planet_prefix + "/.../planet-YYMMDD.osm.pbf"

wait_for_planet_pbf = S3KeySensor(
    task_id="wait_for_planet_pbf",
    bucket_key=PBF_KEY, bucket_name=_planet_bucket, ...
)

convert_planet_pbf_to_parquet = EcsRunTaskOperator(
    task_id="convert_planet_pbf_to_parquet",
    cluster=ECS_CLUSTER_NAME, task_definition=ECS_TASK_DEFINITION,
    overrides={"containerOverrides": [{
        "environment": [
            {"name": "INPUT_S3_PATH", "value": f"s3://{_planet_bucket}/{PBF_KEY}"},
            {"name": "OUTPUT_S3_PATH", "value": planet_bundle.sub_directory("raw")},
        ],
    }]},
)
```

The DAG waits (`S3KeySensor`) for a weekly `planet-YYMMDD.osm.pbf` file to
land in an external S3 bucket/prefix defined by dataset config
(`Dataset.from_name("osm", "planet")` — the PBF file itself is produced by
some process outside this repo, presumably a periodic geofabrik/OSM mirror
sync). Converting the binary `.osm.pbf` into parquet is offloaded to an ECS
Fargate task (`src/pbf_to_parquet.py` supplies the cluster/task-definition
constants) — this is a **black-box third-party/containerized conversion**:
its internal logic (almost certainly an `osmium`-based PBF reader) is not
Python/Spark code in this repo and wasn't traced further. Once parquet lands,
`OSMGeometryPlanet` (`omf/omf/osm/osm_geometry_planet.py`) runs the exact
same node → way → relation geometry-construction functions from
`osm_geometry.py` described in §7 (`build_node_geometry`,
`build_linestring_for_ways`, `build_ways_geometry_from_linestring`,
`merge_ways_into_multipolygon`, `aggregate_polygons_for_relation`), just
against the full planet dump with no prior "base" table to diff against —
i.e. the same geometry-construction logic serves both bootstrap and daily
incremental paths.

The reset DAG also re-plays every day's OSC changes from the planet date
through yesterday (`OSMGeometryOSC`, §7) to catch the planet snapshot up to
"today" before handing control back to the daily DAG.

---

## 9. OSM Coastlines — `coastline_bundle` (feeds `base_land`, `base_water`)

**Files**: `airflow/dags/dataset_osm_coastline_collect_dag.py`,
`overture_coastlines/overture_coastlines/{shapefile_to_parquet.py,coastlines_cluster.py,coastline_diff_compare.py}`

### Raw source: third-party OSM-derived shapefiles, not OSM's own planet dump

```python
COASTLINE_URL = "https://osmdata.openstreetmap.de/download"
LAND_POLYGONS_NAME = "land-polygons-split-4326"
WATER_POLYGONS_NAME = "water-polygons-split-4326"
COASTLINE_LINESTRING_NAME = "coastlines-split-4326"
```

This is a genuinely different upstream from the OSM planet/OSC chain in
§6-§8: it's a mirror maintained by the independent "OSMCoastline" project at
`osmdata.openstreetmap.de` (ODbL-licensed), not `planet.openstreetmap.org`.
The actual download is a black-box shell script inside an ECS Fargate task
(`ecs_download_to_s3`, `src/ecs_helper.py`):

```python
f"apk add --no-cache aria2 aws-cli p7zip && "
f"aria2c -x 16 -s 16 -o /tmp/download.zip '{url}' && "
f"7z x /tmp/download.zip -o/tmp/extracted && "
f"aws s3 cp /tmp/extracted/$FIRST/ {destination} --recursive; "
```

A raw zip download with no checksum/signature verification, unzipped and
copied to S3 verbatim — no parsing of the shapefile happens at this step.

### `ShapefileToParquet` (`overture_coastlines/overture_coastlines/shapefile_to_parquet.py`)

```python
df = self.spark.read.format("shapefile").load(s3_input_shp_path)
df = df.select(select_statement)          # "geometry" only, in this DAG
if subdivide:
    df = df.withColumn("geometry", F.explode(F.expr(f"ST_SubDivide(geometry, {max_vertices})")))
for key, value in json.loads(input_dict).items():
    df = df.withColumn(key, lit(value))    # literal type/ds columns
df.write.format("geoparquet")...save(s3_output_path)
```

Every attribute column from the shapefile except `geometry` is discarded
here. `ST_SubDivide` (run a second time, `subdivide=True, max_vertices=100`,
into a separate `subdivided/` output) is a 1-to-many split — one input
polygon can become several output polygons — nothing is dropped, just
fragmented for downstream join efficiency. No filtering, joins, or dedup.

### `CoastlinesCluster` — diff/QA report, not a data-content transform

Compares the new land/water snapshot against `coastline_base_bundle` (the
last human-approved snapshot, pinned via `COASTLINE_BASE_DS` in
`src/session_parameters.py`): simplifies both sides
(`ST_SimplifyPolygonHull`), equi-joins identical simplified geometries to
exclude unchanged polygons, spatially joins (`ST_Intersects`, bbox then
exact) to find geometries that overlap between snapshots, and calls anything
changed with **no** spatial overlap in the other snapshot a "major change"
(wholesale creation or deletion). DBSCAN clusters those centroids, then
unions raw geometries per cluster into a `new`/`missing` diff report table
(`osm_coastline_diff`). This does not modify the coastline data used
downstream by base — it's a side-channel QA artifact a human reviews before
promoting a new snapshot to become the new `COASTLINE_BASE_DS`.

`theme_base_stage_dag.py`'s own default (`coastline_ds` param defaults to
`COASTLINE_BASE_DS`) means production base builds consume the **vetted**
bundle by default, not necessarily the latest weekly collect — the collect
DAG's job is to produce a *candidate* and diff it before promotion.

`coastline_diff_compare.py` in the same package is dead code — no DAG
references it. The live "compare" stage that populates the public QA
console (`qa-console.overturemaps.org/coastline`) is a separate,
non-Spark container, `containers/coastline-compare/main.py`, which runs raw
DuckDB SQL directly against S3 parquet, shells out to `tippecanoe`
(`os.system(...)`) to build PMTiles, and uploads results via boto3 — worth
flagging as a black-box third-party tool call, distinct from anything in
`overture_coastlines`.

### Output

`SourceRawBundle(provider="osm", resource="coastlines", version=DS)`, at
`datasets/provider=osm/resource=coastlines/version={DS}/run={RUN_ID}/data/{type=land|type=water}` —
this is exactly what `theme_base_stage_dag.py` (§3) passes into `base_land`
and `base_water` as `s3_input_path_coastlines`.

---

## 10. ESA WorldCover — `land_cover_bundle`

**No ingest DAG exists in this repo.** An exhaustive search (across
`airflow/dags/`, all Python packages, dataset config JSON, and
`docs/theme-pipelines/`) found no `source_esa_*`/`dataset_*_worldcover*`
DAG, no download URL, no ECS/sensor task — nothing that populates
`s3://<managed_bucket_source_data>/datasets/provider=esa/resource=esa_worldcover/...`.
`airflow/dags/configs/datasets/esa.json` records only attribution metadata
(name, license `CC-BY-4.0`, source URL `https://esa-worldcover.org/en`) for
the generated release `license.json`, and its `"data_download"` field is an
empty list. `docs/theme-pipelines/base-pipeline/base_workflow_steps.md`
lists "ESA Land Cover" as an **external key input**, consistent with this:
the raw bundle is populated by some manual/out-of-repo process that already
converts ESA's raster/vector WorldCover product into Overture-shaped
Parquet (columns `id, version, subtype, cartography, geometry`) before it
ever reaches this codebase. `theme_base_stage_dag.py` only builds a
`SourceRawBundle` path pointer and resolves whatever partition with a
`success` marker is already sitting there
(`resolve_partitions_task` → `bundle.py`'s S3 partition scan) — for
provenance purposes this is the trace's dead end on the raw side; there is
no code in this repo describing how ESA WorldCover became that Parquet.

`base_land_cover.py`'s transform (already detailed in §4) is the only base
pipeline logic that touches this data: three geometry-sanity filters
(drops null/empty/invalid geometry, unlike bathymetry these are dropped, not
repaired) plus a `sources` provenance stamp. No id re-minting, no
classification rules — the DAG's own comment calls it a copy-through.

---

## 11. NCEI ETOPO/GLOBathy — `bathymetry_bundle`

**Same situation as ESA WorldCover: no ingest DAG in this repo.** No
`source_ncei_*`/`dataset_*_etopo*` DAG, no download URL, no ECS/sensor task
populates `s3://<managed_bucket_source_data>/datasets/provider=ncei/resource=etopo_globathy/...`.
`airflow/dags/configs/datasets/ncei.json`/`globathy.json` record only
attribution metadata (`https://www.ncei.noaa.gov/products/etopo-global-relief-model`,
license `CC0-1.0`) with an empty `"data_download"` list. As with land_cover,
whatever raw ETOPO (bathymetry) + GLOBathy (land-masking) product NCEI
publishes must be fetched, merged, and converted into the
`id, version, depth, cartography, geometry` Parquet shape this pipeline
expects by a process entirely outside this repo, before landing in the
managed source bucket.

`base_bathymetry.py`'s transform (detailed fully in §4) is where real logic
resumes: `ST_MakeValid` repairs (not drops) invalid input geometry — the
job's own reasoning is that dropping a shallow-coverage feature would
wrongly reclassify its footprint as open water — followed by the
grid-chipped shallower-than → deeper-than inversion via two raw
`spark.sql()` blocks (`ST_Intersection`/`ST_Union_Aggr` to compute coverage
per cell, then `ST_Difference` to invert it). Flagging both raw-SQL blocks
explicitly per this trace's brief: `overture_base/overture_base/base_bathymetry.py`
lines ~107-120 and ~125-139.

---

## Summary: linear stage list, raw source → release

Four independent raw-source chains converge at `theme_base_stage_dag`, then
share one path from there to public release.

**OSM chain (feeds `land`, `water`, `land_use`, `infrastructure`):**
1. External HTTP: `planet.openstreetmap.org/replication` OSC changesets, or (bootstrap/reset) a weekly `planet-*.osm.pbf` synced into S3 from outside this repo — `omf/omf/utilities/osm_geometry.py: OSCData`, `airflow/dags/osm/dataset_osm_geometry_reset_dag.py`.
2. PBF → Parquet conversion via a black-box ECS Fargate container — `dataset_osm_geometry_reset_dag.py` (`convert_planet_pbf_to_parquet`).
3. Node/way/relation geometry construction (bootstrap: `OSMGeometryPlanet`; daily: `OSMGeometryOSC` applying OSC diffs onto yesterday's table) — `omf/omf/osm/osm_geometry_{planet,osc}.py`, `omf/omf/utilities/osm_geometry.py` → `geometry_daily` / `geometry_planet` bundles.
4. OSM tag classification into Overture base sub-types (tag pre-filter → declarative rules table → id minting → column select) — `dataset_osm_ingest_dag.py` → `overture_base/{land,water,land_use,infrastructure}_from_osm.py` → `osm_in_overture` bundle.
5. Violation-store adjudication: merge today's fixed features with sprint-reset vetted features per Iceberg violation/fast-forward tables (or plain format copy on reset days) — `dataset_osm_adjudicator_dag.py`, `omf/omf/adjudicator/osm_adjudicator.py` → `overture_rc` bundle.

**OSM Coastlines chain (feeds `land`, `water` ocean/land polygons):**
6. External HTTP zip download from `osmdata.openstreetmap.de` via ECS shell task, shapefile → GeoParquet conversion, base-vs-latest diff/QA clustering — `dataset_osm_coastline_collect_dag.py`, `overture_coastlines/{shapefile_to_parquet,coastlines_cluster}.py` → `coastlines` bundle.

**ESA WorldCover chain (feeds `land_cover`):**
7. Raw ESA WorldCover product fetched/converted to Overture-shaped Parquet by a process outside this repo → `esa_worldcover` bundle (repo-visible trace starts here).
8. Geometry-sanity filter + provenance stamp, pass-through otherwise — `overture_base/base_land_cover.py`.

**NCEI ETOPO/GLOBathy chain (feeds `bathymetry`):**
9. Raw ETOPO/GLOBathy product fetched/converted to Overture-shaped Parquet by a process outside this repo → `etopo_globathy` bundle (repo-visible trace starts here).
10. Repair invalid geometry, grid-chip and invert "shallower than d" coverage into "deeper than d" polygons, re-mint ids — `overture_base/base_bathymetry.py`.

**Converged base pipeline (all six sub-types):**
11. Per-sub-type base job: geometry sanity filters, name promotion, id re-mint safety net, sub-type-specific rules (coastline union for land/water, polygon→line rewrite for water canals), dedup by id keeping the freshest source — `theme_base_stage_dag.py` → `overture_base/base_{land,water,land_use,infrastructure,land_cover,bathymetry}.py` → `ThemeAssembleBundle(theme="base")`.
12. Schema validation gate, internal changelog computation (assigns real public `version` numbers by diffing against the previous release), churn-threshold gate, block-list + optional bbox filtering, spatial repartitioning/rewrite, public changelog, metrics, PMTiles, bridge files, final validation — `theme_base_stage_dag.py`'s output consumed by `theme_promote_dag.py` → `overture_cdp/{compute_internal_changelog,process_data,compute_public_changelog}.py` → `ThemePromoteBundle`.
13. DataSync/copy to public AWS/Azure/archive release buckets, PMTiles multipart copy, STAC catalog publish + CloudFront invalidation, Glue crawler refresh, bundle tagged as released — `release_publish_dag.py`.
