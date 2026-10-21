# Transform shapes across the theme pipelines

A survey of the PySpark transform patterns that exist in the Overture theme
pipelines, gathered to understand what a provenance library has to accommodate.
Compiled 2026-08-19 from `overture_buildings`, `overture_base`,
`overture_coastlines`, `overture_places`, `overture_places_embeddings`,
`overture_transportation`, `overture_addresses`, `osm_checks`, and
`overture_quality`.

Coverage note: `overture_divisions` and `overture_corpus` are Scala/JVM, not
PySpark, so they are outside a PySpark provenance library's reach entirely.

Each entry is one representative example of a distinct shape, not an exhaustive
index. `file:line` paths are relative to the repo root.

## Catalog, by transform shape

### linear (1:1, identity preserved — the `@track` case)

- `overture_buildings/overture_buildings/building_common.py:922` `with_height` — coalesce two tag keys, parse/validate units, nullify out-of-range into a new column
- `overture_buildings/overture_buildings/building_common.py:978` `with_is_underground` — boolean from a tag equality check via when/otherwise
- `overture_base/overture_base/tag_classification.py:235` `rules_to_column` — compile a rule list into one chained when/otherwise struct expression
- `overture_addresses/overture_addresses/ingest/normalizer.py:168` `_nullify_empty_strings` — blank strings to null across all string columns
- `overture_addresses/overture_addresses/ingest/normalizer.py:179` `_nullify_disallowed` — junk-token values ("n/a", "sn") to null
- `overture_quality/overture_quality/utils/address_utils.py:93` `normalize_txt_column` — pure Catalyst text normalization, no UDF
- `overture_transportation/overture_transportation/osm_ways_to_segments.py:216` `_transform_to_overture_schema` — column-by-column reshape of an OSM way into Overture schema
- `overture_quality/overture_quality/corpus_provider_grain.py:36` `pin_sources_to_provider` — rewrite `sources` to a synthesized single-provider struct

### drop (records removed)

- `overture_buildings/overture_buildings/building_filter.py:50` — `join(violation_df, how="anti")`, the release-time drop
- `overture_buildings/overture_buildings/building_matcher.py:189` — anti-join removes duplicate-match records flagged by a prior window step
- `overture_places/overture_places/places_data_providers/places_filter_chain.py:196` — `left_anti` on hard-violation ids, the reference drop
- `overture_base/overture_base/base_common.py:95` `dedup` — `sort().dropDuplicates(["id"])`, keep most-recent source per id
- `overture_buildings/overture_buildings/building_common.py:94` `filter_by_bbox` — spatial `ST_Intersects` filter
- `overture_addresses/overture_addresses/ingest/providers/nad.py:201` `_apply_us_clips` — `left_anti` spatial join against broadcast boundaries
- `overture_transportation/overture_transportation/matcher/gap_fill_matcher.py:392` — window-rank then `filter(rank==1)` (the dedupe idiom)
- `osm_checks/osm_checks/way_duplicate_nodes.py:88` — `join(how="semi")` filter to referenced nodes
- `overture_coastlines/overture_coastlines/coastlines_cluster.py:164` — raw-SQL `WHERE NOT id IN (SELECT ...)` anti-join
- `overture_places/overture_places/places_embed.py:356` — `dropDuplicates(["input_hash", "field_type"])`

### merge (many records collapse into one)

- `overture_buildings/overture_buildings/building_spatial_merge.py:72` — priority-suppression conflation: a building intersecting any higher-priority source is suppressed
- `overture_places/overture_places/merge/base_properties_feeds_merger.py:118` — `groupBy(id).agg(collect_list(struct))` then an RDD `.map()` that priority-suppresses ranked duplicates (two-layer)
- `overture_places/overture_places/merge/attribute_mergers/category_merger.py:11` `CategoryMerger.merge` — per-field Python conflation of two ranked sources (also `names_merger.py`, `list_merger.py`)
- `overture_transportation/overture_transportation/osm_merge_segments.py:56` — `groupBy(component).collect_list` + a UDF graph-traversal chain-merge of connected segments
- `overture_base/overture_base/base_bathymetry.py:72` — `groupBy(depth).agg(first(...))` collapses coverage polygons per depth level
- `overture_coastlines/overture_coastlines/coastlines_cluster.py:258` `aggregate_geometries_in_clusters` — raw-SQL `GROUP BY cluster` with `ST_Union_Aggr`
- `overture_quality/overture_quality/quality_score.py:485` `_dedupe_on_keys` — window collapse to one row per merge-key group
- `osm_checks/osm_checks/important_feature.py:34` — raw-SQL `GROUP BY osm_type, osm_id` with `FIRST()`/`COLLECT_LIST()`, concatenating tag diffs
- `overture_places/overture_places/website_resolve_merge.py:45` — raw-SQL `MERGE INTO` upsert into Iceberg
- `overture_quality/overture_quality/feature_store.py:183` — raw-SQL `MERGE INTO ... UPDATE SET * ... INSERT *`

### id-rebind (a join/coalesce that changes a record's identity column)

- `overture_buildings/overture_buildings/building_matcher.py:199` — `COALESCE(release_id, feed.id) AS id`, adopt the matched GERS id (else keep own). **Adopt-external, 1:1.**
- `overture_transportation/overture_transportation/orbis_ways_to_segments.py:402` — adopt `tags.gers_identifier` as the id. **Adopt-external, 1:1.**
- `overture_base/overture_base/base_land.py:45` — `when(valid_uuid, id).otherwise(uuid3(...))`, deterministic content-hash id via UDF. **Mint-deterministic.**
- `overture_base/overture_base/water_from_osm.py:72` — same, expressed as a `selectExpr` SQL string.
- `overture_transportation/overture_transportation/osm_merge_segments.py:873` — a merged segment gets a fresh `uuid()` replacing its constituents. **Fan-in mint, N:1.**
- **addresses gap** — `overture_addresses/overture_addresses/ingest/providers/oa.py:167` emits `id = null` with the native key in `sources[].record_id`; the rebind happens in a downstream, separate match job. **Cross-job rebind.**

### join (enrichment, not itself a drop or merge)

- `overture_buildings/overture_buildings/building_tag_merge.py:98` — three chained `left` joins (esri/microsoft/lidar height) feeding a later best-height resolution
- `overture_buildings/overture_buildings/building_stage.py:41` — `left` join to distinct `building_parts.building_id` to derive a `has_parts` flag
- `overture_buildings/overture_buildings/building_intersect.py:38` — spatial inner `ST_Intersects` join to attach a counterpart for a violation
- `overture_places/overture_places/places_data_providers/places_filter_chain.py:206` — `left` join then nullify matched columns (soft-filter)
- `overture_places/overture_places/places_data_providers/spatial_filter_mixin.py:260` — broadcast + `ST_Intersects` spatial enrichment
- `osm_checks/osm_checks/relation_duplicate.py:47` — raw-SQL self cross-join to find exact-duplicate pairs

### udf (UDF / pandas_udf / mapPartitions)

- `overture_base/overture_base/base_common.py:17` `generate_uuid3` — plain `@F.udf` namespaced UUID3 (used for id assignment)
- `overture_places/overture_places/places_embed.py:194` — `@pandas_udf` batched model inference, int8-quantized embeddings
- `overture_transportation/overture_transportation/matcher/ml_scorer.py:43` — `@pandas_udf` XGBoost scoring
- `overture_transportation/overture_transportation/matcher/ml_features.py:258` — multi-output struct `pandas_udf` for fuzzy-match name features
- `overture_transportation/overture_transportation/combobulator/pyspark_udf.py:371` — UDF returning a large nested struct of derived road properties
- `overture_places/overture_places/places_data_providers/util/tomtom_geocoder.py:974` — `rdd.mapPartitionsWithIndex` for QPS-throttled geocoding
- `osm_checks/osm_checks/relation_open_ways.py:38` — `spark.udf.register` closure that stitches line fragments into rings, called from SQL

### window (row_number / rank / lag)

- `overture_buildings/overture_buildings/building_filter.py:21` — `row_number().over(partitionBy(id).orderBy(record_id))` + `filter(==1)` (the dedupe idiom)
- `overture_places/overture_places/places_data_providers/places_filter_chain.py:97` — `row_number` dedup by id, ordered by `names.primary`
- `overture_quality/overture_quality/utils/similarity.py:108` `get_best_match` — `row_number` to pick the single best candidate per input
- `overture_buildings/overture_buildings/building_pre_match_filter.py:154` — `count().over(partitionBy(record_id))` to flag duplicate record_ids
- `overture_transportation/overture_transportation/matcher/base.py:701` — `lag()` to find consecutive-match breaks
- `overture_transportation/overture_transportation/transportation_qa.py:63` — `sum().over(empty window)` for a global total without a second action

### explode (1 -> N, id often repeated)

- `overture_buildings/overture_buildings/building_parts_from_osm.py:58` — `explode(members)`, one row per OSM relation member
- `overture_buildings/overture_buildings/tile_water.py:52` — raw-SQL `explode(ST_SubDivide(geom))`, one polygon becomes many, same id repeated
- `overture_quality/overture_quality/corpus_provider_grain.py:14` `explode_corpus_providers` — `explode(sources)`, one row per `(id, provider)`
- `overture_transportation/overture_transportation/osm_ways_to_segments.py:116` — `posexplode(nds)`, position-preserving explode of node references

### raw-sql (the transform is a SQL string)

- `overture_buildings/overture_buildings/building_from_osm.py:83` — an entire OSM-history-to-building pipeline (dedup, joins, filters, union) as one CTE query
- `overture_buildings/overture_buildings/building_matcher.py:106` — spatial cross-join-with-predicate IoU matching as SQL
- `overture_coastlines/overture_coastlines/coastlines_cluster.py` — nearly the whole pipeline as chained view-to-view SQL
- `osm_checks/osm_checks/utils.py:230` `SparkCheckLegacy.materialize_check` — runs a caller-supplied `spark.sql(self.query)` string against temp views
- `osm_checks/osm_checks/violation_store_delete.py:24` — raw-SQL `MERGE INTO ... WHEN MATCHED THEN DELETE`

### aggregation (stats, not identity-merge)

- `overture_places/overture_places/places_data_providers/places_filter_chain.py:285` `_compute_filter_stats` — count/groupBy-count into a FilterStats summary
- `overture_buildings/overture_buildings/building_post_merge_filter.py:130` — `groupBy(id).agg(count(...))` on exploded corner angles, then filter on the count
- `overture_quality/overture_quality/training_bundle/compute_category_counts_job.py:51` — `groupBy(category).count()`
- `overture_quality/overture_quality/cross_theme/features.py:25` `buffer_count` — `groupBy(key).agg(count(...))` then join back as a feature

### shapes the category rubric did not anticipate

- **opaque whole-DataFrame transform** — `overture_coastlines` calls `sedona.stats.clustering.dbscan`; `matcher/ml_scorer.py` runs a model. Records in, records out, no per-record logic cairn can inspect.
- **soft-filter / nullify-on-violation** — `places_filter_chain.py:206` keeps the row but nulls its bad fields. This is a revision (`wasRevisionOf`), not a drop.

## Design implications

1. **Per-field merge provenance, not just per-record.** The places attribute-mergers decide which source won which field (height from microsoft, name from OSM) inside a Python `.map()` the decorator cannot see. A `source_ids` array captures "these records merged" but not "this field came from that source." That finer grain is real; the merge function itself would have to report it.

2. **Dedupe-by-window is the single most common drop, in every theme.** `row_number().partitionBy(k).orderBy(o).filter(==1)`. Given the ubiquity, it likely deserves a first-class recorder (`track_dedup(df, keys, order)`) rather than being expressed as a generic before/after diff every time.

3. **id-rebind has four flavors, and one is cross-job.** Adopt-external (GERS coalesce), mint-deterministic (uuid3 from content), fan-in-mint (merged segment gets a new uuid), and — for addresses — a rebind that happens in a *separate downstream job*. The last is the two-id-space and cross-bundle problems combined: an intra-bundle tracker cannot see it.

4. **raw-SQL is not an edge case; sometimes it is the whole pipeline,** and `osm_checks` runs caller-supplied SQL strings. Inside these, cairn is blind, and the only tool is a before/after diff of the whole thing or the escape hatch. Notably, `SparkCheck`'s "SQL string produces a violations DataFrame" shape is exactly the existing violation-store pattern, which argues the drop recorder should accept a pre-built violations frame, not only a predicate.

5. **explode breaks the "id names one record" assumption.** After `explode`, many rows share an id (subdivided water, provider-grain corpus). Any event keyed on `record_id` after an explode is ambiguous about which exploded row. The current model does not account for identity becoming non-unique mid-pipeline.

6. **Most UDF-heavy work is linear.** Embeddings, geocoding, ML scoring, combobulator — they enrich a column and the record survives. `@track` (definition layer) is enough there; no row-level event is needed unless they also drop or rebind. The expensive-looking work is the easy case.

7. **A single job composes many shapes per-step.** `building_matcher.py` chains raw-sql join, window dedup, anti-join drop, id-rebind coalesce, and a second window dedup in ~150 lines. Instrumentation has to compose per-step, which either means breaking these into `@track`ed helpers or accepting job-level granularity.
