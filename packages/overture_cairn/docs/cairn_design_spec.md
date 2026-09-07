## Overview

### Terminology

* *"cairn"* - A pile of stones used to mark a path.
* *"Cairn"* - The provenance tracking system specified here.
* *"a Cairn"* - A pair of tables logging how a dataset was created

These two tables are:

1. An `operations` table that tracks the dataset-level transformations performed by a specific instance of a process.
2. A `row_detail` table that includes, for each entry in the `operations` table (optionally),  row-level information about the transformation.

If your pipeline has multiple Cairns, you should be able to collect them by concatenating their respective tables.

## Table Schemas

### Operations

An operation is essentially *a directed relationship* that transforms one or multiple input datasets into a *single* output dataset. The operations table is a *catalog* of these relationships, but it does not contain information regarding *what* was mapped to what.

| Column | Description | Reason for inclusion | Example |
| -- | -- | -- | -- |
| `op_id` | The ID of this operation; distinct across runs and bundles. I suggest a composite `{run_id}.{slug(op_key)}`, the definitions for these columns follow.| The operation identifier that we use to build the lineage graph. | `20260903XXXX.reduce_precision (str)` |
| `run_id` | Put simply, the `run_id` of the bundle in which this Cairn lives. | This column makes records distinguishable when Cairns from different bundles are concatenated. | `20260903XXXX (str)` |
| `op_key` | This is a (descriptive) slug of the operation name. | Gives us a pseudo-stable identifier that can be tracked across code versions | `reduce_precision (str)` |
| `output_key_columns` | The columns that the output of the operation is keyed on. | Necessary for knowing how to trace the lineage graph at the row-level. | `["gers_id", "provider"] (array[str])` |
| `description` | Plain-text description of *what* this operation did. | Makes the lineage graph human-interpretable and establishes a way to understand our pipelines without reading code. | `"Trailing/leading whitespace is removed from names." (str)` |
| `has_row_detail` | If `true`, then this operation *may* have associated entries in the `row_detail` table. | This is mostly for convenience and tracking how well we're capturing lineage. | `true (bool)` |
| `physical_source` | What physical location this operation reads from, if any. If the operation reads from multiple physical locations, it should be split into smaller operations. | This lets the bundle-entrypoint operations specify what sources they draw information from. | `"s3://overture-stuff/data.json" (str)` |
| `physical_dest` | What physical location this operation writes to, if any. If an operation writes to multiple locations, it should be split into multiple operations. | This lets bundle-exit point operations specify what they end up materializing. | `"s3://overture-stuff/output.parquet" (str)` |
| `input_op_ids` | The IDs of the operations that feed into this one, if any. | This is, perhaps, initially counterintuitive: if operations are edges in the data transformation graph, why are we associating edges with one another? But recall: some operations *never materialize their data*. Thanks to Spark's Catalyst optimizer, an intermediate function that accepts a PySpark `DataFrame` and outputs another is *never even guaranteed to have the output of that function in memory*. Because of this, in most of Overture's use cases, ops link *directly to other ops*. In other words, the operations table is its own graph, with operations as nodes and `input_op_ids` as its edges; this sits one level above the finer graph that `row_detail` builds, where records are the nodes and `kind` names the edges between them. | `["20260903XXXX.reduce_precision", "20260903XXXX.simplify_geometry"] (array[str])`
| `timestamp` | When this operation was logged. | This column does not necessarily indicate when the operation *happened* (in some cases this may be an unanswerable question) -- it just captures when the operation record was added to Cairn. | `2026-09-07 13:01:00 (timestamp)` |

#### Per-Cairn Operations Table Validations
* If `physical_source` is not null, `input_op_ids` must be empty.
* If `physical_dest` is not null, `input_op_ids` has exactly one entry.
* If an operation's `physical_dest` is not null, no other operation is allowed to reference it in its `input_op_ids`.
* The directed graph formed by joining `op_id`s by `input_op_ids` must be acyclic.
* Every element of `input_op_ids` is present in the aggregate `op_id`s of the Cairn.
* Each `op_id` is unique.

Given these requirements, the "type" of operation is actually easily inferrable from the operation's parameters:
* "Read" operations have non-null `physical_source`.
* "Write" operations have non-null `physical_dest`.
* "Copy" operations have non-null `physical_source` AND non-null `physical_dest`.
* All other operations (transforms, joins, filters, etc.) have null `physical_source` and `physical_dest`.

These rules also enforce a level of tracking detail: if a job reads from multiple locations, it must be split into multiple "Read" operations. These "Read" operations can then be combined via additional operation(s) with `len(input_op_ids)>1`.

### Row Detail
| Column | Description | Reason for inclusion | Example |
| -- | -- | -- | -- |
| `op_id` | Which operation was run. | This gives us a foreign key into the operations table. | `20260903XXXX.reduce_precision (str)` |
| `kind` | The kind of relationship (`drop`, `mint`, `derived_from`, `content_change`, `flagged`) | Justification below. | `"mint" (str)` |

### Cairn does not support column-level tracking alone

Cairn does not track columnar lineage in the Operations table. To explain why, consider the hypothetical case where places datasets A and B are merged and the phone number is selected per-record by whichever record has the higher confidence. In this scenario, column tracking at the row level is clear: there would be an entry for each record indicating *which* phone number was "passed through". But at the dataset-level, the only capturable information is the *logic* which was used to select the phone number.

A structured, dataset-level record of *which* columns fed which would have to describe arbitrary selection logic: confidence comparisons, priority orders, fallback chains, and so on. That logic does not reliably fall into a small, closed schema, and attempting to track it would probably be more trouble than it's worth. Instead, each operation holds a plaintext `description` and a pointer to its associatd code.

#### Justification for `kind`:


## Addendum: Loose Mapping to W3C PROV

The most comprehensive existing provenance data model is W3C's [PROV-DM](https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/). W3C Prov is composed of three types of nodes: **Entities** (information), **Activities** (processes), and **Agents** (actors). Here is how each is represented in Cairn:
* **Entities** are *our data*. By construction, Cairn is a relational ontology, i.e. it does not track entities -- just the *relationships* between Entities. At detailed grains, the names (IDs) of the Entities must be stored, but in general Cairn does not materialize Entities in their own table, and does not store any information re. Entities other than their names (IDs).
* **Activities** are *code*. Again, Cairn does not maintain an Activity mirror, but it *references* them by pointing at code versions.
* **Agents** have no parallel. In the PROV-DM, "Agent" may refer to a specific human, institution, or AI actor. Cairn is specifically for tracking *automated data processing pipelines*, and, consequently, is only concerned with the last case. Because AI actors are only callable through code, Cairn simply records the programmatic activity, from which the Agent is recoverable.

W3C PROV joins these nodes with **Relations**, such as `wasDerivedFrom` or `wasGeneratedBy`. The most-analogous concept in Cairn is the classification described by row detail in `kind`, but this is probably a misleading comparison.

Crucially, Cairn differs from the W3C PROV-DM recommendation in that Cairn is a *lineage* tracking system, while PROV-DM is a *provenance* tracking system. In other words:
> **The nodes in Cairn's lineage graph are records (data)**. IN PROV-DM, records, actors, and processes all get their own nodes.

Tabulated:
| Model | Nodes | Edges |
| -- | -- | -- |
| Cairn | Datasets, Records | Operations with rich metadata |
| W3C PROV-DM | Datasets, Records, People, Processes, Activities | Relations with minimal metadata |