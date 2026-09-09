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

| Column | Description | Reason for inclusion | Example + type |
| -- | -- | -- | -- |
| `op_id` | The ID of this operation; distinct across runs and bundles. I suggest a composite `{run_id}.{slug(op_key)}`, the definitions for these columns follow.| The operation identifier that we use to build the lineage graph. | `20260903XXXX.reduce_precision (str)` |
| `run_id` | Put simply, the `run_id` of the bundle in which this Cairn lives. | This column makes records distinguishable when Cairns from different bundles are concatenated. | `20260903XXXX (str)` |
| `op_key` | This is a (descriptive) slug of the operation name. | Gives us a pseudo-stable identifier that can be tracked across code versions | `reduce_precision (str)` |
| `code_ref` | The fully-qualified module and function where this operation's logic lives. | Unlike `op_key`, this field is automatically filled and is not guaranteed to be persisted across refactors. | `"overture_base.base_land.promote_names" (str)` |
| `code_version` | The commit (or other revision id) of the code that was run for this operation. | Automatically filled; pinpoints the exact code that was run when paired with `code_ref`. | `"a1b2c3d" (str)` |
| `output_key_columns` | The columns that the output of the operation is keyed on. | Necessary for knowing how to trace the lineage graph at the row-level. | `["gers_id", "provider"] (array[str])` |
| `description` | Plain-text description of *what* this operation did. | Makes the lineage graph human-interpretable and establishes a way to understand our pipelines without reading code. | `"Trailing/leading whitespace is removed from names." (str)` |
| `has_row_detail` | If `true`, then this operation *may* have associated entries in the `row_detail` table. | This is mostly for convenience and tracking how well we're capturing lineage. | `true (bool)` |
| `physical_source` | What physical location this operation reads from, if any. If the operation reads from multiple physical locations, it should be split into smaller operations. | This lets the bundle-entrypoint operations specify what sources they draw information from. | `"s3://overture-stuff/data.json" (str)` |
| `physical_dest` | What physical location this operation writes to, if any. If an operation writes to multiple locations, it should be split into multiple operations. | This lets bundle-exit point operations specify what they end up materializing. | `"s3://overture-stuff/output.parquet" (str)` |
| `input_op_ids` | The IDs of the operations that feed into this one, if any. | This is, perhaps, initially counterintuitive: if operations are edges in the data transformation graph, why are we associating edges with one another? But recall: some operations *never materialize their data*. Thanks to Spark's Catalyst optimizer, an intermediate function that accepts a PySpark `DataFrame` and outputs another is *never even guaranteed to have the output of that function in memory*. Because of this, in most of Overture's use cases, ops link *directly to other ops*. In other words, the operations table is its own graph, with operations as nodes and `input_op_ids` as its edges; this sits one level above the finer graph that `row_detail` builds, where records are the nodes and `kind` names the edges between them. | `["20260903XXXX.reduce_precision", "20260903XXXX.simplify_geometry"] (array[str])` |
| `passthrough_input_op_ids` | Which of `input_op_ids` a reader may take an absent `row_detail` entry to speak for. A record of one of these inputs with no entry survived this operation under the same identity. A record of any other input has no row-level relationship to the output established by its absence. | Absence carries a claim, and for a multi-input operation nothing else says which input that claim covers. Two tables keyed by `id` routinely play different roles in one join, so matching `output_key_columns` cannot answer this: key columns say how to identify a record, not whether that record belongs in the output. An empty list authorizes no inference, which is different from saying no records contributed, since explicit entries still record what they did. | `["20260903XXXX.read_feed"] (array[str])` |
| `timestamp` | When this operation was logged. | This column does not necessarily indicate when the operation *happened* (in some cases this may be an unanswerable question) -- it just captures when the operation record was added to Cairn. | `2026-09-07 13:01:00 (timestamp)` |

#### Per-Cairn Operations Table Validations
* If `physical_source` is not null and `physical_dest` is null, `input_op_ids` must be empty. A read draws from a location, so it consumes no operation.
* If `physical_dest` is not null, `input_op_ids` has exactly one entry. This covers writes and copies, which both put one operation's output somewhere.
* If an operation's `physical_dest` is not null, no other operation is allowed to reference it in its `input_op_ids`.
* The directed graph formed by joining `op_id`s by `input_op_ids` must be acyclic.
* Every element of `input_op_ids` is present in the aggregate `op_id`s of the Cairn.
* Each `op_id` is unique.
* If `physical_source` or `physical_dest` is not null, `has_row_detail` is `false`. Reads, writes, and copies move records without touching their contents, so they have nothing to report.
* If `output_key_columns` is not null, it names at least one column, no column twice, and nothing blank. Row detail ids are read positionally against it, so a repeated name would make an id ambiguous.
* If `has_row_detail` is `true`, `output_key_columns` is not null. Entries whose ids have no key to be read against cannot be interpreted.
* `run_id` is unique across every Cairn that will ever be collected together, not only within a theme. `op_id` uniqueness rests entirely on this.
* Every element of `passthrough_input_op_ids` appears in `input_op_ids`, and none appears twice.
* If `physical_dest` is not null, `passthrough_input_op_ids` equals `input_op_ids`. A write and a copy carry every record they handle to a location and can hold no row detail, so absence is the only available reading.
* If `physical_source` is not null and `physical_dest` is null, `passthrough_input_op_ids` is empty. A read consumes no operation, so it has no input to pass records through from.

Given these requirements, the "type" of operation is actually easily inferrable from the operation's parameters:
* "Read" operations have non-null `physical_source`.
* "Write" operations have non-null `physical_dest`.
* "Copy" operations have non-null `physical_source` AND non-null `physical_dest`.
* All other operations (transforms, joins, filters, etc.) have null `physical_source` and `physical_dest`.

These rules also enforce a level of tracking detail: if a job reads from multiple locations, it must be split into multiple "Read" operations. These "Read" operations can then be combined via additional operation(s) with `len(input_op_ids)>1`.

### Row Detail
| Column | Description | Reason for inclusion | Example + type |
| -- | -- | -- | -- |
| `op_id` | Which operation was run. | This gives us a foreign key into the operations table. | `20260903XXXX.reduce_precision (str)` |
| `input_op_id` | If an operation had multiple inputs ops, which input does this Row Detail entry refer to? | Recall that an operation may have multiple `input_op_ids`, but the row-detail level is exclusively (single id → single ID) mappings. To distinguish *which* dataset an ID in this table belongs to, we need to record *which* of the `input_op_ids` in the operations table was referenced. This column is null when the operation has only one input. | `20260903XXXX.simplify_geometry (str)` |
| `kind` | The kind of relationship: `dropped`, `minted`, `derived_from`, `content_changed`, or `flagged`. | Justification below. | `"minted" (str)` |
| `input_id` | The input record ID. | - | `["42", "tomtom"] (array[str])` |
| `output_id` | The output record ID. | - | `["1337", "meta"] (array[str])` |
| `affected_output_columns` | This array asks: which columns in the output table does this entry concern? `Null` means 'all of them'. | If `kind=derived_from`, these are the columns the input record supplied. If `kind=content_changed`, these are the columns that changed. If `kind=flagged`, these are the flagged columns (violations, check results). | `["height", "name"] (array[str])` |
| `column_change` | What happened to the values in `affected_output_columns`: `set` (previously empty), `replaced` (overwritten), or `cleared` (removed). One value per entry. If the answer varies depending on the column, then just write multiple rows to this table. | `affected_output_columns` says which columns were transfered from input to output; this adds detail to that, indicating whether it's a replacement/blanking/removal. It's only meaningful when `kind=content_changed` because that's the only time when a record has a before and an after to compare. Implementers shouldn't normally have to fill this by hand, and the field is generally nullable and optional. | `"set" (str)` |
| `detail` | How this record was changed. | The "why" lives in the operations table, the "how" lives here. | - |

#### Justification for `kind`
`kind` answers the question "what happened to the IDs present in this operation?". It's not intended to be a complete taxonomy of the many ways a row can be affected, just an indicator of how the input ID and data relate to the output ID and data. It is best described by the following pseudocode:
```python
def determine_kind():
    if not has_input_id and has_output_id:
        # We generated a new ID
        return "minted"
    if has_input_id and not has_output_id:
        # We dropped an ID
        return "dropped"
    if has_input_id and has_output_id:
        if input_id != output_id:
            # One ID affected another
            return "derived_from"
        elif input_id == output_id:
            if any(value_changed(col) for col in columns):
                # The ID is the same, but the data changed
                return "content_changed"
            else 
                # We're just flagging this row
                return "flagged"
    raise SomeError("Need at least one ID per row detail entry!")
```

Incidentally, `column_change` is this same classification applied one grain down. At the cell level, `set` is a mint, `cleared` is a drop, and `replaced` is a content change. The missing relative is a cell-level `derived_from` ("this value came from that other column"), which is deliberately left to `detail` prose for the reasons in the column-tracking section below.

#### Column-level provenance on merges

Merges don't use `column_change` (there's no before-state on the output record to compare against), but they still carry column-level provenance through `affected_output_columns`. Consider records A and B merging into C, where A is the higher-confidence base record but B's website wins its field:

| kind | input_id | output_id | affected_output_columns | detail |
| -- | -- | -- | -- | -- |
| derived_from | [A] | [C] | null | base record, won on confidence |
| derived_from | [B] | [C] | ["websites"] | higher-confidence website |

"Which record did C get its website from?" is answered by finding the `derived_from` entry into C whose column list names it. Note that entries group their columns differently depending on `kind`: a `derived_from` entry covers one input record, so it lists every column that record supplied, while a `content_changed` entry covers one `column_change` value, so it lists every column that met the same fate. The two never collide, because a `content_changed` entry has exactly one input and a `derived_from` entry has no fate to record.

#### Per-Cairn Row Detail Table Validations
* Every operation referenced in `row_detail` (via `op_id`) must have null `physical_source` and `physical_dest`.
* Every `op_id` and `input_op_id` in Row Detail must exist in the Operations table.
* `input_op_id` is non-null if and only if the associated operation's `input_op_ids` has more than one entry.
* `input_op_id` must be one of the parent operation's `input_op_ids`.
* If `kind=minted`, `input_id` and `input_op_id` must be null.
* If `kind=dropped`, `output_id` must be null.
* If `kind=content_changed` or `kind=flagged`, `input_id` must equal `output_id`.
* If `kind=derived_from`, `input_id` must differ from `output_id`.
* `column_change` must be null unless `kind=content_changed`.
* If `kind=dropped` or `kind=minted`, `affected_output_columns` must be null.
* Any operation carrying entries has `has_row_detail=true`.
* If `kind=content_changed`, at least one of `affected_output_columns` and `detail` is non-null. An entry with neither says something changed without saying what. A `flagged` entry needs neither, since the operation it belongs to is the finding.
* `output_id` has as many parts as the operation's own `output_key_columns`.
* `input_id` has as many parts as the `output_key_columns` of the operation that produced that record, which is `input_op_id` when the entry names one and the operation's sole input otherwise.
* If the operation's `input_op_ids` is empty, `input_id` must be null. An operation consuming nothing has no input records, so an id on the way in would name a row from no dataset. Such an operation can still write `minted` entries.

#### When to name columns

Column-level detail is only necessary when *more than one input could have supplied the output value*.

Including it at other times is allowed. The thing to avoid is mixing conventions inside one operation: `null` means "all of them", so naming columns on some entries and not others leaves a reader unable to tell which of the two a `null` meant.

#### What an absent entry means

Row detail entries are deltas, so absence carries a claim rather than saying nothing. The claim is scoped by `passthrough_input_op_ids`:

> For a declared pass-through input, a record with no row detail entry survived under the same identity. For any other input, absence establishes no row-level relationship to the output.

"Same identity" rather than "unchanged", because an operation is allowed to rewrite values by a uniform rule without writing an entry per record. A whitespace strip changes every name it touches and records nothing, and its records still passed through.

Which inputs qualify, by shape of operation:

| Operation | Inputs eligible for implicit pass-through |
| -- | -- |
| Identity-preserving filter or normalization | Its one input |
| Base-table enrichment | The base input only |
| Feed matched against a corpus | The feed input only |
| Union with a valid output key | Both inputs |
| Aggregate, or a fully explicit derivation | None |

An empty list authorizes no inference. That is different from claiming no records contributed, since explicit entries still say what they did. In particular, an unmentioned record of a non-declared input should not be read as unused, because nothing guarantees every contribution was recorded.

Matching `output_key_columns` is a compatibility check and never a substitute for the declaration. A declaration naming an input keyed differently from the output is worth questioning, but two tables keyed by `id` routinely play different roles in one join, and a rename that leaves identity alone or a group-by on the same column both make key comparison misleading.

### Cairn does not support column-level tracking alone

Cairn does not track columnar lineage in the Operations table. To explain why, consider the hypothetical case where places datasets A and B are merged and the phone number is selected per-record by whichever record has the higher confidence. In this scenario, column tracking at the row level is clear: there would be an entry for each record indicating *which* phone number was "passed through". But at the dataset-level, the only capturable information is the *logic* which was used to select the phone number.

A structured, dataset-level record of *which* columns fed which would have to describe arbitrary selection logic: confidence comparisons, priority orders, fallback chains, and so on. That logic does not reliably fall into a small, closed schema, and attempting to track it would probably be more trouble than it's worth. Instead, each operation holds a plaintext `description` and a pointer to its associated code.

## Examples

These show how common pipeline shapes land in the two tables. The `run_id` prefix on op ids is omitted for brevity, and each table only shows the columns that matter for the example.

### A filter

Almost every job has this skeleton, where a read feeds a transform which feeds a write. The transform here is a validity filter.

Operations:

| op_key | description | physical_source | physical_dest | input_op_ids | has_row_detail |
| -- | -- | -- | -- | -- | -- |
| read_land | Read staged land features | s3://.../land/ | null | [] | false |
| drop_invalid_geometry | Remove rows whose geometry fails validity checks | null | null | [read_land] | true |
| write_land | Write cleaned land features | null | s3://.../land_clean/ | [drop_invalid_geometry] | false |

Row detail for `drop_invalid_geometry`:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| dropped | ["osm/w123"] | null | invalid geometry |

Only the removed rows get entries. A record with no entry passed through untouched, so most records do not need an entry. The read and write ops have no row detail of their own.

### Minting ids

A transform carves records out of unkeyed data (say, land polygons cut from a coastline) and gives each one a fresh id:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| minted | null | ["550e8400..."] | new land feature derived from coastline |

Mints come from transforms. A read whose records already carry ids records nothing here, since those identities existed before Cairn saw them. An operation that hands new ids to records which already had one (a provider key, say) records `derived_from` entries, which the next example covers.

### Changing ids

A matcher assigns final ids: records that match an existing corpus record take its id, and unmatched records keep their own.

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| derived_from | ["tmp-8f3e"] | ["gers-04c2"] | matched existing building, IoU 0.87 |

One entry per record whose id changed. The unmatched records keep their ids, so they get no entry.

### A split

One record becomes several, each with its own new id. Several `derived_from` entries sharing an input cover this:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| derived_from | ["w1"] | ["w1-a"] | subdivided for tiling |
| derived_from | ["w1"] | ["w1-b"] | subdivided for tiling |

A merge is the same shape with the repetition on the other side (several entries sharing an `output_id`). See the column-level provenance section above for a merge that also records which inputs supplied which columns.

### An enrichment

An operation fills in `height` from a reference dataset. Some records arrive with no height, and others arrive with one the operation overrides. The record ids stay the same, so these are `content_changed` entries, and `column_change` separates the two situations:

| kind | input_id | output_id | affected_output_columns | column_change | detail |
| -- | -- | -- | -- | -- | -- |
| content_changed | ["b1"] | ["b1"] | ["height"] | set | filled from lidar |
| content_changed | ["b2"] | ["b2"] | ["height"] | replaced | lidar overrode source-supplied height |

### A flag

A QA pass marks records with suspicious phone numbers and leaves their values alone:

| kind | input_id | output_id | affected_output_columns | detail |
| -- | -- | -- | -- | -- |
| flagged | ["p9"] | ["p9"] | ["phones"] | matches a known junk-number pattern |

The operation's `description` says what the check looks for. A `detail` is worth writing when the entry has something record-specific to add.

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