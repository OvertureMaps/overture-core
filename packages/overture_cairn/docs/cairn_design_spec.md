## Overview

### Terminology

* *"cairn"* - A pile of stones used to mark a path.
* *"Cairn"* - The provenance tracking system specified here.
* *"a Cairn"* - A pair of tables logging how a dataset was created

These two tables are:

1. An `operations` table that tracks the dataset-level transformations performed by a specific instance of a process.
2. A `row_detail` table that includes, for each entry in the `operations` table (optionally),  row-level information about the transformation.

If your pipeline has multiple Cairns, you should be able to collect them by concatenating their respective tables.

### Scope

Cairn records what happened to input records, how output records were made, and
which steps built a dataset. The retained data, code, and job settings can help
fill in the story, but complete reconstruction is not guaranteed.

`core` defines the schemas and their rules. Adapters capture and store facts
using their own engine, such as Spark or DuckDB. Reading traces and linking
bundles belong outside core. Concatenating Cairns collects their records; linking
a read to a write also requires knowing the exact data version.

## Table Schemas

### Operations

An operation is essentially *a directed relationship* that transforms one or multiple input datasets into a *single* output dataset. The operations table is a *catalog* of these relationships, but it does not contain information regarding *what* was mapped to what.

| Column | Description | Reason for inclusion | Example + type |
| -- | -- | -- | -- |
| `op_id` | The ID of this operation; distinct across runs and bundles. I suggest a composite `{run_id}.{slug(op_key)}`, the definitions for these columns follow.| The operation identifier that we use to build the lineage graph. | `20260903XXXX.reduce_precision (str)` |
| `run_id` | The unique ID of the recording execution. Separate attempts must not mix their records under one ID. | Keeps operations distinct when Cairns are collected across bundles and runs. | `20260903XXXX (str)` |
| `op_key` | This is a (descriptive) slug of the operation name. | Gives us a pseudo-stable identifier that can be tracked across code versions | `reduce_precision (str)` |
| `code_ref` | The fully-qualified module and function where this operation's logic lives. | Unlike `op_key`, this field is automatically filled and is not guaranteed to be persisted across refactors. | `"overture_base.base_land.promote_names" (str)` |
| `code_version` | The commit (or other revision id) of the code that was run for this operation. | Automatically filled; pinpoints the exact code that was run when paired with `code_ref`. | `"a1b2c3d" (str)` |
| `output_key_columns` | The columns that the output of the operation is keyed on. | Necessary for knowing how to trace the lineage graph at the row-level. | `["gers_id", "provider"] (array[str])` |
| `description` | Plain-text description of *what* this operation did. | Makes the lineage graph human-interpretable and establishes a way to understand our pipelines without reading code. | `"Trailing/leading whitespace is removed from names." (str)` |
| `identity_capture_status` | `partial` or `complete`, defaulting to `partial`. Complete capture accounts for this operation's input fates and output origins through entries or the declared pass-through rule. | Tells readers when an absent entry supports a conclusion. It does not promise complete column detail or complete upstream history. | `"complete" (str)` |
| `physical_source` | What physical location this operation reads from, if any. If the operation reads from multiple physical locations, it should be split into smaller operations. | This lets the bundle-entrypoint operations specify what sources they draw information from. | `"s3://overture-stuff/data.json" (str)` |
| `physical_dest` | What physical location this operation writes to, if any. If an operation writes to multiple locations, it should be split into multiple operations. | This lets bundle-exit point operations specify what they end up materializing. | `"s3://overture-stuff/output.parquet" (str)` |
| `input_op_ids` | The IDs of the operations whose outputs this operation consumes. | Intermediate datasets need not be stored, so operations link directly to operations. These links form the dataset-level graph; row detail describes record-level links. | `["20260903XXXX.reduce_precision", "20260903XXXX.simplify_geometry"] (array[str])` |
| `passthrough_input_op_ids` | Inputs whose records survive under the same identity except for recorded outcomes. Defaults to an empty list. Readers may infer survival from absence only with `identity_capture_status=complete`. | Identifies which inputs supply the continuing record stream. Matching key columns cannot distinguish a base table from reference data. An empty list permits no implicit survival links. | `["20260903XXXX.read_feed"] (array[str])` |
| `timestamp` | When this operation was logged. | This column does not necessarily indicate when the operation *happened* (in some cases this may be an unanswerable question) -- it just captures when the operation record was added to Cairn. | `2026-09-07 13:01:00 (timestamp)` |

#### Per-Cairn Operations Table Validations
* If `physical_source` is not null and `physical_dest` is null, `input_op_ids` must be empty. A read draws from a location, so it consumes no operation.
* If `physical_dest` is not null, `input_op_ids` has exactly one entry. This covers writes and copies, which both put one operation's output somewhere.
* If an operation's `physical_dest` is not null, no other operation is allowed to reference it in its `input_op_ids`.
* The directed graph formed by joining `op_id`s by `input_op_ids` must be acyclic.
* Every element of `input_op_ids` is present in the aggregate `op_id`s of the Cairn.
* Each `op_id` is unique.
* `identity_capture_status` is non-null and is either `partial` or `complete`.
* If `physical_source` or `physical_dest` is not null, the operation has no row detail. Reads, writes, and copies move records without touching their contents.
* If `output_key_columns` is not null, it names at least one column, no column twice, and nothing blank. Row detail ids are read positionally against it, so a repeated name would make an id ambiguous.
* `run_id` is unique across every Cairn that will ever be collected together, not only within a theme. `op_id` uniqueness rests entirely on this.
* Every element of `passthrough_input_op_ids` appears in `input_op_ids`, and none appears twice.
* If `physical_dest` is not null, `passthrough_input_op_ids` equals `input_op_ids`. Writes and copies preserve their input records and repeat that input's `output_key_columns`.
* If `physical_source` is not null and `physical_dest` is null, `passthrough_input_op_ids` is empty. A read consumes no operation, so it has no input to pass records through from.

Given these requirements, the "type" of operation is actually easily inferrable from the operation's parameters:
* "Read" operations have non-null `physical_source` and null `physical_dest`.
* "Write" operations have non-null `physical_dest` and null `physical_source`.
* "Copy" operations have non-null `physical_source` AND non-null `physical_dest`.
* All other operations (transforms, joins, filters, etc.) have null `physical_source` and `physical_dest`.

These rules also enforce a level of tracking detail: if a job reads from multiple locations, it must be split into multiple "Read" operations. These "Read" operations can then be combined via additional operation(s) with `len(input_op_ids)>1`.

#### Complete and partial capture

`complete` covers record relationships for this operation, including merges,
splits, drops, and new identities. `partial` includes no capture and capture of
only some effects. Known entries remain useful in either case; describe missing
coverage in `description`.

An empty detail table can be complete. A filter that rejects zero records has
nothing to write. Helpers for known operations can fill in the status; a generic
transform defaults to partial. Reads, writes, and copies can be complete without
row entries. A read's account starts at its source, so "complete" does not claim
to explain how that source was made.

**simplification:** The status covers the whole operation. If one input is only
partly recorded, the operation is partial. Add per-input coverage only if actual
usage needs it.

Schema checks cannot prove that all real effects were recorded. Adapters must
not claim complete record capture when duplicate or unknown keys make individual
records impossible to distinguish. Composite keys can identify records after a
join or explode.

For older recordings without `identity_capture_status`, readers use `partial`.
The old `has_row_detail` field does not establish completeness.

### Row Detail
| Column | Description | Reason for inclusion | Example + type |
| -- | -- | -- | -- |
| `op_id` | Which operation was run. | This gives us a foreign key into the operations table. | `20260903XXXX.reduce_precision (str)` |
| `input_op_id` | If an operation had multiple input ops, which input operation supplied the `input_id`?. This stays null for a sole input and for a mint. | The same ID values can occur in different inputs, so we need a way to distinguish which operation this record refers to. | `20260903XXXX.simplify_geometry (str)` |
| `kind` | The kind of relationship: `dropped`, `minted`, `derived_from`, `content_changed`, or `flagged`. | Justification below. | `"minted" (str)` |
| `input_id` | The input record ID. | - | `["42", "tomtom"] (array[str])` |
| `output_id` | The output record ID. | - | `["1337", "meta"] (array[str])` |
| `affected_output_columns` | This array asks: which output columns does this entry concern? Null means no column-level detail was recorded. | If `kind=derived_from`, these are the columns the input supplied or helped compute. If `kind=content_changed`, these are the changed columns. If `kind=flagged`, these are the columns the finding concerns. | `["height", "name"] (array[str])` |
| `column_change` | For a `content_changed` entry: `set` (filled from empty), `replaced` (changed a present value), or `cleared` (made empty). Null means "we don't know". For implementers: columns with the same fate should occupy the same record. | Describes the before/after change to a surviving record separately from which input supplied the value. | `"set" (str)` |
| `detail` | How this record was changed. | The "why" lives in the operations table, the "how" lives here. | - |

#### Justification for `kind`
`kind` describes the fact an entry records:

| Kind | Fact |
| -- | -- |
| `minted` | An identity was created without an input identity. An unknown source is not a mint. |
| `dropped` | This input record did not survive under the same identity. |
| `derived_from` | An input record supplied or helped compute an output record. The input-output IDs may match. |
| `content_changed` | A continuing record's values changed. The input-output IDs match. |
| `flagged` | A finding about a surviving record. The input-output IDs match. |

The following pseudocode  may provide some clarity (though do not take it as a definition):

```python
def determine_kind():
    if not has_input_id and not has_output_id:
        raise ValueError("An entry needs an input or output identity.")
    if not has_input_id:
        return "minted"
    if not has_output_id:
        return "dropped"
    if input_id != output_id or recording_contribution:
        return "derived_from"
    if values_changed:
        return "content_changed"
    if recording_finding:
        return "flagged"
    return None
```

`recording_contribution` is true when the caller records where an output came
from, including a same-ID merge contributor. That branch does not require an
extra content-change entry. `None` means there is no fact to record, rather than
that an unchanged record should be flagged. Missing endpoints describe the known
fact; a before/after diff alone cannot distinguish a rebind from a drop and mint.

`column_change` describes what happened to a cell on a continuing record. It
does not name the source column; any source-column explanation belongs in
`detail` or the operation's code.

#### Column-level provenance on merges

Consider a small merge whose inputs each contain one record, A and B. Its output
keeps A's ID and has four columns: `id`, `name`, `geometry`, and `websites`.
A supplies the first three; B supplies the website. All datasets are keyed by
`["id"]`.

| op_key | input_op_ids | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- |
| read_a | [] | [] | complete |
| read_b | [] | [] | complete |
| merge | [read_a, read_b] | [] | complete |

`read_a` and `read_b` are reads of the two source locations. The merge records
all contributions explicitly:

| kind | input_op_id | input_id | output_id | affected_output_columns |
| -- | -- | -- | -- | -- |
| derived_from | read_a | ["A"] | ["A"] | ["id", "name", "geometry"] |
| derived_from | read_b | ["B"] | ["A"] | ["websites"] |

The website entry answers where the value came from. No third row is required
because the output retained A's ID. A separate `content_changed` entry is optional
if the caller also wants to record whether the website filled a blank or replaced
a value. `column_change` stays null on the contribution entries.

If we know only that A and B contributed, the same merge can be recorded without
column detail:

| kind | input_op_id | input_id | output_id | affected_output_columns |
| -- | -- | -- | -- | -- |
| derived_from | read_a | ["A"] | ["A"] | null |
| derived_from | read_b | ["B"] | ["A"] | null |

Both record links are known, so `identity_capture_status` can still be `complete`.
Neither entry says which fields its input supplied. A reader can trace the
contributors, but cannot use these entries to choose the website's source.

#### Per-Cairn Row Detail Table Validations
* Every operation referenced in `row_detail` (via `op_id`) must have null `physical_source` and `physical_dest`.
* Every `op_id` and every non-null `input_op_id` must exist in the Operations table.
* Except for `minted`, `input_op_id` is non-null if and only if the operation has more than one input.
* A non-null `input_op_id` must be one of the operation's `input_op_ids`.
* If `kind=minted`, `input_id` and `input_op_id` must be null, and `output_id` must be non-null.
* If `kind=dropped`, `input_id` must be non-null and `output_id` must be null.
* If `kind=content_changed` or `kind=flagged`, both IDs must be non-null and equal.
* If `kind=derived_from`, both IDs must be non-null. They may be equal or different.
* `column_change` must be null unless `kind=content_changed`.
* If `kind=dropped` or `kind=minted`, `affected_output_columns` must be null.
* A non-null `affected_output_columns` lists at least one column. Use null when column detail was not recorded; an empty list is invalid.
* If `kind=content_changed`, at least one of `affected_output_columns` and `detail` is non-null. An entry with neither says something changed without saying what. A `flagged` entry needs neither, since the operation it belongs to is the finding.
* A non-null `output_id` requires non-null `output_key_columns` on its operation and has the same number of parts.
* A non-null `input_id` requires non-null `output_key_columns` on its input operation and has the same number of parts. Use `input_op_id` when present and the operation's sole input otherwise.
* If the operation's `input_op_ids` is empty, `input_id` must be null. An operation consuming nothing has no input records, so an id on the way in would name a row from no dataset. Such an operation can still write `minted` entries.

#### Grouped outcomes

Within each operation, group entries by input operation and `input_id`, resolving
the sole input when `input_op_id` is null. Mints have no input to group.

When a record has `derived_from` entries, they name its output links. Do not add
an implicit same-ID link. Complete capture requires every link, including a
retained identity: a split that keeps A and creates B records `A -> A` and
`A -> B`. A changed ID alone does not require an extra drop entry.

Content changes and flags describe surviving records but do not replace a
split's contribution links. Entries sharing endpoints describe one record link,
not extra contributors.

Reject a drop combined with a same-ID derivation, content change, or flag for the
same input record in the same operation. A drop can coexist with contributions
under other IDs. These rules require looking at related rows together; checking
each entry alone is not enough. Core defines the rules, and adapters apply them
to their tables.

#### When to name columns

Name columns where the source choice would otherwise be lost, such as selecting
a website from competing providers. For `derived_from`, name the columns
supplied; for `content_changed`, the columns changed; for `flagged`, the columns
the finding concerns. A single contributor can supply only part of a record, so
input count alone does not tell us which columns it supplied.

Null means no column-level detail was recorded. For a contribution, it says
"this record contributed, but we did not record which fields." For a content
change or flag, it describes the record without naming particular fields.
A content change with no column list still needs `detail` to say what changed.

To claim that an entry concerns all columns, list them. Null is not a wildcard
or a claim about columns left unnamed by other entries. Named and null lists may
appear in the same operation: some facts can have column detail while others do
not. Several inputs may name the same column when its value was computed from
all of them.

There is no column-completeness field. Report known sources without assuming
they are the only ones. Same-ID survival alone does not prove that a field was
unchanged. Omitting column detail does not make a complete record-level account
partial. Rules for tracing unrecorded fields through enrichments and known
filters remain open; `identity_capture_status` does not settle them.

#### What an absent entry means

Row detail entries are deltas. For a record known to exist in an input, first
read its explicit outcomes. If there are none, apply this table:

| identity_capture_status | Input declared pass-through? | Meaning of no entry |
| -- | -- | -- |
| complete | Yes | The record survived under the same identity. |
| complete | No | The record made no direct contribution to the output. |
| partial | Either | The record's fate is unknown. |

Known links remain useful during partial capture, but other links may be missing.
Follow the known branches and show the gap. A reference record's donor entry does
not count as an outcome for a separate base record.

"Same identity" does not mean unchanged values. An operation can rewrite values
by a uniform rule without writing an entry per record. Stripping whitespace from
names preserves identity even where the names change.

Which inputs qualify, by shape of operation:

| Operation | Inputs eligible for implicit pass-through |
| -- | -- |
| Identity-preserving filter or normalization | Its one input |
| Base-table enrichment | The base input only |
| Feed matched against a corpus | The feed input only |
| Union with a valid output key | Both inputs |
| Aggregate, or a fully explicit derivation | None |

An empty pass-through list permits no implicit survival links. Explicit entries
still record contributions. The no-contribution conclusion for complete capture
is local to this operation; it does not claim that a record had no influence on
a selection rule.

Matching `output_key_columns` is a compatibility check and never a substitute for the declaration. A declaration naming an input keyed differently from the output is worth questioning, but two tables keyed by `id` routinely play different roles in one join, and a rename that leaves identity alone or a group-by on the same column both make key comparison misleading.

#### Reading backward

Start with a known output and follow explicit incoming links. Also consider
possible pass-through links; a recorded donor is not proof that no other input
contributed.

With complete capture, if an output has no explicit origin and only one possible
pass-through input, it must have come from that input. A complete single-input
filter therefore needs no stored intermediate data to trace a survivor backward.

An explicit mint or derivation can already explain the output, so it does not
prove that another same-ID input existed. When several inputs remain possible,
such as a union, use known operation behavior, an earlier trace, or retained
input data to establish membership. Apply each candidate's forward rule,
including any drop or rebind. Leave a link unknown if the evidence does not
settle it.

Readers need not store an inventory of every input ID in Cairn. They may use the
retained data. Reads, writes, and copies preserve contents, so column links can
cross an exact versioned handoff without row entries.

### Cairn does not support column-level tracking alone

Cairn does not track columnar lineage in the Operations table. To explain why, consider the hypothetical case where places datasets A and B are merged and the phone number is selected per-record by whichever record has the higher confidence. In this scenario, column tracking at the row level is clear: there would be an entry for each record indicating *which* phone number was "passed through". But at the dataset-level, the only capturable information is the *logic* which was used to select the phone number.

A structured, dataset-level record of *which* columns fed which would have to describe arbitrary selection logic: confidence comparisons, priority orders, fallback chains, and so on. That logic does not reliably fall into a small, closed schema, and attempting to track it would probably be more trouble than it's worth. Instead, each operation holds a plaintext `description` and a pointer to its associated code.

## Examples

These show how common pipeline shapes land in the two tables. The `run_id` prefix on op ids is omitted for brevity, and each table only shows the columns that matter for the example.

### A filter

Almost every job has this skeleton, where a read feeds a transform which feeds a write. The transform here is a validity filter.

Operations:

All three operations are keyed by `["id"]`.

| op_key | physical_source | physical_dest | input_op_ids | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- | -- | -- |
| read_land | s3://.../land/ | null | [] | [] | complete |
| drop_invalid_geometry | null | null | [read_land] | [read_land] | complete |
| write_land | null | s3://.../land_clean/ | [drop_invalid_geometry] | [drop_invalid_geometry] | complete |

Row detail for `drop_invalid_geometry`:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| dropped | ["osm/w123"] | null | invalid geometry |

Only the removed rows get entries. Because capture is complete and `read_land`
is declared pass-through, a known input with no entry survived under the same
identity. The read and write have no detail of their own.

If the filter rejects nothing, its empty detail table is still complete. If an
adapter records only some of its rejects, use `identity_capture_status=partial`
and explain the limit in `description`. The recorded drops remain useful, but
absence no longer says that a record survived.

Reading backward, a known output of this complete filter must have come from
`read_land`. There is only one input and the filter creates no records, so
the reader does not need to reload the input to establish that link.

### Minting ids

A transform carves records out of unkeyed data (say, land polygons cut from a coastline) and gives each one a fresh id:

| op_key | input_op_ids | output_key_columns | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- | -- |
| mint_land | [read_coastline] | ["id"] | [] | complete |

Every created identity gets an entry; one is shown here:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| minted | null | ["550e8400..."] | new land feature derived from coastline |

Mints come from transforms. A read whose records already carry ids records nothing here, since those identities existed before Cairn saw them. An operation that hands new ids to records which already had one (a provider key, say) records `derived_from` entries, which the next example covers.

### Changing ids

An ID-assignment step consumes records whose matches have already been computed.
Matched records take their assigned final ID; unmatched records keep their own.
Both input and output are keyed by `["id"]`.

| op_key | input_op_ids | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- |
| assign_ids | [matched_records] | [matched_records] | complete |

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| derived_from | ["tmp-8f3e"] | ["gers-04c2"] | matched existing building, IoU 0.87 |

One entry per changed ID. The unmatched records need no entry because the input
is declared pass-through and capture is complete. The earlier matching step is
responsible for recording any corpus contributions; this example only assigns
the IDs from that result.

### A split

One record becomes several, and one child keeps the input ID. Both input and
output are keyed by `["id"]`.

| op_key | input_op_ids | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- |
| subdivide | [land_records] | [land_records] | complete |

Records that were not subdivided pass through. A subdivided record lists every
child, including the one that kept its ID:

| kind | input_id | output_id | detail |
| -- | -- | -- | -- |
| derived_from | ["w1"] | ["w1"] | retained id on one tile |
| derived_from | ["w1"] | ["w1-b"] | subdivided for tiling |

The first entry matters. Recording only `w1 -> w1-b` would not let a reader infer
that `w1` also survived. A merge repeats the output side instead; the earlier
merge example shows this while also recording which input supplied each field.

### An enrichment

An operation fills `height` from a reference dataset. The base records are keyed
by `["id"]`, the reference records by `["ref_id"]`, and the output by `["id"]`.

| op_key | input_op_ids | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- |
| fill_height | [base, heights] | [base] | complete |

Record every chosen donor. Base records continue under their own IDs, while
unused reference records do not become output:

| kind | input_op_id | input_id | output_id | affected_output_columns |
| -- | -- | -- | -- | -- |
| derived_from | heights | ["lidar-1"] | ["b1"] | ["height"] |
| derived_from | heights | ["lidar-2"] | ["b2"] | ["height"] |

These entries are enough to say where the heights came from. If the caller also
wants to distinguish filling a blank from replacing a value, it can add:

| kind | input_op_id | input_id | output_id | affected_output_columns | column_change |
| -- | -- | -- | -- | -- | -- |
| content_changed | base | ["b1"] | ["b1"] | ["height"] | set |
| content_changed | base | ["b2"] | ["b2"] | ["height"] | replaced |

Those rows are optional. They describe the base records' value changes, not
additional height donors. Complete identity capture does not claim that the
unlisted fields' column sources were recorded.

### A flag

A QA pass marks records with suspicious phone numbers and leaves their values alone:

| op_key | input_op_ids | output_key_columns | passthrough_input_op_ids | identity_capture_status |
| -- | -- | -- | -- | -- |
| flag_phones | [places] | ["id"] | [places] | complete |

| kind | input_id | output_id | affected_output_columns | detail |
| -- | -- | -- | -- | -- |
| flagged | ["p9"] | ["p9"] | ["phones"] | matches a known junk-number pattern |

The operation's `description` says what the check looks for. A `detail` is worth writing when the entry has something record-specific to add.

## Adapter rules

Data and lineage must describe the same decisions. If a step gives A a random
ID X, record A to X from that result; do not generate another ID while recording
lineage. Reuse the chosen IDs, donors, and rejection decisions. Stable inputs
and repeatable logic can be evaluated again; changing decisions must be shared.

How to do that belongs in each adapter. Core contains no Spark, DuckDB, or other
engine-specific execution logic. Helpers should fill in fields that follow from
known behavior, while manual recording remains available.

A before/after comparison needs known limits. For a filter, missing IDs are
drops. For arbitrary code, an old ID disappearing and a new one appearing does
not prove a drop and mint; they may be a rebind or merge. Record only what the
comparison establishes and leave identity capture partial when links are missing.

Use the pipeline's existing way of publishing data. Link a finished Cairn only
after its tables are written, and keep attempts separate. If data is published
after a lineage failure, bundle metadata must show that lineage is missing or
partial. A failed read is an error, not an empty detail table. These storage
rules belong to adapters and bundle integration, not core.

An exact data version, such as a bundle output or a database snapshot, lets a
reader link a write to a later read. A matching path alone is not enough if its
contents changed. Unresolved handoffs stay visible as gaps. Job settings and
model references can live in existing bundle metadata; credentials do not
belong in Cairn.

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