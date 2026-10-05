# Proposal: how to read and record a Cairn

Status: adopted for the core contract, 2026-09-13. The
[design spec](./cairn_design_spec.md) and
[recording guide](./cairn_recording_guide.md) define the agreed rules.
This document keeps the reasoning and the remaining work.

The core implements the capture status, contribution rules, null column meaning,
and grouped checks. Small reading examples live in the tests, outside core.
Engine adapters and versioned bundle linking are not implemented. Column
carry-over is deliberately not recorded; see Settled since adoption.

## Goal and scope

The [motivation document](./cairn_motivation.md) describes Cairn as markers that
show how data was made. The goals are to answer three questions:

1. What happened to this input record?
2. How was this output record made, including where its fields came from?
3. What steps built this dataset?

Keep the two tables and record only the detail needed to answer these questions.
Start with the parts of a pipeline that are easy to describe, and add detail over
time. Cairn does not need every pipeline to use the same engine or helpers.

Keep the responsibilities separate:

| Part | Responsibility |
| --- | --- |
| `core` | Table schemas and rules for checking them. No database execution, file access, Spark logic, or bundle linking. |
| Adapters | Capture facts and read or write tables using their own engine, such as Spark or DuckDB. |
| Bundle integration | Attach a Cairn to the data it describes and link exact input and output versions across bundles. |
| Reader | Follow recorded links and report where information is missing. Keep this separate from `core`. |

Cairn, the retained data, and the code may together explain or reconstruct much
of a pipeline's work. That is useful, but not guaranteed. The tables alone do not
need to store every prior value or every fact needed to rerun a job.

## 1. Say whether the record is complete

### Replace `has_row_detail` with useful capture information

Remove `has_row_detail` from the proposed schema. Whether entries exist is
available from `row_detail`; the flag does not tell a reader whether missing
entries mean anything. Do not add another stored count or flag to replace it.
A storage layer can use its own table metadata to skip empty data.

Add one field to `operations`: `identity_capture_status`, taking `partial` or
`complete` and defaulting to `partial`.

`complete` means all input fates and output origins are covered by entries or
the declared pass-through rule. This includes merges, splits, drops, and new
identities, not just changed ID strings.

Do not add `column_capture`. A merge can record all contributing records
without recording which one supplied each field. Column entries state known
facts, but identity completeness does not prove that all column sources were
recorded. See Settled since adoption for why no column-completeness field is
coming.

`partial` includes no capture and capture of only some effects. Explain the limit
in `description`. An empty detail table can still be complete: a filter that
rejects zero records has nothing to write.

A helper for a known operation can set this field itself. A successful write
that preserves the data needs no row entries to be complete. A generic transform
must not claim completeness just because it has one input or no entries.

**simplification:** The field covers the whole operation. If one input is only
partly recorded, the operation is partial. Add per-input coverage only if real
usage needs it.

### Reading forward

Start with a record known to exist in an input. Its identity includes which
input it belongs to, not just its ID values. Two inputs may both contain `42`.

Read all entries for that input and record together. Follow the recorded links.
If capture is partial, show those known links and say that others may be missing.

When there is no recorded outcome, a reader may conclude that the record survived
under the same identity only if both conditions hold:

1. The input is in `passthrough_input_op_ids`.
2. The operation has `identity_capture_status=complete`.

For a complete operation, no entry from an input outside the pass-through list
means that record made no direct contribution to the output. For a partial
operation, it means the answer is unknown.

Survival does not mean unchanged values. Stripping whitespace from names can
preserve every identity without writing a content-change entry for every row.

### What an explicit outcome means

Within each operation, read entries as a group for `(input_op_id, input_id)`,
resolving the sole input when `input_op_id` is null.

When a record has `derived_from` entries, those entries name its output links.
Do not add a same-ID link just because its input is declared pass-through. To
claim complete capture, the writer must list every output link, including one
that retains the input ID. For example, a split that keeps A and creates B
records both `A -> A` and `A -> B`.

`content_changed` records a same-ID survival and a value change. `flagged`
records an assertion on a surviving record. Neither replaces a split's link
list. Entries with the same endpoints describe the same record link, not extra
contributors.

If a survival fact accompanies contribution links, complete capture needs the
same-ID contribution too. Reject a complete recording that omits it. Partial
recordings keep the known survival while reporting the missing coverage.

A `dropped` entry rules out survival of that input record under the same
identity. It can coexist with contributions to outputs under other IDs. Reject
a drop combined with a same-ID derivation, content change, or flag for the same
input record in the same operation.

Partial capture still permits missing links. Follow known links and report the
gap; do not silently fill it with pass-through. A reference record's donor entry
does not count as an outcome for a separate base record.

### Reading backward

Start with a known output record and follow explicit incoming links. Also
consider possible pass-through links; an explicit donor is not proof that no
other input contributed.

For a complete operation, if an output has no explicit origin and only one
possible pass-through input, that input must have contained the record. No data
read is needed. This lets a reader trace a survivor backward through complete
single-input filters even when their intermediate results were never stored.

That shortcut does not apply when an explicit mint or derivation already
explains the output, or when more than one pass-through input remains possible.
An output with ID `42` does not prove that both inputs to a union contained `42`.
Use known operation behavior, an earlier part of the trace, or retained input
data to settle which records existed. For each candidate, apply the forward
rule, including any recorded drop or rebind. Leave links unknown where the
evidence does not settle the answer.

Do not require every Cairn to store a copy of all input IDs. Readers are allowed
to use the data alongside Cairn.

### Checks and older recordings

Core checks field values, input references, ID shapes, and the conflicting
outcomes listed above. Checking conflicts requires grouping entries by input
record, not just checking each row on its own. Core defines that rule; adapters
apply it to their tables. Key definitions are required for entries that carry
IDs, regardless of completeness. Reads, writes, and copies still have no row
detail; validate that directly.

Core cannot prove that an adapter recorded every real effect. Adapters need small
examples with known answers. They must not claim complete record capture when
duplicate keys make individual records impossible to distinguish. Use composite
keys where needed, including for rows created by an explode.

When a reader encounters an older Cairn without `identity_capture_status`, treat
it as `partial`. Ignore `has_row_detail` when deciding completeness.

## 2. Adapters must record the results actually used

The rule is engine-independent:

> Data and lineage must describe the same decisions. Do not rerun a changing
> calculation separately to produce its lineage.

For example, if a step gives record A a random ID X, record the mapping A to X
from that result. Do not generate another random ID while writing the mapping.
Randomness is allowed; making the same choice twice is not a safe assumption.

The pipeline and adapter must share the chosen IDs, donor records, and rejection
decisions. Use existing intermediate results where possible. Preserve those
fields until the lineage has been recorded.

Repeating a calculation is fine when stable inputs and repeatable logic give the
same result. Spark laziness alone does not require a checkpoint. If a result must
be stored to keep both outputs in sync, that is an explicit pipeline or adapter
choice. Core neither detects these cases nor manages their execution.

This rule applies equally to Spark, DuckDB, and other adapters. Their execution
details belong in their own documentation. Do not add engine-specific state or
options to the schema.

### Finishing a write

Use the pipeline's existing way of publishing output. Attach a finished-Cairn
reference only after the referenced tables have been written. No new transaction
system is needed in core.

If the pipeline publishes data after a lineage failure, its bundle metadata must
show that lineage is missing or partial. No finished-Cairn reference means
unavailable, not complete. Readers must report failed reads rather than treating
unreadable data as an empty table.

Do not mix attempts. Each execution needs its own run identity, or the writer
must safely replace an unfinished attempt before publishing it.

A fully written Cairn can contain partial operations. Successful storage and
complete capture are different facts.

## 3. Make the rules easy for implementers to follow

Let callers submit facts directly. Add helpers only for common operations whose
behavior is known. The helpers should fill in fields that follow from their
behavior, so callers do not have to repeat the same declarations.

| Caller uses | Helper handles |
| --- | --- |
| A known filter | Record rejected IDs, declare the input as pass-through, and mark identity capture complete once all outcomes are accounted for. |
| An old/new ID mapping | Write the supplied links, including contributions that retain an ID. |
| A donor table | Write the supplied input-record and output-column links. Do not guess donors from matching values. |
| Arbitrary code | Record known facts and default to partial capture. |

For manual recording, the guide should ask callers to identify the inputs and
keys, declare pass-through inputs, supply known facts, and state whether those
facts are complete. A complete example should show the operation and its row
detail together.

A before/after comparison is useful, but its limits must be clear. For a known
filter, missing IDs are drops. For arbitrary code, an old ID disappearing and a
new one appearing could be a rebind or merge. Do not label them as a drop and a
mint without knowing what happened.

If a comparison shows that a value changed but not where it came from, record
the change without guessing a source. Likewise, record-only lineage remains
useful when field-level detail is too expensive.

Change pipeline code only where it discards a fact needed for a query. For
example, return the chosen donor ID from a merge rather than rewriting the whole
merge around Cairn.

## 4. Try the reading rules on small examples

Start with a small reader used by the existing tests, outside `core`. It needs
only in-memory operation and detail rows plus known input/output records.
Do not build a query service, public reader API, or database layer yet.

The reader should distinguish recorded facts, conclusions allowed by the rules,
and unknown links. Show which operation a gap belongs to. Keep following known
branches even when another branch has a gap.

### Row-detail rules the reader needs

Allow `derived_from` to name an input and output with equal IDs. It means "this
record contributed," not "this ID changed." Use it for merges and splits even
when the output keeps an input ID. Count contributing records, not input tables.

Keep `content_changed` for a before/after change to a surviving record. A donor
link is enough to record where a merged height came from. An extra content-change
entry is optional if the caller also wants to record whether it filled a blank
or replaced a value. Keeping an input ID does not require that extra entry.

Keep `column_change` on `content_changed`. If the output has no prior record,
do not invent one just to give its fields a before-state.

Keep a teaching sketch like this in the design spec, with a matching decision
tree in the recording guide. It helps choose a kind for one fact; the written
rules still define which facts must be recorded and which combinations are valid.

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

Here, `recording_contribution` means the caller is recording which input supplied
or helped compute an output. The IDs may match. Choose this branch for a merge
contribution even when values also changed; it does not ask for a second entry.
`None` means there is no fact to record, not that the record should be flagged.
An absent input ID means there was no input identity for this fact, not that its
source is unknown.

A column reader follows recorded sources. Same-ID survival alone does not prove
that a column was unchanged. A known change without a known source leaves a gap.
Known sources can be reported without claiming they are the only sources.

Reads, writes, and copies preserve contents by definition, so a reader may carry
column links across a known data version without per-row entries. Do not apply
this rule to arbitrary transforms.

### Column-list meanings

Use null for no column-level detail. The entry describes the record without
claiming which fields it concerns. A non-null list names the columns supplied,
changed, or flagged, according to the entry's kind. To claim all columns, list
them. An empty list is invalid.

Named and null lists can appear in one operation. A null contributor does not
claim fields named by another entry, or automatically supply the remaining ones.

| Example | Facts the recording must express |
| --- | --- |
| Record-only merge of A and B into C | Both records contributed. Leave both column lists null. The record links can still be complete. |
| Field-selection merge | A supplied names and geometry; B supplied websites. Each claim names those columns. |
| Height enrichment | B supplied height to A. Decide how unchanged names and geometry are traced without listing them for every row. |
| A column computed from several records | All actual contributors can name that column; multiple claims are not automatically a conflict. |
| A column set to a constant | The operation supplies the value. Do not invent an input record as its donor. |

Unknown column sources do not by themselves require
`identity_capture_status=partial`. That status covers record relationships.
A content change without a column list still needs `detail` to say what changed;
a flag can rely on its operation to describe the finding.

Unchanged fields through an enrichment or a filter have no recorded source, by
decision rather than omission. A reader answers that question from the
descriptions of the operations on the record's path. See Settled since adoption.

### Examples to cover

| Case | Expected answer |
| --- | --- |
| Complete filter with no rejects | A known input survived despite an empty detail table. |
| Complete single-input filter with no stored intermediate data | A known output with no explicit origin traces back to the sole input. |
| Partial filter | Known drops are visible; other fates are unknown. |
| Enrichment where both inputs use the same IDs | Base and reference records stay distinct. An unused reference does not become output. |
| Merge retaining an input ID | All contributors are traced, including the one whose ID survived. |
| Split retaining an input ID | The retained record and new children all appear. A flag on one child does not hide the others. |
| Recorded `A -> B` without `A -> A` | Do not infer that A also survived. A complete split must list the retained identity if there is one. |
| Drop and same-ID survival for one input record | Reject the conflicting claims, even if each entry passes its own checks. |
| Union with different records in each input | Backward reading does not invent a record in the other input. |
| Full record links with partial column detail | Contributors are known; a unique field source is not claimed without evidence. |
| Same-ID value change | The record survives, but changed values are not described as unchanged. |
| Write followed by a read | Link the exact data version or report a gap. |

Compare the answers with the known data and links, not just whether the rows
pass schema checks. These examples are a way to settle reading rules before
adapters depend on them.

## 5. Keep the promise useful and honest

Cairn records enough to follow how data was made where that information is
available. It also shows which steps are not fully recorded.

The data, code, and job settings can fill in details that do not belong in every
row entry. For example, a unit conversion can often be understood from its
description and code. A donor choice that was discarded from the output needs
to be recorded where it happens.

Do not promise that Cairn always reconstructs complete provenance, even alongside
the data. Sources may expire, old values may be lost, or an external step may
expose only some of its work. State the gap rather than inventing an explanation.

Record input datasets, including reference data, in the operation graph. Row
detail records contributors; it need not list every losing candidate that
influenced a choice. Descriptions explain the selection rule.

Use existing bundle metadata to link exact data versions, code, job settings,
and model files. Do not copy settings stores or credentials into Cairn.
Linking across bundles stays outside core, as the motivation document describes.
Concatenating tables collects the records; matching paths alone does not prove
that a read used a particular write.

## Remaining work

1. Try one pipeline section containing a filter, enrichment, merge, and a stored
   result. Use it to answer the three questions at the start of this document.
2. Measure the extra work and remaining gaps before adding more helpers.

## Settled since adoption

**Tracing unrecorded fields, 2026-09-14.** Cairn does not record column
carry-over. A field no entry mentions has no recorded source, and whether it was
touched is answered by reading the descriptions of the operations on the record's
path.

The option considered was a per-operation list of the columns an operation may
have written, letting a reader infer that any column named nowhere carried over.
A Spark adapter could have filled most of that in from analysis-time schemas at
no cost, though schema availability is a property of dataframe engines rather
than something core can require. It was rejected on three grounds: the
declaration lands on every operation, nothing can verify it, and a forgotten
entry yields a confident wrong answer instead of a gap. Reading a dozen
descriptions is a worse interface than a query and a better one than a query that
lies.

Two consequences worth keeping in view. A rename at ingest, where a provider
field becomes an Overture field, is a column-level derivation the model does not
express, so provider normalization is described rather than traced. And nested
paths never needed a naming convention, since no field names columns outside row
detail.

**Defaulting pass-through, 2026-09-14.** A transform with one input keyed the way
its output is keyed now defaults to that input passing through. It resolves when
the operation is recorded, so the stored list stays explicit and no reader infers
anything. An empty list remains available for a group-by keyed on the column it
grouped, which key columns alone cannot distinguish from a filter. Multi-input
operations still declare, since two inputs keyed alike routinely play different
roles.
