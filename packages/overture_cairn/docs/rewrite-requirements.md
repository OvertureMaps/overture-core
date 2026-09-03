# Cairn rewrite — requirements

What the rewrite must do, and the limits it has to work within. It stays mostly
at the level of what and why, not how, though it records a few design decisions
we have already made. It does not pick APIs or table shapes. Written 2026-08-19
from the design conversation before the rewrite. See also `transform-shapes.md`,
a list of the real transform patterns across the theme pipelines.

## What cairn is for

Cairn keeps a record of what a PySpark pipeline does to its data. For any one
record, it can answer what happened to it and why. For any dataset, it can
answer how that dataset was built.

## The main idea

- Cairn is the shared format and storage that pipeline steps report into. It can
  watch the simple steps on its own, but that is a convenience. The real promise
  is this: if a step does something cairn can't see, the step tells cairn what it
  did.
- What cairn stores must be readable by a person and written by a person. Cairn
  must not try to work out what happened by reading Spark's internal query plans.

## The three questions cairn must answer

- **Q1** — given an output record, how was it made? Follow it back to its
  sources, through the steps that touched it, with the reasons.
- **Q2** — given an input record, where did it go? Was it dropped and why, merged
  into what, or changed how.
- **Q3** — how is a dataset built? The steps, in order, and what each is for.

## What it must do

### Two layers

1. **The steps** — a record of which steps ran and what each is for. Cheap. Runs
   no Spark job.
2. **The records** — a record of what happened to specific rows, each with a
   plain-language reason.

### The record-level facts it must be able to state

- A record was dropped, and why.
- Several records became one, and which ones.
- One record produced several others, and which.
- A record was kept but changed, and why.
- A record's id changed, from one id to another.
- A record got its first id, where none existed before. This is different from
  the fact above: a changed id has an old value to point back to, a minted id
  does not. Real code needs both — a matched building's id changes from a
  placeholder to a GERS id (a change), but the placeholder itself came from
  nowhere (a mint). Treating a mint as a change with a null "from" loses the
  distinction between "we don't know the old id" and "there was no old id."

### Ids

- A record's id may be one column or several columns together (for example
  `provider` and `id` in the corpus). Everywhere cairn uses an id, it must accept
  a group of columns, not just one.
- An id can change partway through a pipeline (a source key becomes a GERS id at
  match). Cairn must be able to record that change directly.
- Cairn does not guess id changes; the step reports them. Cairn must support two
  ways to report one: inline, where the old and new id are both in the row (the
  common case), and as a small old-to-new table, for when several records merge
  into one new id.
- The id change is also the point that links one bundle's records to the next, so
  it must be recorded in a way a later cross-bundle step can follow.

### Column-level detail

- An event must be able to say which column or columns it is about (for example:
  the height came from source A, the name came from source B; or these fields
  were blanked for reason X). An event about the whole record just names no
  column.
- One event may cover several columns when they share the same source and reason.
  Columns from different sources are separate facts and stay separate events.

### The transform patterns it must handle

Cairn must handle the range of PySpark transforms across the theme pipelines (see
`transform-shapes.md`). In short:

- plain one-in-one-out transforms (most steps; the step-level record is enough)
- drops: simple filters, anti-joins, and keep-the-best-per-group dedupes (the
  most common drop, in every theme)
- merges (many become one), including the kind where the per-field choice is made
  in Python that cairn can't see into
- id changes: taking an id from another table, building an id from the row's
  content, giving a merged record a fresh id, and id changes that happen in a
  later, separate job
- enrichment joins, UDFs (mostly plain one-in-one-out), and counts/aggregates
- black boxes: raw SQL (sometimes the whole pipeline), caller-supplied SQL text,
  and third-party functions that take a whole DataFrame and return one

### The three states a step can be captured in

Every step must end up in one of these, never left uncategorized:

- **The step reports what it did (preferred).** Used for merges, id changes, and
  drops where the reason matters.
- **Cairn compares before and after (the fallback).** For a black box, cairn
  looks at the input and the output and works out what left or changed. This
  gives "what", not "why". It also cannot handle merges or id changes (it sees old
  rows vanish and new ones appear with no way to link them), and it breaks when
  the id is no longer unique. It is easy to build but limited, which is why it is
  the fallback.
- **The step touches no ids at all.** A lot of real code is plumbing: checking a
  bucket exists, listing partitions, comparing a schema. These are not black
  boxes to shrink later; they are done, permanently, and should say so plainly
  rather than sit unlabeled or get force-fit into "reported" or "compared." The
  six pipeline traces turned out to be full of these once we went stage by
  stage, so the format needs a real slot for "nothing to report here" that
  reads as complete, not as a gap.
- The reporting way is where the real design effort goes; the other two exist
  so nothing falls through without a label.

### Moving from black boxes to clear steps

- A job can be part-reported and part-black-box. Cairn must allow that mix.
- The before/after way and the reporting way must produce the same kind of
  output. So turning a black box into clear steps just fills in more detail; it
  never changes the output format.
- Cairn should record how each step was captured (reported or compared), so the
  list of remaining black boxes can be looked up and shrinks over time.

## Limits it must work within

### Size

- The record-level data can be bigger than the source data. It must never be
  pulled back to the driver, and must never leave Spark. The only thing that may
  come back to the driver is the small result of a single-record lookup.
- A record with nothing to report is not written at all. An untouched record is
  implied by its absence, not stated as "no change." At building scale, writing
  an explicit row for every record a step didn't touch would mean roughly 2.5
  billion rows per step, for a fact that's already true by default. This also
  means an id's fate is always found the same way: look for a row that mentions
  it; if there isn't one, it passed through unchanged.

### Safety

- Cairn only watches. It must never change the data or make a job fail.
- Cairn must have a strict mode for development and testing that shows its own
  errors instead of hiding them.

### One bundle at a time

- Cairn tracks one bundle's work. Linking records across bundles is done
  separately, through the bundle's `metadata.json`, and is not cairn's job.
  Cairn's output must be safe to stack together across separate runs, which means
  ids that stay unique from one run to the next.

### No reading query plans

- What cairn stores comes from what people write down: reasons, what a step is
  for, which columns are the id. Not from taking apart Spark's query plans.
  Reading plans would give structure but never the reason, is blind to what UDFs
  do, and ties cairn to Spark's internals. Column-level detail comes from what
  steps report, not from plans.

### What it covers

- Cairn covers PySpark pipelines. The Scala themes (divisions, corpus) are out of
  reach and out of scope.

## What must make it easy to use

For people rewriting existing jobs:

- The common case, a plain transform, should take almost no effort.
- You name the id column(s) once, not at every call. An id change updates it.
- The calls are named after what they do in a pipeline, not after graph terms.
- Every record-level call gives back the DataFrame, so it slots into existing
  code without rearranging it.
- No setup code: starting and finishing a run is handled for you.
- The whole set of choices should be small enough to keep in your head.

## How it relates to the violation store

The violation store was a temporary way to surface some of this information and
report it onward (for example to OSM). The end goal is for cairn's output to
replace it, because cairn's record should be more complete.

- This rewrite's job is to make cairn's output complete enough to replace the
  violation store. Actually building the replacement, the thing that reads cairn's
  output and reports it onward, is a follow-on project, not this one. Cairn does
  not care how its output is later viewed.
- Cairn must not build adapters onto the existing violation tables. Steps that
  already write their own violation tables stay unread by cairn until they are
  reworked to report to cairn. This covers the buildings validation jobs, the
  places filter chain, `osm_checks`, and the `overture_quality` checks.
- Buildings currently depends on the violation store, and reworking it to be a
  normal pipeline that reports to cairn is part of the eventual goal. But it is
  not the first pipeline to tackle; see the approach note.

## Decisions already made

- **Do not use OpenLineage.** Its main use is linking jobs together, which
  `metadata.json` already does; it has no record-level idea; and its column
  tracking works by reading query plans, which is off the table.
- **Do not build a "declare your whole pipeline" framework yet.** Letting cairn
  work out the step order on its own is fine for now. A full declared-pipeline
  rewrite waits until it is clearly needed.
- **Keep the event format loose.** Plain reason text, with an optional fixed name
  for a check when you want to group or trend it. Do not force a strict check
  format.

## How we'll get there (approach)

- Build cairn's core against one real pipeline first, and let instrumenting that
  pipeline drive the API. The tracker and the pipeline work are not separable: you
  cannot instrument a pipeline before the API exists, and you cannot trust the API
  until it has met a real pipeline.
- Get that one pipeline working end to end and look at the output before applying
  cairn to any others.
- Before picking that pipeline, we surveyed all six themes for real (not just
  `overture_base`): `pipeline-trace-*.md` and `pipeline-mechanics-*.md` in this
  folder cover buildings, places, transportation, addresses, base, and
  divisions, backward from release publish to raw source, with code and
  file:line citations. That survey is the actual basis for picking a starting
  point now, not a guess from one pipeline's README.
- Rather than one whole pipeline, start with the handful of spots the survey
  found already doing cairn's job informally, in an incompatible one-off format:
  buildings' matcher already writes its rejected duplicate matches to their own
  filter table; a couple of the transportation segment-merge jobs already carry
  an old-id or merged-ids field for debugging; buildings' spatial merge writes
  its own conflate filter records. Converting an existing, working record of
  "what happened to this id" into cairn's format is cheaper than instrumenting
  a job that keeps no record at all, and it tests the schema against real id
  mechanics (mint, rebind, merge) immediately instead of after the fact.

## Hard cases the rewrite must answer

Patterns the earlier prototype did not handle cleanly. The rewrite must decide
what to do with each, even if the answer is "leave it unread for now".

1. **Per-field merge** — which source won which column. The step has to report
   this; cairn can't work it out by watching.
2. **Keep-but-blank** — a row that is kept but has some fields blanked is a
   change, not a drop. The format must be able to say so.
3. **Explode with no real split** — some `explode`s are not a record producing
   several new records; they are the same record temporarily copied across many
   rows for storage or computation (subdividing a polygon for tiling, fanning a
   corpus row out per provider). There is nothing meaningful to report, since
   nothing actually split, but the id is now on many rows at once, which breaks
   both by-id lookup and by-id compare. Telling this apart from a real split (see
   the record-level facts above) matters, and how to handle the broken id lookup
   in this case is still open.
4. **In-place table update (`MERGE INTO`)** — changes a table directly instead of
   passing a DataFrame through a step, so it doesn't fit the "a record flows
   through steps" picture.
5. **Id change in a later job** — the id change happens in a different job than
   the one that made the record (addresses leaves id assignment to a later match
   job). Only captured if that job also uses cairn and the cross-bundle link is
   in place.

## Out of scope

- Linking records across bundles (that is `metadata.json`'s job).
- Building adapters that read the existing violation tables into cairn.
- Building the replacement for the violation store: reading cairn's output and
  reporting it onward (a follow-on project).
- Getting column-level detail by reading Spark's query plans (it comes from what
  steps report instead).
- Scala pipelines.

A "declare your whole pipeline" framework is not out of scope forever, only
deferred; see Decisions already made.
