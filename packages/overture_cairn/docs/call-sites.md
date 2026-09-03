# What calling cairn looks like

Sketches of the Spark adapter's call sites, drawn against real jobs. None of this
is implemented; the point is to see whether the design reads well in code we
already have before writing any of it. Cited line numbers are current as of
2026-08-25.

The whole surface is eleven names:

| | |
| --- | --- |
| `start_run(spark, run_id, out)` | opens a run, writes both tables on exit |
| `run.declarative_op(name, reason)` | the operation says what it did |
| `run.comparative_op(name, reason)` | something works it out from the ends |
| `run.not_applicable_op(name, reason)` | the operation handles no records |
| `op.drop(df)` | records left |
| `op.mint(df)` | records got a first id |
| `op.derived_from(df)` | records came from other records |
| `op.content_change(df)` | records kept their id, values moved |
| `op.flag(df)` | records were found wanting and kept anyway |
| `op.keep_where(df, cond)` | sugar: keep the matching rows, record the rest as drops |
| `op.compare_ends(before, after)` | sugar: work the drops out by comparison |

`run.*_op` returns an operation handle, and each is named for the schema value it
sets: the first two set `recording_method`, the third sets `records_captured`. The
five edge methods are spelled exactly like the `kind` they write, so nothing needs
translating between a call site and the table.

`op.*` takes a DataFrame of ids and adds to the edge table, returning nothing. The
data never passes through cairn.

## The cheapest case

Plumbing. One line, no edges, and it reads as a finished statement. This one never
sees a record, which is what makes it `not_applicable_op`.

```python
run.not_applicable_op(
    "check output location",
    "fail early if the bundle path is not writable",
)
```

## The workhorse: a filter with a reason

`BuildingMatcher` drops footprints whose best overlap is too weak
(`building_matcher.py:143-150`). Today the threshold is a bare `where`, and the
reason lives only in the source.

```python
# before
match_df = match_df.where("iou > 0.5")

# after
match_df = run.declarative_op(
    "drop weak overlaps",
    "an overlap below 0.5 IOU is not the same building",
).keep_where(
    match_df,
    "iou > 0.5",
    id="id",
    detail=F.concat(F.lit("best IOU "), F.round("iou", 3)),
)
```

`keep_where` returns the surviving rows, so it drops into existing code without
rearranging it, and every dropped id gets an edge carrying its own number. The
explicit form is there when the shape does not fit:

```python
op = run.declarative_op("drop weak overlaps", "an overlap below 0.5 IOU is not the same building")
op.drop(match_df.where("iou <= 0.5"), id="id", detail=F.col("iou"))
match_df = match_df.where("iou > 0.5")
```

## Two inputs, and one of them is the id authority

The matcher reads the corpus and a feed, and they are different lineages. Reads
are declared separately even though the SQL fuses them, which costs nothing at
runtime and is what lets each edge say which side it came from.

```python
corpus = run.declarative_op(
    "read corpus",
    "prior release footprints, the source of GERS ids",
    parents=None,
    physical_source=[corpus_path],
)
feed = run.declarative_op(
    "read feed",
    f"this run's {source_name} ingest",
    parents=None,
    physical_source=[feed_path],
)

matched = run.declarative_op(
    "match by footprint overlap",
    "a feed building and a released building with high IOU are the same building",
    parents=[corpus, feed],
)
```

Then the rebind, which is the point of the whole job
(`building_matcher.py:197-228`, `id = COALESCE(mapping.release_id, feed.id)`):

```python
run.declarative_op(
    "rebind matched buildings to their release id",
    "a matched building keeps the id it was published under; an unmatched one keeps its own",
    parents=[matched],
).derived_from(
    v_mapping,
    input_id="id",              # the ingest-minted placeholder
    output_id="release_id",     # the GERS id from the corpus
    input_op=feed,
    detail=F.concat(F.lit("IOU "), F.round("iou", 3)),
)
```

Unmatched buildings get no edge, because nothing happened to their id. That is
the delta rule doing its job on the majority of rows.

## Column level, where it actually carries information

`BuildingTagMerge` picks a height from four candidates and already computes
`height_source` to say which one won (`building_tag_merge.py:176-195`). That
column is then concatenated into the `sources` array and disappears
(`:228-229`). The information exists and has nowhere to go.

```python
heights = run.declarative_op(
    "merge height from signal sources",
    "the first non-outlier height in source priority order wins",
    parents=[staged, esri, lidar, ms],
)

# height_source is empty when the input's own height won, so this is only the
# rows where another source supplied the value
won_elsewhere = df2.where(F.size("height_source") > 0).select(
    "id",
    F.col("height_source")[0]["record_id"].alias("donor_id"),
    F.col("height_source")[0]["dataset"].alias("donor_dataset"),
)

heights.derived_from(
    won_elsewhere,
    input_id="donor_id",
    output_id="id",
    columns=["height"],
    column_change="replaced",
    detail=F.concat(F.lit("height from "), "donor_dataset"),
)
```

One edge per building whose height came from somewhere else, naming the column
and the donor. Buildings whose own height won get nothing, which is both correct
and most of them.

This is the only place in the buildings pipeline where column-level detail is
worth writing. The nineteen `bc.with_*` calls at ingest
(`building_ingest.py:41-59`) each derive a column from the same source row, so
they are recoverable from the code and get no edges at all.

## A black box, opened partway

The Scala matcher reports its rejects and says nothing about its accepts. The
record can say exactly that.

```python
jar = run.declarative_op(
    "match same places",
    "the Scala matcher clusters places; only its rejects are visible from here",
    complete=False,
)
jar.drop(rejects_df, id="record_id", detail=F.col("reject_reason"))
```

Finishing the account later flips `complete=False` and changes nothing else.

For code nobody has opened at all, the comparison gets a floor under it without
touching the job:

```python
op = run.comparative_op("legacy sql block", "one raw SQL statement, not yet broken apart")
op.compare_ends(before_df, after_df, id="id")
```

That yields which ids left and never why, which is the honest limit of watching
from outside.

## Scratch stops being invisible

The matcher writes `matched_inter/` halfway through because the overlap join is
the slowest part of the job (`building_matcher.py:118-124`). Nothing today
records that this location exists.

```python
run.declarative_op(
    "materialise overlap pairs",
    "the IOU join is the slowest step, so it lands on disk once and is read back",
    physical_dest=[f"{base}/matched_inter/source={source_name}/"],
)
```

No edges, because the write changes nobody's id. It is still a `declarative_op`,
though, because the write handles every record it persists, and the line between
the two helpers is whether records went through at all. A bucket check gets
`not_applicable_op`. A write that happened to change nothing is a declaration with
nothing to declare.

A location written by one operation and read by another in the same run is
scratch. Read but never written is an input. Written but never read is an output.
Cairn records the paths and stays ignorant of which is which.

## What this replaces

Three places in buildings already record something edge-shaped, in three
incompatible formats, none of which can be queried together:

| today | what it is |
| --- | --- |
| `filter_df` with `filter_type="duplicate_match"` (`building_matcher.py:157-179`) | dropped ids with a reason, as its own parquet file |
| the conflate filter records in `BuildingSpatialMerge` | dropped ids with a reason, different columns |
| `height_source` folded into `sources` (`building_tag_merge.py:228-229`) | column-level donor, mixed into an array with everything else |

Each is a correct fact about a record that somebody bothered to compute. Cairn is
one destination for all three.

## Not decided yet

- Whether `parents` can be inferred from DataFrame identity. The adapter can see
  which tracked frames arrived, so most of these arguments could disappear.
  Worth trying once something runs.
- Whether `detail` accepts a Column expression everywhere or only a literal.
  The examples above assume expressions, which is the useful case and the more
  expensive one.
- How a decorated function supplies its name and reason when it runs in a loop
  over sources, since names have to stay unique within a run.
