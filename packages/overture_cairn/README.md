# overture-cairn

A record of what a data pipeline does to its data. For any one record, it can say
what happened to it and why. For any dataset, it can say how that dataset was
built.

A Cairn is two tables:

- **`operations`**, one row per operation, where an operation maps one or more
  input datasets to a single output. A run has hundreds of these at most.
- **`row_detail`**, one row per fact about one record's identity. This is the
  record-level layer, and it can be larger than the data itself.

Row detail records selected facts rather than every record at every step.
When `identity_capture_status` is `complete`, a known input record with no entry
survived under the same ID if its input is listed in `passthrough_input_op_ids`.
For other inputs, no entry means no direct contribution to the output. With
`partial` capture, absence means unknown. Surviving records can have changed values.

Cairns live inside the bundle they describe. Collecting the full lineage chain
means following the exact data versions named by bundle metadata. Matching
storage paths alone does not establish which saved data a later step read.
Bundle linking stays outside core.

## Layout

`overture_cairn.core` holds the types a Cairn is made of, the run that builds one,
and the rules it has to satisfy. It imports no compute engine and writes nothing.
The run stores only operations. Validation accepts small sets of row detail;
adapters apply the same rules to larger tables using their own engines.

Adapters live alongside the core and are the only place a table appears. Each one
knows how to build row detail for its engine and where to put both tables. None
are written yet.

## Record a filter

This example records a filter that rejected record `a`. The pipeline must supply
the IDs it actually rejected. These calls record the work; they do not read,
filter, or write the data.

```python
from overture_cairn import (
    IdentityCaptureStatus,
    Kind,
    Run,
    check_row_detail,
    row_detail_row,
)

run = Run("example-run", strict=True, output_key_columns=["id"])
source = run.read("read", "Read the input records", "input.parquet")
filtered = run.transform(
    "filter",
    "Keep records with valid geometry",
    inputs=[source],
    passthrough=[source],
    identity_capture_status=IdentityCaptureStatus.COMPLETE,
)
entries = [
    row_detail_row(
        filtered.op_id, Kind.DROPPED, input_id=["a"], detail="Invalid geometry"
    )
]
run.write(
    "write",
    "Save the filtered records",
    "output.parquet",
    input=filtered,
    identity_capture_status=IdentityCaptureStatus.COMPLETE,
)
run.validate()
check_row_detail(entries, run.ops, run.problems)
operations = run.operation_rows()
```

All recording methods default to partial capture. Mark an operation complete
only when its entries and pass-through rule account for every input fate and
output origin. This does not claim that earlier operations or column sources
were fully recorded.

Use `derived_from` to record contributions, including those whose IDs match.
An extra `content_changed` entry is optional when you also want to describe how
values changed. A null column list means no column detail was recorded.

## Status

Early. The schema, the four operation shapes, the validations, and the run that
registers operations are implemented. The tests include a small reader for
working through record relationships; it is not a public reader API. Nothing
writes a table yet. Column carry-over is deliberately not recorded, so a field
no entry mentions is explained by the operation descriptions on its path.

`docs/` carries the design: `cairn_motivation.md` for why it exists,
`cairn_design_spec.md` for the schema and its rules,
`cairn_recording_guide.md` for a decision procedure that turns a piece of pipeline
code into rows, `call-sites.md` for what calling it is meant to look like, and the
`pipeline-trace-*` and `pipeline-mechanics-*` pairs for the survey of existing
pipelines that the design was drawn against.
