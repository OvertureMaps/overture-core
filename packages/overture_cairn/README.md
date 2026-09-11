# overture-cairn

A record of what a data pipeline does to its data. For any one record, it can say
what happened to it and why. For any dataset, it can say how that dataset was
built.

A Cairn is two tables:

- **`operations`**, one row per operation, where an operation maps one or more
  input datasets to a single output. A run has hundreds of these at most.
- **`row_detail`**, one row per fact about one record's identity. This is the
  record-level layer, and it can be larger than the data itself.

Row detail entries are deltas. A record an operation did not touch gets no entry,
and that absence means it passed through unchanged. Writing the untouched case
explicitly would mean a row per record per step, for something already true by
default.

Cairns live inside the bundle they describe. Collecting the full lineage chain
means walking a bundle's `metadata.json` and concatenating what each Cairn along
the way recorded, so no operation here needs to know that bundles exist.

## Layout

`overture_cairn.core` holds the types a Cairn is made of, the run that builds one,
and the rules it has to satisfy. It imports no compute engine, holds no per-record
data, and writes nothing, so building a Cairn and checking one work on a machine
with nothing else installed.

Adapters live alongside the core and are the only place a table appears. Each one
knows how to build row detail for its engine and where to put both tables. None
are written yet.

## Status

Early. The schema, the four operation shapes, the validations, and the run that
registers operations are implemented. Nothing writes a table yet.

`docs/` carries the design: `cairn_motivation.md` for why it exists,
`cairn_design_spec.md` for the schema and its rules,
`cairn_recording_guide.md` for a decision procedure that turns a piece of pipeline
code into rows, `call-sites.md` for what calling it is meant to look like, and the
`pipeline-trace-*` and `pipeline-mechanics-*` pairs for the survey of existing
pipelines that the design was drawn against.
