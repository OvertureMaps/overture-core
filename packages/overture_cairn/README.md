# overture-cairn

A record of what a data pipeline does to its data. For any one record, it can say
what happened to it and why. For any dataset, it can say how that dataset was
built.

A cairn is two tables:

- **`ops`**, one row per operation, where an operation is a step the pipeline
  author considered worth naming. A run has hundreds of these at most.
- **`edges`**, one row per fact about one record's identity. This is the
  record-level layer, and it can be larger than the data itself.

Edges are deltas. A record an operation did not touch gets no edge, and that
absence means it passed through unchanged. Writing the untouched case explicitly
would cost a row per record per step for a fact that is already true by default.

## Layout

`overture_cairn.core` holds the types a record is made of, the run that builds
one, and the rules it has to satisfy. It imports no compute engine, holds no
per-record data, and writes nothing, so building a record and checking one work
on a machine with nothing else installed.

Adapters live alongside the core and are the only place a table appears. Each one
knows how to build edges for its engine and where to put both tables. None are
written yet.

## Status

Early. The core's types and rules are implemented; the run that registers
operations is an outline. Nothing writes a table yet.

`docs/` carries the design: `cairn_motivation.md` for why it exists and what the
schema is, `call-sites.md` for what calling it is meant to look like, and the
`pipeline-trace-*` and `pipeline-mechanics-*` pairs for the survey of existing
pipelines that the design was drawn against.
