## Overview

A Cairn consists of two tables:

1. An `operations` table that tracks the dataset-level transformations performed by a specific instance of a process.
2. A `row_detail` table that includes, for each entry in the `operations` table (optionally),  row-level information about the transformation.

These tables are intended to be easily concatenatable.

## Table Schemas

### Operations

An operation is essentially a *function* that transforms some input data. This table is a *catalog* of functions, but does not contain information regarding *what* was passed through said functions.

| Column | Description | Reason for inclusion | Example |
| -- | -- | -- | -- |
| `op_id` | The ID of this operation; distinct across runs and bundles. I suggest a composite `{run_id}.{slug(op_key)}`, the definitions for these columns follow.| The operation identifier that we use to build the lineage graph. | `20260903XXXX.reduce_precision (str)` |
| `run_id` | Put simply, the `run_id` of the bundle in which this Cairn lives. | This column makes records distinguishable when Cairns from different bundles are concatenated. | `20260903XXXX (str)` |
| `op_key` | This is a (descriptive) slug of the operation name. | Gives us a pseudo-stable identifier that can be tracked across code versions | `reduce_precision (str)` |
| `output_key_columns` | The columns that the output of the operation is keyed on. | Necessary for knowing how to trace the lineage graph at the row-level. | `["gers_id", "provider"] (array[str])` |
| `description` | Plain-text description of *what* this operation did. | Makes the lineage graph human-interpretable and establishes a way to understand our pipelines without reading code. | `"Trailing/leading whitespace is removed from names." (str)` |
| `has_row_detail` | If `true`, then this operation *may* have associated entries in the `row_detail` table. | This is mostly for convenience and tracking how well we're capturing lineage. | `true (bool)` |
| `physical_sources` | What physical location(s) this operation reads from, if any. | This lets the bundle-entrypoint operations specify what sources they draw information from. Because an operation can have multiple inputs, | `["s3://overture-stuff/data.json", "https://data-portal.com/data"] (array[str])` |
| `physical_dest` | What physical location this operation writes to, if any. | This lets bundle-exit point operations specify what they end up materializing. | `"s3://overture-stuff/output.parquet"` |

