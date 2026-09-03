
# Cairn: Motivation

Hi\! Adam here. I organize my thoughts best while writing, so I’m doing that here – no agents\!

## Introduction

Here at Overture, we use a few kinds of DAGs:

* Airflow DAGs that orchestrate processes  
* Spark MapReduce DAGs that are generated dynamically from our code  
* ASTs *in* our code (trees are a type of DAG)  
* Package dependency DAGs  
* Kedro DAGs, sometimes  
* I’m sure there are others here and there…

However, one of the most important DAGs is conspicuously absent-ish from this list: the *data* DAG.

Overture is a giant data processing machine; we acquire data from our members, process it, and put it into our product, which is also data. Much of our value, as an organization, is tied to the pipelines we have constructed, through which our data flows. This is common knowledge, internally.

But when asked to describe *how* this data flows, we are at a loss. Our data evolution records are incomplete, fragmented across bridge files, Python packages, Airflow DAGs, Docker containers, and their absence begets dangling, unanswerable questions.[^2] We would love to capture this information – but how?

Let’s generalize: what, overall, can we *do* with data? Let’s list the options[^3]:

1. Add it (∅ → x)  
2. Remove it (x → ∅)  
3. Transform it (x → y)  
4. Merge it (\[x, y, …, w\] → z)  
5. Split it apart (x → \[y, z, …, w\])  
6. Do something complicated to it (\[x, y, …, w\] → \[a, b, …, z \])[^4]

You may wonder about the “units” here: does “x” refer to a dataset? A single record? A specific column in a record? Unfortunately, the answer is “yes”; you can track these on all three levels – here are some examples:

| Process Type | Dataset-Level | Row-Level | Column-Level |
| :---- | :---- | :---- | :---- |
| Addition | Provider data upload | `INSERT INTO` | `ADD COLUMN` |
| Removal | S3 retention drop | `DELETE FROM` | `DROP COLUMN` |
| Transformation | PBF to Parquet | `normalize_record()` | `UPDATE z SET y=f(x)` |
| Merge | Places matching | \[place A, place B\] → place C | `SET y=f(x + w)` |
| Split | OSM → ways, nodes | Explode water geometry | `x = a[1], y = a[2]` |
| Many-to-many | Feeds → releases | Road network resegmentation | Complex UDFs |

Right now, we’ve only documented our dataset-level flows, but even that lineage is scattered across READMEs and repos. The public knows essentially none of it. We also have the violation store, changelog, bridge files, and a few other spots where we’ve tried to preserve at least the row-level mappings, but these are also isolated from one another.

What we *want* is the whole table, out in the open, as transparent as we can make it.

Cairn is my work-in-progress attempt to get us there.

## What’s a Cairn?

Wikipedia defines a Cairn as “*...a human-made pile (or stack) of stones raised for a purpose, usually as a marker or as a burial mound.*” I don’t want to make a giant library of maps; I want little piles of rocks to show the way.[^5] By that, I mean that these markers will *not cross bundles*. Instead, they will live inside a bundle, and show exactly how (only) that specific bundle was constructed from other bundles.[^6] To retrieve the full lineage chain, we can *simply walk a bundle’s* `metadata.json` and string together what Cairn has recorded inside each bundle.

This makes the task of designing Cairn a little more manageable: we no longer need to worry about making a centralized store or a global lineage DAG: we only need to figure out how to make bundles self-describing functions of their inputs. Taking it a step further, Cairn doesn’t even need to know bundles exist; it just needs to know how to record what happens *within the jobs that create bundles*.[^7] Cairn is not our unified provenance table, but it *enables* the creation of such a provenance table – which, with the code in its current state, would be a nightmare to make from scratch.

## Levels of Tracking

“The jobs that create bundles” are (mostly) `SparkSedonaJob`s. They can be… messy. Some of them use big `.sql("""...""")` blocks. Some of them use UDFs. Some of them are really, really complicated. If Cairn is supposed to hook into every single column-level transform, that means we either need to interpret Spark’s parsed plans, or we need to rewrite all of our code. Neither of these options seem great if we want to get this up and running in the next year.[^8] Instead, we need to build Cairn at multiple levels: if your code is too difficult or computationally intensive to log at the column-and-row level, you can step back to the row or even the “general dataframe statistics” level.

This means that Cairn needs a variety of different hooks that you can drop into your code, and ideally, hooks that encourage programming patterns supporting column-and-row-level tracking.

Some tech debt, first: I lied when I said there were three different levels of tracking (dataset, row, column) – there are actually four:

1. **Dataset-level**: A.K.A. metadata/asset-level; maybe it knows the schema, but not the rows.

2. **Row-level**: Tracks the whole row as an atomic entity. If even a single column changed, then the whole row has changed.

3. **Column-level**: Tracks the *column transformation logic*. At this level, we’re not recording what happens per-record, necessarily, but we know the *general process* through which each record’s columns were created, and maybe some statistics.

4. **Row & Column-level**: Tracks everything. You have a full record of how each column was changed for each row (e.g. “which provider did this place get its phone number from?” has an answer).

This is the table I keep in my head (it’s obvious, but I’m including it just in case):

|  | Doesn’t track rows | Tracks rows |
| :---- | :---- | :---- |
| Doesn’t track columns | Dataset-level | Row-level |
| Tracks columns | Column-level | Row \+ Col-level |

The awkward part is that 2 and 3 are … orthogonal. They converge on 4., but they’re independent goals. Cairn stays unopinionated on this, supporting both (it’s not as bad as I’m making it sound).

## Prior Work

Wouldn’t it be nice if someone else already solved these problems for us?

| Software | Tracks rows? | Tracks Cols? | Notes |
| :---- | :---- | :---- | :---- |
| OpenLineage (SW) | No | Yes | Well-established, Apache 2.0, but overlaps with our bundle code |
| [Spline](https://github.com/AbsaOSS/spline-spark-agent) (SW) | No | Yes | Hooks into the Spark driver and reads Catalyst execution plans; lots of gaps in the code. |
| [Titian](https://github.com/UCLA-SEAL/Titian) (SW) | Yes | Yes | 2018 (pre-Dataframe / Catalyst era), unmaintained |
| [Apache Atlas](https://atlas.apache.org/#/) (SW) | No | No | Built on Hadoop, too high-level |
| Provenance Semirings (algorithm) | Yes | Yes | Tightly coupled to your specific relational algebra operators; there’s no up-to-date Spark implementation, and per-row approaches tend to assume pipeline determinism. |
|  |  |  |  |

## How could Cairn embed itself in our code?

* **Decorator-based approaches:** That is, a `@track(input_id_col="id")`. This is conceptually nice, because it’s *very* unobtrusive to implement. The decorator can essentially wrap your filter function in a record-tracking anti-join on the pre- and post-op dataframes. The downside is that you can’t *just* use a decorator without some explicit conditions on what happens inside the function, which means you’ll probably need to edit your code anyway. Their uses are limited, too: much of our code isn’t a simple `DataFrame` → `DataFrame` transformation captured inside of a function. An upside is that we can automatically read function docstrings and use those to flesh out `reason` attributes, etc.  
* **Declarative approaches:** We make our *own* versions of basic Spark SQL functions like `filter()` and `join()` (e.g. `track_filter()`, `track_join()`) and then swap out our calls with these whenever possible. The implementation looks cleaner than the decorator approach, but there are a lot of edge cases to cover (we do a lot of joining/merging/filtering in raw SQL, we still have non-Spark pipelines to worry about, etc.).  
* **Sidechain approaches:** Instead of “hooking in” to our pipelines, we make lineage retention an auxiliary process that you call *after* your pipeline runs. This means that your pipeline is required to emit data (maybe just in-memory, but still) that describes what it did to the IDs it processed.

However, regardless of what we choose, we’re still missing something: the *order* in which things happened. Sure, we’re scoping ourselves to bundles, but our provenance tracking still happens *within* bundles, and we need to understand the ordering of the (sometimes complex) transformations that happen within there. The system needs to maintain some sort of internal stack (or maybe ID system) of operations that were applied to the data.

Open questions:

* Which implementation approach do we use? Do we use a combination of them? What do we want to support, in-code? This is a big question, and probably the most important one for me.  
* What’s the right way to track the order of operations?  
* I’m pretty sure the right schema looks like a table with (`input_id`, `output_id_or_null`, `action`, `reason`, …) or something like that. What other columns do we need?  
* How are we going to track intra-bundle order of operations?

Requirements for Cairn:

* The core package / schema / logic is reasonably decoupled from the Spark implementation (so one could make a DuckDB adapter, or something).  
* Is able to be used everywhere in our pipelines (assuming necessary edits on our pipelines, of course\!)  
* Makes the pipeline conform to *its* logic; does not feel like a solution custom-tailored to our pipelines. Basically, it’s a little bit opinionated.  
* Captures the information necessary to answer “the three questions” (given a provider’s input record, what happened to it? – given a published output record, how was it made? – what is the overall structure of our pipelines?)  
* Is flexible enough to support “implementation rollout”. Basically, if a part of our pipeline is near-inscrutable, that’s okay, we just track its inputs and outputs, but *later*, we’re able to come back and refactor that piece so that we get more fine-grained tracking… *but we keep the high-level tracking, too*. On that note:  
* ~~Supports an *operation hierarchy*. Basically, ways to group nodes (or maybe edges??) together, a la contraction hierarchies or compound graphs. This is useful for being able to talk about a record in “this stage of our pipelines” instead of “this specific Spark filter statement”.~~ (eventually this would be nice, but I don't think it's *necessary* for now)
* Supports row-level tracking, and *some* form of column-level tracking that lets us answer “what source did this attribute come from?”

## Implementation Proposal

Lineage lives inside of "a cairn", which consists of...

### Two tables in each bundle:

First, `ops`: A table of *operations* representing the logical data flow DAG. This one only has as many rows as you have discrete data processing steps, so it can live in Python's memory.

| Column | Description |
| --- | --- |
| `run_id` | The run_id of the bundle the cairn is inside of |
| `name` | The name of the operation (must be unique within the cairn, after slugging) |
| `op_id` | The operation ID; just `{run_id}.{slug(name)}`, e.g. `r1.drop-weak-overlaps` |
| `parent_op_ids` | Which ops fed into this one? |
| `reason` | *Why* this operation happens (required!) |
| `records_captured` | Indicates how completely Cairn is able to capture this operation (`COMPLETE`/`INCOMPLETE`/`NOT_APPLICABLE`) |
| `recording_method` | *How* this step's info was recorded within Cairn. This can either be `DECLARATIVE` or `COMPARATIVE` |
| `output_id_cols` | The columns that the operation's outputs are keyed by (e.g. `[gers_id, provider]`) |
| `physical_source` | What physical location this operation reads from, if any |
| `physical_dest` | What physical location this operation writes to, if any

And `edges`: A table that captures *row-level information* about which operations were applied to different records:

| Column | Description |
| --- | --- |
| `op_id` | Which operation was run? (must exist in the `ops` table) |
| `input_op_id` | Which operation fed this one? This column must be set to an `op_id` that is a member of `ops[op_id].parent_op_ids` (or null if `input_id` is null) |
| `kind` | The kind of change (`drop`, `mint`, `derived_from`, `content_change`, `flagged`) |
| `input_id` | The input record ID (an array of columns) |
| `output_id` | The output record ID (an array of columns) |
| `columns` | Which columns were affected? |
| `column_change` | Can be `set` (filled from empty), `replaced` (overwrote a value), or `cleared` |
| `detail` | *Why*, for this one record (e.g. "IOU 0.31, below 0.5") |


The coolest thing about this schema is that it lets you **straight-up concatenate** the cairns from different bundles! Nothing here breaks when you join the tables with each other. The only rough spot is at the bundle edges, but you can always join on the `physical_source` and `physical_dest` columns or search through the bundle metadata.

Anyway, the most important detail here is probably the `kind` column, which contains...

### A way to represent arbitrary operations:

The entries of `kind` refer to how an operation affects the *IDs* of a record. Let's see how the taxonomy of operations I describe in my introduction fits into this data model.

| Operation type (from the taxonomy in my introduction) | Edge kind |
| :---- | :---- |
| Add | Falls under `mint`; we minted a new ID |
| Remove | Falls under `drop`; we removed a record |
| Transform | Falls under `content_change`; we transformed the data, or `derived_from` if the ID was modified. |
| Merge | Multiple `derived_from`/`content_change` entries, one row per input, all sharing an `output_id` |
| Split | Multiple `derived_from`/`content_change` entries, one row per output, all sharing an `input_id` |
| Many-to-many | One entry per actual derivation (at most M x N of them)! |

Basically, if the ID changed, you use `derived_from`, if it didn't change, you use `content_change`. Joins and merges are written as edge lists that indicate "hey, this record influenced that one".

There is also a `flagged` kind that lets us say "hey, something's up with this particular record" without actually changing it. This is useful for debugging, logging violations/checks, etc.

### Some more decisions:

* **Pass-throughs are not recorded**: If we have a filter that drops 2% of rows, we don't record anything for the other 98%. The default assumption is that data passes through an `op` unchanged unless otherwise specified.

* **IDs are column sets**: We don't always key on just one column! ID types are *arrays* of strings, not just strings. If ID *is* just one column, cool, the array has one entry.

* **The `core` subpackage is JUST the schema**: `overture_cairn.core` doesn't depend on Spark at all; it just describes the tables and some validations you can perform on them. specific RDBMS/processing engine implementations are delegated to other sub-packages which import `core` (similar to what our schema repository does).

* **Checks are just ops that emit `flagged` edges**: We have a few dozen check implementations spread across `osm_checks`, `overture_transportation/validations`, and the buildings filter jobs, plus three different words for the same output (`violation_name`, `check_name`, `filter_type`). Cairn can log these as `flagged` edges! I'm going to suggest that we centralize them (`overture_checks`?) and plop in an integration point with Cairn.

* **The best Spark implementation is less important than the schema**: The decorator approach is nice because it encourages refactoring big messy blocks of code into discrete functions. The sidechain approach is nice because it's probably the least amount of work. But *both* approaches can write to the Cairn schema. I think we can see what works best for each pipeline and start with the easier options.

* **`records_captured` indicates how well we're tracking an operation's lineage**: 
    * `COMPLETE`: the edges account for the operation's whole effect on identity.
    * `NOT_APPLICABLE`: the operation touches no record identities at all. These no-ops are useful flags for recording general pipeline progress so that the full chain of operations is represented in the final graph.
    * `INCOMPLETE`: incomplete or nonexistent row-level tracking. This is basically a stopgap that lets us know "hey, we need to fix this! TODO!" while still capturing the logic-level information about what happened.

* **Cairn defines a data flow DAG**: The `input_op_id`/`parent_op_ids` make the two tables act as edge lists for a graph. We can easily validate the acyclicity of the op DAG with a runtime check.

* **Column-and-row-level tracking is performed conservatively**: Yes, I could see a world where we want column-level lineage everywhere, and technically, I think this architecture supports that (admittedly, there is a lot of flexibility). But I *really* don't think it's worth the I/O and storage cost. Anyway, to prevent a multiplicative (rows ⨉ columns) blowup, I suggest two things:
    1. We allow a single row to specify multiple changed/added columns. That way, if we pulled 10 attributes from a source, we write one row, not ten.
    2. We only report columns when *more than one donor could have supplied the values*. Otherwise, the modifications are recoverable from the code and we can just rely on the regular info.

* **This can actually work with the corpus**: The corpus can be "unrolled" by indexing its snapshots, like a recurrent neural network. We just include the Iceberg snapshot inside the operation paths, and boom! We're now tracking lineage across the corpus history (thanks, Iceberg). There are still some open questions (where is the Cairn stored, physically), but conceptually, at least, the corpus doesn't break anything.

* **Cairns do not look beyond themselves**: A cairn can check its own self-consistency, but it does not make any attempt to understand the world outside of itself. There is no code to piece together information across bundles in here; that will live elsewhere (somewhere more Overture-specific). As long as we know where the inputs and outputs are, we can piece things together later.

* **The bundle-level split isn't strictly necessary**: Yeah, I said you have one cairn per bundle, but it's not like that *needs* to be the case. You *could* just write everything to the same centralized table instead, but keep the schema. I think the per-bundle approach is *cooler*, but both are options.

## Option B
This solution is obviously a somewhat-complex system. There is a simpler one, but it is less flexible and probably more expensive: rows drag along a `history` column (schema: `struct<op, reason, kind, from_id>`) with a record of everything that happened to them. I’ll cover the pros, and cons:

### Pros
- No need for a post-hoc walk; all the info is right there.
- The schema is slimmer, and we don't need any additional tables to hold it, just a new column everywhere else.
- Implementation is easier (maybe?)

### Cons
- The size of the column scales with the size of the pipeline. We start hauling O(n^2) in the asymptotic limit. This is especially noticeable for column-level operations.
- We would still need an auxiliary table for them dropped rows, which was kind of the point of all of this anyway.
- It's unclear how the corpus would work with this. Would a merge operation combine histories? Does that mean a single GERS ID would indefinitely accumulate history?
- We no longer have the flexibility to opt-out of row-level tracking; it has to happen everywhere, or it doesn't happen at all.

Food for thought!

## Appendix A: Adam’s Rules of Helpful Lineage Systems

1. **Helpful lineage systems should match the resolution processes are described at.** If your codebase is written in PySpark, your lineage tracking should think in terms of dataframes and Python functions. If your lineage lives deeper than your code, it requires some kind of “decompilation” step. If it lives higher, it’s probably not descriptive enough to help with debugging.

2. **Helpful lineage systems are information-conserving.** A codebase with a well-formed lineage system never drops any information. It may claim to filter or drop data, but these are really unitary operations that only *organize* data.

3. **Helpful lineage systems are easily-navigable.** A road is only useful insofar as its ability to take you somewhere. When designing a lineage system, it’s a good idea to make “how many steps does it take for an outsider to find something” an optimization target. I think the best way to realize this is via *hierarchical lineage*: the system should support nested descriptions of data flows (e.g. it can answer “what filters did this particular job run apply to its data?”)

4. **Helpful lineage systems do not assume deterministic pipelines.**

5. **Helpful lineage systems are extensible.** Data is clean, but pipelines are often messy. If data flows through unstandardized subsystems, any attempt at a ‘one-size fits all’ will fail to track edge cases. This problem goes away if the lineage system is abstract enough to be easily extensible (really, this is just a rule about good software design).

[^1]:  Photo credit: https://www.flickr.com/people/logicalrealist/

[^2]:  “Dangling” as in “looming”, like the sword of Damocles, or possibly like a small copy of the United States in the ocean near the Gulf of Guinea.

[^3]:  Relational algebra is also a starting point (σ, π, ∪, −, ×, ρ) – but it’s further from plain English, and doesn’t include queries like `SUM`, `COUNT`, `AVG`, `GROUP BY`, and other operations covered by extended / nested relational algebra. The taxonomy I suggest here is higher-level and less-precise, but it makes up for those losses in interpretability. A downside: [provenance semirings](https://dl.acm.org/doi/10.1145/1265530.1265535) are harder to implement.

[^4]:  You may have noticed that there is some redundancy within this classification: since I’m being loose with terms, I *could* just define 6\. as a combination of 4\. and 5.: \[x, y, …, w\] → q → \[a, b, …, z \]. However, this necessitates that *no information is lost* upon the transfer to q, which we can’t necessarily guarantee. I guess I could’ve explicitly introduced a *concatenation* operator, then 6\. just becomes a set of concatenated merges, but this is getting into the weeds. The point is that this sort of operation *can* happen, sometimes via black-box (e.g. a model with multiple inputs and multiple outputs), which makes this representation useful.

[^5]:   Alex will say “then why not call it ‘breadcrumbs’?”, but that isn’t as mappy, and it has too many syllables (fight me).

[^6]:  Want to hear a nice consequence of this architecture? *Every bundle will need to have an explicit input and output schema*. If a TX schema doesn’t match an RX schema, we can tell Cairn to flag it (or even fail).

[^7]:  You might be thinking “what about the corpus?” – we’ll get there when we get to Scala.

[^8]:  I did consider parsing Spark’s DAGs, and Cairn still records them when it’s able to. I think nothing beats well-written human docstrings, but some LLMs are surprisingly good at making sense of even Catalyst-optimized Spark plans. It could be worth it to play around with automatically generating descriptive English explanations of Spark jobs by dumping the plans into Bedrock.