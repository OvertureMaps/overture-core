# How to record an operation

A decision procedure for turning a piece of pipeline code into Cairn rows. Work
top to bottom; each answer either lands you on a recording or sends you to the next
question. See [cairn_design_spec.md](./cairn_design_spec.md) for what the columns
mean and which rules the tables have to satisfy. Finish at Part 7 even if the
operation needs no row detail.

## Part 1: How many operations is this?

Start with a piece of code and split it before recording anything.

* **Does it write to more than one location?**
  * **Yes** → one write operation per location. Split, then re-ask.
  * **No** → continue.
* **Does it read from more than one location?**
  * **Yes** → one read operation per location, then a transform that consumes all
    of them. Split, then re-ask.
  * **No** → continue.
* **Does it produce more than one output dataset?** For example, candidate pairs
  and then records, or separate accepted and rejected tables at the same grain.
  * **Yes** → one operation per output. Split them, and re-ask for each.
  * **No** → continue.
* **Does it materialize something partway through and read it back?** A checkpoint
  before an expensive step is the usual reason.
  * **Yes** → a write, then a read, with the halves either side becoming their own
    operations. Nothing may consume a write, so the second half reaches the data by
    reading the location back.
  * **No** → continue.
* **Does it change records while reading or writing them?**
  * **Yes** → separate the read, transform, and write as needed. A read-filter-write
    is not a copy, even if all three happen in one block of code.
  * **No** → you have one operation. Go to Part 2.

## Part 2: Which shape is it?

Nothing stores the shape. It follows from the two location columns, so answering
these fills them in.

* **Does it take records out of a location?** (reading a parquet path, an
  API, a database)
  * **And also put bytes somewhere?** → **Copy**. Set `physical_source` and
    `physical_dest`. `input_op_ids` has exactly one entry. The grain carries over
    from that input, so repeat its `output_key_columns`.
  * **Otherwise** → **Read**. Set `physical_source`. `input_op_ids` is empty,
    because a read starts a branch. Set `output_key_columns` to whatever the
    records are keyed on as they arrive.
* **Does it put an operation's output in a location?**
  * **Yes** → **Write**. Set `physical_dest`. `input_op_ids` has exactly one
    entry. Repeat that input's `output_key_columns`.
* **Neither** → **Transform**. Both location columns are null. `input_op_ids`
  names every operation it consumed. Go to Part 2b.

Reads, writes, and copies have no row detail. They move records without changing
their contents. A write and a copy pass their one input through; a read has no
input operation. Set `passthrough_input_op_ids` accordingly, fill in
`description`, and go to Part 7. Their successful record movement can be complete
without an entry per record. A read does not claim to explain its source's history.

## Part 2b: Which inputs may an absent entry speak for?

A transform has to say which inputs supply the continuing records. Name an input
in `passthrough_input_op_ids` when its records survive under the same identity
except for recorded outcomes. Leave it out otherwise. This declaration lets
absence mean survival only when `identity_capture_status` is `complete`.

* **Do records from every input keep their identities, except for recorded
  outcomes?** A filter, a normalization, a union with a valid output key.
  * **Yes** → name all of them.
* **Is one input the records and the others reference data you look things up in?**
  An enrichment against a height table, a feed matched against a corpus.
  * **Yes** → name the record stream only. The reference records never become
    output records, so absence must not be read as their having survived. Note that
    both sides are often keyed by `id`, so nothing about the keys could tell a
    reader this.
* **Are all output links explicit, or is there no same-identity pass-through?**
  A fully recorded merge, an aggregate, a count.
  * **Yes** → name none of them. Any surviving identity is recorded explicitly;
    an absent entry does not supply another link.

The default is an empty list. That means no implicit survival links, not that
there are no contributions: explicit entries still say what happened. One input
does not settle the answer either. A group-by and a filter both have one input,
but only the filter has surviving records that need no entry. Go to Part 3.

## Part 3: Does this transform need row detail?

Row detail is only worth writing where the information is unrecoverable from the
output or the operation's rule. Ask what a reader could work out on their own.
This section says which facts are needed, not whether you managed to capture
them all. Record the facts you know and use Part 7 to say whether the account is
complete.

* **Does it remove records?** (a filter, an anti-join, a dedupe that keeps one row
  per group)
  * **Yes** → row detail needed. The removed rows are gone from the output, so
    nothing else can say what left. Go to Part 4.
* **Does it create an identity or change a record's id?** (minting a uuid,
  assigning a shared id, taking an id from another table)
  * **Yes** → record the new identity or the link from the old one. Go to Part 4.
* **Does it combine records, choose a donor's value, or split one into several?**
  * **Yes** → row detail needed. Which inputs produced which output lives nowhere
    else. Go to Part 4.
* **Does it assert something about particular records without changing them?** (a
  check, a validation, a violation)
  * **Yes** → row detail needed. The judgment is stored nowhere in the data. Go to
    Part 4.
* **None of the above.**
  * **Did it leave the records alone?**
    * **Yes** → no row detail. Write `description` and go to Part 7.
  * **Does the same rule explain the value changes, without a per-record donor
    choice?** A whitespace strip, a unit conversion, a reprojection.
    * **Yes** → the operation's description and code can explain the rule. No
      per-row entry is required for that alone. Go to Part 7. This does not claim
      that every prior value can be recovered from the output.
    * **No** → record known changes as `content_changed` entries. Go to Part 4.

## Part 4: Which `kind` does each entry get?

Write one entry per fact or link, not necessarily one per record. First identify
the fact you know, then use these questions. They match the teaching sketch in
the design spec.

For every entry with an input ID, set `input_op_id` when the operation has more
than one input. Choose the input that supplied this entry's record. With one
input, leave it null. Mints always leave it null.

* **Are you recording a contribution, with both an input and output identity?**
  An ID rebind, merge, split, or chosen donor.
  * **Yes** → `derived_from`. Set both IDs, even when they are equal.
    `column_change` stays null. Go to Part 5.
* **Did the operation create an identity with no input identity?**
  * **Yes** → `minted`. Set `output_id`. Leave `input_id`, `input_op_id`,
    `affected_output_columns`, and `column_change` null. Go to Part 7.
    Reading existing IDs is not minting, and an unknown source is not a mint.
* **Are you recording that an input did not survive under the same identity?**
  * **Yes** → `dropped`. Set `input_id`; leave `output_id`,
    `affected_output_columns`, and `column_change` null. A `detail` is worth
    adding when this record's reason goes beyond the operation's description.
    Go to Part 7. A rebind already has an output link; changing an ID does not
    require an extra drop.
* **Are you recording a value change to a surviving record?**
  * **Yes** → `content_changed`. Set both IDs to the same value. Fill in
    `affected_output_columns` or `detail` so the entry says what changed.
    Go to Part 5, then Part 6.
* **Are you recording a finding about a surviving record?**
  * **Yes** → `flagged`. Set both IDs to the same value and leave `column_change`
    null. Column names and `detail` are optional; the operation identifies the
    finding. Go to Part 5.
* **None of the above** → no entry. An unchanged record is not automatically a
  flagged record. Go to Part 7.

A merge writes one contribution per input record, even when those records came
from a single input table. A split writes one per child. If a split keeps A and
creates B, write both `A -> A` and `A -> B`; the second link alone does not imply
the first. To claim complete capture, list all the links. A flag or content-change
entry does not replace that list.

A donor link is enough to record where a value came from. An extra
`content_changed` entry is optional if you also want to record whether the value
filled a blank or replaced something. Keeping an input ID does not require the
extra entry.

Do not drop a record and also claim that it survived under the same identity in
the same operation. A drop can coexist with contributions under other IDs.

## Part 5: Which columns does the entry name?

`affected_output_columns` is null for `dropped` and `minted`, so this applies to
the other three.

* **Are you recording column-level detail for this entry?**
  * **No** → leave the list null. The record-level fact still stands, but the
    entry makes no claim about particular columns. A content change still needs
    `detail` to say what changed. Go to Part 6 for a content change, or Part 7
    otherwise.
  * **Yes** → continue.
* **Which fact is this entry recording?**
  * **A contribution** → name the output columns this input supplied or helped
    compute. A merge choosing a phone number between providers is the clearest
    case.
  * **A content change** → name the changed columns.
  * **A finding** → name the columns the finding concerns.
* **Does the entry concern all columns?**
  * **Yes** → list all of them.
  * **No** → list only the columns the entry concerns.
* **Do different changed columns have different fates?**
  * **Yes** → group them into separate entries for Part 6.

For a merge where A supplies names and B supplies websites, name those columns
on their respective entries. If all you know is that both records contributed,
leave both lists null. The links can still form a complete record-level account.

Null means no column-level detail was recorded, not all columns or the columns
nobody else named. It is fine to name columns on some entries and leave others
null. Several inputs can name the same column if its value was computed from
all of them. An empty list is invalid; use null for no column detail.

`identity_capture_status` covers record relationships, not column sources.
Rules for tracing unrecorded fields through an enrichment or filter remain open
in the design spec; a null list does not establish those sources.

Go to Part 6 for a content change, or Part 7 for the other kinds.

## Part 6: Which `column_change`?

Only a `content_changed` entry carries one. Ask what was in the column before.

* **Was the column empty before?**
  * **Yes** → `set`.
  * **No** → **Is it empty now?**
    * **Yes** → `cleared`.
    * **No** → `replaced`.

An adapter should fill this in on its own. A comparison-based capture already ran
the check that decided the entry exists, and a helper that overwrites a column
holds the earlier state. The field is nullable so a hand-built entry can skip it,
and null means the fate went unstated. Use the adapter's documented meaning of
"empty" consistently. Go to Part 7.

## Part 7: Did we capture the whole effect on identities?

Ask this after accounting for all the operation's records, not after each entry:

* **Do the entries and declared pass-through rule cover all input fates and
  output origins?**
  * **Yes** → set `identity_capture_status=complete`.
  * **No, or we do not know** → set `identity_capture_status=partial`, the default.
    Say what is missing in `description`. Keep the facts that were captured.

Complete means complete for this operation's identities. It does not promise
complete upstream history, every prior value, or every column's source.
Ambiguous keys prevent complete record capture; give distinct rows distinct keys.

An empty detail table does not decide the status. A filter that rejects nothing
can be complete. A filter whose rejects were never recorded is partial.
Helpers for known operations can fill the status in for the caller.

With partial capture, an absent entry means unknown. With complete capture, it
means same-ID survival for declared pass-through inputs and no direct
contribution for other inputs.

## Examples and limits

The [design spec's examples](./cairn_design_spec.md#examples) show operation rows
alongside row detail, including a complete filter, a partial filter, same-ID
contributions, and optional content-change entries.

* **An aggregate or other grain change:** set `output_key_columns` to the output
  grain and declare no pass-through inputs unless records really do survive.
  Record known contributions, or mark capture partial when the links are missing.
* **A before/after comparison:** for a known filter, missing IDs are drops. For
  arbitrary code, disappearing and appearing IDs do not establish a drop and
  mint; they might be a rebind or merge. Do not guess those links.
* **Work outside your process:** record the write and the later read. An external
  matcher may change IDs or keep them; equal IDs alone do not prove continuity.
  Link exact data versions where that information exists, and leave other
  handoffs as gaps.
* **A changing calculation:** capture the results the pipeline actually used.
  Share the chosen IDs, donors, and rejection decisions rather than computing
  them separately for lineage. How to do that belongs in the adapter, not core.
