# How to record an operation

Start with one step in your code and follow the chart to see what to record.
A record is one item in a dataset, such as a building or a place. Its ID
identifies that item within the dataset. An input is a dataset the step uses,
and an output is the dataset it produces.

Give the step a description. Use `output_key_columns` to name the fields that
identify its output records. The [recording guide](./cairn_recording_guide.md)
explains the full rules for filling in the fields.

```mermaid
flowchart TD
    start(["Choose one step in your code."])
    compound{"Does the step mix reading or saving data with changing records,<br/>use multiple sources or destinations, produce multiple outputs,<br/>or save data for a later step to read?"}
    split["Record these actions as separate operations.<br/>Separate reading, changing, and saving data.<br/>Give each operation one output.<br/>Record saving and later loading data as a write followed by a read."]
    shape{"Does the step read or save stored data?"}
    readOp["Record a read operation.<br/>Set physical_source to the location it reads.<br/>Leave the lists of input operations and pass-through inputs empty."]
    writeOp["Record a write operation.<br/>Set physical_dest to the location it saves to.<br/>Name its one input operation and declare that its records pass through.<br/>Keep the same fields for identifying records."]
    copyOp["Record a copy operation.<br/>Set physical_source and physical_dest to the two locations.<br/>Name its one input operation and declare that its records pass through.<br/>Keep the same fields for identifying records."]
    transformOp["Record a transform operation.<br/>Leave both location fields empty.<br/>Name every operation that supplies an input dataset."]
    passthrough["Fill in the pass-through input list, passthrough_input_op_ids.<br/>These inputs supply records that carry forward under the same IDs.<br/>For filtering or cleaning values, name the starting input.<br/>For adding reference values, name the main input, not the reference input.<br/>For combining datasets without changing records, name all inputs.<br/>If every record's path is recorded explicitly, or none carry forward, leave the list empty."]
    effects{"What does the step do to the records?<br/>Follow every branch that applies.<br/>Record only facts you know."}
    dropRows["Add a dropped entry for each removed record.<br/>Put its ID in input_id and leave output_id empty.<br/>Add a reason if needed."]
    mintRows["Add a minted entry for each new record with no input record.<br/>Put its ID in output_id.<br/>Leave input_id and input_op_id empty."]
    deriveRows["Add a derived_from entry for each input record that contributes to an output record.<br/>Record both IDs, even when they are the same.<br/>If a split keeps the original record, record that link too."]
    uniform{"Does one rule explain the changes<br/>without needing a separate entry for each record?"}
    changeRows["Add a content_changed entry for each change you record.<br/>Put the record's ID in both ID fields.<br/>Name the changed columns or explain the change in detail.<br/>Optionally, use column_change to say whether a value was filled in, replaced, or cleared."]
    flagRows["Add a flagged entry for each finding you record.<br/>Put the record's ID in both ID fields.<br/>Name the columns or explain the finding if useful."]
    rowFields["Link each entry to this operation.<br/>Use input_op_id to identify its input when there are multiple inputs.<br/>For dropped and minted entries, set affected_output_columns to null.<br/>For other entries, use this field to name the columns you have information about.<br/>Use null if you did not record information about specific columns."]
    noRows["Do not add record-level entries for this branch.<br/>Describe what the operation does."]
    coverage{"Can the entries and pass-through rule explain what happened to every input record<br/>and where every output record came from,<br/>using IDs that distinguish records within each dataset?"}
    complete["Set identity_capture_status to complete.<br/>The recording accounts for every record in this step."]
    partial["Set identity_capture_status to partial.<br/>Keep the facts you know.<br/>Explain what is missing in the operation's description."]
    finish(["The operation is recorded."])

    start --> compound
    compound -->|It does.| split
    split -->|Follow the chart for each operation.| start
    compound -->|It does not.| shape
    shape -->|It only reads stored data.| readOp
    shape -->|It only saves data.| writeOp
    shape -->|It copies data between locations without changing records.| copyOp
    shape -->|It neither reads nor saves stored data.| transformOp
    readOp --> noRows
    writeOp --> noRows
    copyOp --> noRows
    transformOp --> passthrough
    passthrough --> effects
    effects -->|It removes records that contribute to no output record.| dropRows
    effects -->|It creates records that do not come from input records.| mintRows
    effects -->|It combines or splits records, changes their IDs, or takes values from other records.| deriveRows
    effects -->|It changes values without changing IDs or taking values from other records.| uniform
    effects -->|It notes findings about records that remain in the output.| flagRows
    effects -->|It makes no record changes and notes no findings.| noRows
    effects -->|You do not know how the input and output records are related.| partial
    uniform -->|One rule is enough.| noRows
    uniform -->|Individual entries are needed.| changeRows
    dropRows --> rowFields
    mintRows --> rowFields
    deriveRows --> rowFields
    changeRows --> rowFields
    flagRows --> rowFields
    rowFields -->|Finish recording all known effects first.| coverage
    noRows --> coverage
    coverage -->|Every record is accounted for.| complete
    coverage -->|Some records are unexplained, or you are unsure.| partial
    complete --> finish
    partial --> finish
```

When the chart tells you to leave a location or ID field empty, use `null`.
This means no value was supplied. Use `null` for an unspecified column list
too; an empty column list written as `[]` is not allowed. The lists of input
operations and pass-through inputs do allow `[]` when they have no members.

`input_op_id` identifies the operation that supplied an input record when the
step uses multiple inputs. Leave it empty when there is only one input or when
the entry records a newly created record with no input record.

Use `column_change` only on `content_changed` entries, and only if you want to
record how the value changed. Use `set` when an empty value was filled in,
`replaced` when an existing value changed, and `cleared` when a value became
empty.

Taking a value from another record does not require an extra `content_changed`
entry. Add one only if you also want to describe the before-and-after change.
Changing a record's ID does not require an extra `dropped` entry. Do not call a
record `minted` just because you do not know where it came from.

All branches that apply describe the same operation. Consider them together
before deciding whether every record is accounted for. A column list set to
`null` means no information about specific columns was recorded; it does not
mean all columns. Missing information about columns alone does not make the
record relationships incomplete.
