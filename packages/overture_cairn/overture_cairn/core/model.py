"""The types a Cairn is made of, and the shape of the two tables it becomes.

Operations are objects, because one run has a countable number of them. Row detail
is only a schema, because a single operation can describe more records than fit in
one process, so whatever materializes those entries does so as a table inside an
adapter.

Nothing here writes anything. A Cairn is two tables, named by ``OPERATIONS`` and
``ROW_DETAIL`` and described by ``OPERATION_COLUMNS`` and ``ROW_DETAIL_COLUMNS``. An
adapter decides what they are stored as and where they go.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

#: The two tables a Cairn is made of. Fixed, because collecting Cairns from
#: different adapters means finding the same two names in each.
OPERATIONS = "operations"
ROW_DETAIL = "row_detail"


class Kind(Enum):
    """The fact an entry records about an input, an output, or their relationship.

    ``DERIVED_FROM`` records a contribution, even when the IDs match.
    ``CONTENT_CHANGED`` records a value change on a surviving record.
    Merges and splits are groups of contribution links.
    """

    DROPPED = "dropped"
    MINTED = "minted"
    DERIVED_FROM = "derived_from"
    CONTENT_CHANGED = "content_changed"
    FLAGGED = "flagged"


class ColumnChange(Enum):
    """What happened to the values in an entry's ``affected_output_columns``.

    ``SET`` fills an empty value, ``CLEARED`` empties a present value, and
    ``REPLACED`` changes a present value. Only ``CONTENT_CHANGED`` carries this
    field, because it records how values changed.
    """

    SET = "set"
    REPLACED = "replaced"
    CLEARED = "cleared"


class IdentityCaptureStatus(str, Enum):
    """Whether entries and pass-through rules account for every record.

    This says nothing about missing column detail or earlier operations.
    """

    PARTIAL = "partial"
    COMPLETE = "complete"


class OpType(Enum):
    """Which of the four shapes an operation has.

    :func:`op_type_of` reads this off ``physical_source`` and ``physical_dest``.
    Nothing stores it, so an operation cannot disagree with itself about what it
    is. ``TRANSFORM`` covers everything that touches no physical location, which
    is most of a pipeline.
    """

    READ = "read"
    WRITE = "write"
    COPY = "copy"
    TRANSFORM = "transform"


#: Which ends an entry of each kind carries, as ``(input_id, output_id)``.
ENDPOINTS: Mapping[Kind, Tuple[bool, bool]] = {
    Kind.DROPPED: (True, False),
    Kind.MINTED: (False, True),
    Kind.DERIVED_FROM: (True, True),
    Kind.CONTENT_CHANGED: (True, True),
    Kind.FLAGGED: (True, True),
}

#: Kinds whose entry describes one surviving record, so both ids are the same one.
SAME_ID_KINDS: Tuple[Kind, ...] = (Kind.CONTENT_CHANGED, Kind.FLAGGED)

#: Kinds that concern a whole record, so naming columns would say nothing.
WHOLE_RECORD_KINDS: Tuple[Kind, ...] = (Kind.DROPPED, Kind.MINTED)


def slug(name: str) -> str:
    """Reduce an operation name to the characters an id can safely carry."""
    kept = [c if (c.isalnum() or c in "._") else "-" for c in name.strip().lower()]
    return re.sub(r"-+", "-", "".join(kept)).strip("-")


def op_id_for(run_id: str, op_key: str) -> str:
    """Build an operation's id from the run it belongs to and its key.

    Writers and readers both derive ids this way, so the rule lives out here where
    each of them can reach it.
    """
    return f"{run_id}.{slug(op_key)}"


@dataclass(frozen=True)
class Op:
    """One operation, which maps one or more input datasets to a single output.

    Structure comes from ``input_op_ids``, which makes the operations table a
    graph in its own right, with operations as its nodes. Two operations are
    ordered relative to each other only when one fed the other, so genuinely
    parallel branches stay unordered.

    ``op_id`` derives from ``op_key``, which holds still when somebody inserts a
    step upstream. Comparing one operation across runs goes through ``op_key``,
    since ``op_id`` carries ``run_id`` and so differs every run by design.
    """

    op_id: str
    run_id: str
    op_key: str
    description: str
    timestamp: datetime
    code_ref: Optional[str] = None
    code_version: Optional[str] = None
    output_key_columns: Optional[Tuple[str, ...]] = None
    identity_capture_status: IdentityCaptureStatus = IdentityCaptureStatus.PARTIAL
    physical_source: Optional[str] = None
    physical_dest: Optional[str] = None
    input_op_ids: Tuple[str, ...] = ()
    passthrough_input_op_ids: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("input_op_ids", "passthrough_input_op_ids"):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        if self.output_key_columns is not None:
            object.__setattr__(
                self, "output_key_columns", tuple(self.output_key_columns)
            )


def op_type_of(op: Op) -> OpType:
    reads = op.physical_source is not None
    writes = op.physical_dest is not None
    if reads and writes:
        return OpType.COPY
    if reads:
        return OpType.READ
    if writes:
        return OpType.WRITE
    return OpType.TRANSFORM


def resolve_input_op_id(row: Mapping[str, Any], op: Op) -> Optional[str]:
    """Identify the source of an input ID, or return None if it is unresolved.

    Validate the entry before using the result to follow a record.
    """
    if row.get("input_id") is None:
        return None
    if row.get("input_op_id") is not None:
        return row["input_op_id"]
    return op.input_op_ids[0] if len(op.input_op_ids) == 1 else None


def keeps_grain_of(op: Op, inputs: Iterable[Op]) -> Tuple[Op, ...]:
    """Which of an operation's inputs are keyed the same way as its output.

    A compatibility check, useful for questioning a pass-through declaration that
    names an input keyed differently from the output. Matching key columns say how
    to identify a record, and never whether that record belongs in the output, so
    an operation says which of its inputs a reader may draw conclusions about in
    ``passthrough_input_op_ids`` and nothing here substitutes for that. Two tables
    keyed by id routinely play different roles in one join.

    Comparing key column names is also loose in both directions. A rename that
    leaves identity alone drops an input from this result, and a group-by on the
    same column keeps the name while changing what one record means.
    """
    return tuple(i for i in inputs if i.output_key_columns == op.output_key_columns)


@dataclass(frozen=True)
class Column:
    """One column of one Cairn table.

    ``type`` comes from a small vocabulary, ``string``, ``timestamp``,
    and ``string[]``, which an adapter maps onto its own type system.
    """

    name: str
    type: str
    nullable: bool
    doc: str


OPERATION_COLUMNS: Tuple[Column, ...] = (
    Column(
        "op_id",
        "string",
        False,
        "Derived from run_id and op_key, so it stays unique when Cairns from"
        " different bundles are collected. Compare one operation between runs on"
        " op_key.",
    ),
    Column(
        "run_id",
        "string",
        False,
        "The run of the bundle this Cairn lives in. Several runs make up a larger"
        " unit of work, and collecting their Cairns is how that unit gets"
        " assembled.",
    ),
    Column(
        "op_key",
        "string",
        False,
        "A descriptive slug for the operation, unique within a run.",
    ),
    Column(
        "code_ref",
        "string",
        True,
        "The fully-qualified module and function holding this operation's logic.",
    ),
    Column(
        "code_version",
        "string",
        True,
        "The revision of the code that ran, which is what tells you whether the"
        " logic moved between two runs.",
    ),
    Column(
        "output_key_columns",
        "string[]",
        True,
        "The columns this operation's output is keyed on. Row detail ids are read"
        " positionally against these.",
    ),
    Column(
        "description",
        "string",
        False,
        "What this operation did and why it happens, in prose. The why is the half"
        " a reader cannot recover from the code, and it is also the fallback for"
        " any field no row detail entry mentions.",
    ),
    Column(
        "identity_capture_status",
        "string",
        False,
        "Whether all input fates and output origins are covered by entries or"
        " pass-through rules. Values are partial (the default) and complete."
        " This does not promise complete column detail or upstream history.",
    ),
    Column(
        "physical_source",
        "string",
        True,
        "The location this operation read. An operation reading several locations"
        " is split into one read apiece.",
    ),
    Column(
        "physical_dest",
        "string",
        True,
        "The location this operation wrote. An operation writing several locations"
        " is split into one write apiece.",
    ),
    Column(
        "input_op_ids",
        "string[]",
        True,
        "The operations whose output this one consumed.",
    ),
    Column(
        "passthrough_input_op_ids",
        "string[]",
        True,
        "Inputs whose records survive under the same IDs unless entries say"
        " otherwise. Absence implies survival only with complete identity capture."
        " With complete capture, an absent entry for any other input means no"
        " direct contribution. With partial capture, absence means unknown.",
    ),
    Column(
        "timestamp",
        "timestamp",
        False,
        "When this operation was logged, which is not always when it ran.",
    ),
)

ROW_DETAIL_COLUMNS: Tuple[Column, ...] = (
    Column("op_id", "string", False, "The operation this entry belongs to."),
    Column(
        "input_op_id",
        "string",
        True,
        "Which of the operation's inputs this entry's input_id belongs to, and so"
        " which output_key_columns to read it against. Null when the operation has"
        " one input, since there is nothing to disambiguate.",
    ),
    Column(
        "kind", "string", False, "The fact recorded about a record or contribution."
    ),
    Column(
        "input_id",
        "string[]",
        True,
        "Identity on the way in, absent for a mint. Positional against the input"
        " operation's output_key_columns.",
    ),
    Column(
        "output_id",
        "string[]",
        True,
        "Identity on the way out, absent for a drop. Positional against this"
        " operation's output_key_columns.",
    ),
    Column(
        "affected_output_columns",
        "string[]",
        True,
        "Which output columns this entry concerns. Null means no column detail"
        " was recorded; claiming all columns requires listing them."
        " Under derived_from these are the columns the input supplied, under"
        " content_changed the ones whose values changed, and under flagged the ones"
        " being called out. An empty list is invalid.",
    ),
    Column(
        "column_change",
        "string",
        True,
        "What happened to those columns' values. Only a content_changed entry can"
        " carry one, and null there means the fate went unstated.",
    ),
    Column("detail", "string", True, "How this particular record was changed."),
)


def op_row(op: Op) -> Dict[str, Any]:
    """Flatten an operation into the row shape ``OPERATION_COLUMNS`` describes.

    Every adapter renders operations through this, so two adapters writing the
    same run produce the same rows.
    """
    row: Dict[str, Any] = {}
    for column in OPERATION_COLUMNS:
        value = getattr(op, column.name)
        if isinstance(value, Enum):
            value = value.value
        row[column.name] = list(value) if isinstance(value, tuple) else value
    return row


def row_detail_row(
    op_id: str,
    kind: Kind,
    *,
    input_op_id: Optional[str] = None,
    input_id: Optional[Sequence[str]] = None,
    output_id: Optional[Sequence[str]] = None,
    affected_output_columns: Optional[Sequence[str]] = None,
    column_change: Optional[ColumnChange] = None,
    detail: Optional[str] = None,
) -> Dict[str, Any]:
    """Build one row detail entry in the shape ``ROW_DETAIL_COLUMNS`` describes.

    Convenience for tests and for adapters assembling small sets by hand. An
    adapter working at scale builds these as columns instead, in which case this
    is the reference for key order and value types.
    """
    return {
        "op_id": op_id,
        "input_op_id": input_op_id,
        "kind": kind.value if isinstance(kind, Kind) else kind,
        "input_id": list(input_id) if input_id is not None else None,
        "output_id": list(output_id) if output_id is not None else None,
        "affected_output_columns": list(affected_output_columns)
        if affected_output_columns is not None
        else None,
        "column_change": column_change.value
        if isinstance(column_change, ColumnChange)
        else column_change,
        "detail": detail,
    }


#: Every field of :class:`Op`, for checking the type and the schema agree.
OP_FIELD_NAMES: Tuple[str, ...] = tuple(f.name for f in fields(Op))
