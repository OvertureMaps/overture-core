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
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

#: The two tables a Cairn is made of. Fixed, because collecting Cairns from
#: different adapters means finding the same two names in each.
OPERATIONS = "operations"
ROW_DETAIL = "row_detail"


class Kind(Enum):
    """What happened to the ids one operation handled.

    The value follows from three questions: is there an input id, is there an
    output id, and if there are both, are they equal. That makes this set
    exhaustive over identity outcomes, which is the whole of what it claims to
    describe.

    A merge, a split, and a rebind are all shapes of a set of ``DERIVED_FROM``
    entries, so none of them appears here. Within one operation, one input
    reaching one output is a rebind, several sharing an ``output_id`` are a merge,
    and several sharing an ``input_id`` are a split. Reading the shape off a group
    saves a caller from naming it, and it lets a many-to-many stay one thing that
    answers to one name.
    """

    DROPPED = "dropped"
    MINTED = "minted"
    DERIVED_FROM = "derived_from"
    CONTENT_CHANGED = "content_changed"
    FLAGGED = "flagged"


class ColumnChange(Enum):
    """What happened to the values in an entry's ``affected_output_columns``.

    This is :class:`Kind` asked one grain down, about a single cell. ``SET`` is a
    mint, ``CLEARED`` is a drop, and ``REPLACED`` is a content change.
    The cell-grain equivalent of ``DERIVED_FROM`` would name which other column a
    value came from, and that stays in ``detail`` prose, because the logic behind
    such a choice does not fit a closed schema.

    Only a ``CONTENT_CHANGED`` entry can carry one, since that is the only kind
    whose record has both a before and an after.
    """

    SET = "set"
    REPLACED = "replaced"
    CLEARED = "cleared"


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
    has_row_detail: bool = False
    physical_source: Optional[str] = None
    physical_dest: Optional[str] = None
    input_op_ids: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_op_ids", tuple(self.input_op_ids))
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


@dataclass(frozen=True)
class Column:
    """One column of one Cairn table.

    ``type`` comes from a small vocabulary, ``string``, ``bool``, ``timestamp``,
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
    Column("description", "string", False, "What this operation did, in prose."),
    Column(
        "has_row_detail",
        "bool",
        False,
        "Whether this operation may have entries in the row detail table.",
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
    Column("kind", "string", False, "What happened to the ids."),
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
        "Which output columns this entry concerns. Null means all of them.",
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
