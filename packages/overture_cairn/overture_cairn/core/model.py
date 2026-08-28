"""The types a cairn record is made of, and the shape of the tables it becomes.

Operations are objects because a run has a countable number of them. Edges are
only a schema, because one operation can produce more edges than fit in a process,
so whatever materialises them does so as a table inside an adapter.

Nothing here writes anything. A record is two tables named by ``OPS`` and
``EDGES``, described by ``OP_COLUMNS`` and ``EDGE_COLUMNS``, and an adapter decides
what they are stored as and where they go.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple

#: The two tables a record is made of. Fixed, because concatenating records from
#: different adapters means finding the same two names in each.
OPS = "ops"
EDGES = "edges"


class EdgeKind(str, Enum):
    """What became of a record's identity.

    No caller may add to these, so a reader can match on them exhaustively. They
    say what happened to an identity and nothing about why.

    ``CONTENT_CHANGE`` is for a record that already existed and whose values moved,
    so its ``input_id`` equals its ``output_id``. ``DERIVED_FROM`` is for an output
    that exists because of its input, so the two differ.

    ``FLAGGED`` is the one kind that says nothing about identity. An operation
    asserted something about a record and left it alone, which is what a check
    does when it finds a problem without dropping the row. Its ``input_id`` equals
    its ``output_id`` like a content change, and unlike one, nothing moved. Such an
    edge needs no ``detail``, because the operation that emitted it is the finding.

    A merge, a split, and a rebind are shapes of a set of ``DERIVED_FROM`` edges,
    and no single edge carries one, which is why none of them appears here. Within
    one operation, one input reaching one output is a rebind, several sharing an
    ``output_id`` are a merge, several sharing an ``input_id`` are a split, and both
    at once is a many-to-many. Labelling the shape would ask a caller to restate
    what the edges already show, and a many-to-many edge would answer to two labels
    at once. That is also the test a new kind has to pass: nothing else in the
    record implies a flag, so it earns a place here.
    """

    DROP = "drop"
    MINT = "mint"
    DERIVED_FROM = "derived_from"
    CONTENT_CHANGE = "content_change"
    FLAGGED = "flagged"


class ColumnChange(Enum):
    """What happened to the columns an edge names.

    The three values are the same question asked about each end, which is what
    tells ``SET`` and ``REPLACED`` apart: they differ in what was there before.

    =================  =============  ==================
    before / after     after: empty   after: has a value
    =================  =============  ==================
    empty              no edge        ``SET``
    has a value        ``CLEARED``    ``REPLACED``
    =================  =============  ==================

    ``SET`` fills a gap and is routine. ``REPLACED`` overrides a value somebody
    supplied, which is the one worth being able to count.

    Identity and value are separate axes, so each gets its own field.
    """

    SET = "set"
    REPLACED = "replaced"
    CLEARED = "cleared"


class RecordsCaptured(Enum):
    """Whether the edges account for every record identity an operation touched.

    This is what tells a reader how to take a missing edge. Under ``COMPLETE``, a
    record with no edge passed through untouched, which is what makes the edges
    affordable as deltas. Under ``INCOMPLETE``, a missing edge means nobody knows.

    ``NOT_APPLICABLE`` belongs to an operation that handles no records at all, such
    as checking that a location exists or comparing two schemas. The test is whether
    records went through it, not whether any identity changed: a write handles every
    record it persists and a filter that dropped nothing still saw them all, so both
    are ``COMPLETE`` with no edges. Keeping those apart from the ones that never had
    records is what makes either group worth querying for.

    Nothing can confirm the claim, because cairn never sees an operation's input.
    Recording it puts the claim somewhere a reader can find and question it.
    """

    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    NOT_APPLICABLE = "not_applicable"


class RecordingMethod(Enum):
    """Where the account of an operation came from.

    ``DECLARATIVE`` means the operation said what it did. ``COMPARATIVE`` means
    something worked it out from the input and the output, which gives what changed
    and never why. A comparison is also blind to a merge or an id change, because
    all it sees is old identities disappearing and new ones appearing with nothing
    linking them.

    Both produce the same edges, so opening up a black box changes this field and
    leaves the format alone. An operation with nothing to capture has no method.
    """

    DECLARATIVE = "declarative"
    COMPARATIVE = "comparative"


@dataclass(frozen=True)
class IdSpec:
    """The column or columns that together identify a record.

    One spec covers the composite case, so no caller has to special-case a
    two-column key.

    An operation names the spec for the records it emits. The spec for what it takes
    in belongs to whichever operation produced those records, so an edge names that
    operation and reads the columns from there. A step drawing on two sources can be
    taking in two grains at once, which is what makes the indirection worth it.
    """

    columns: Tuple[str, ...]

    @classmethod
    def of(cls, *columns: str) -> "IdSpec":
        return cls(tuple(columns))

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))

    def __str__(self) -> str:
        return "+".join(self.columns)


def slug(name: str) -> str:
    """Reduce an operation name to the characters an id can safely carry."""
    kept = [c if (c.isalnum() or c in "._") else "-" for c in name.strip().lower()]
    return re.sub(r"-+", "-", "".join(kept)).strip("-")


def op_id_for(run_id: str, name: str) -> str:
    """Build an operation's id from the run it belongs to and its name.

    Both a writer and a reader derive ids the same way, so the rule lives out here
    where each of them can reach it.
    """
    return f"{run_id}.{slug(name)}"


@dataclass(frozen=True)
class Op:
    """One operation, registered when the calling code declares it.

    Structure comes from ``parent_op_ids``, which makes the operations a DAG. The
    order two of them ran in is recorded only when one fed the other, which leaves
    genuinely parallel branches unordered.

    The id comes from the name, which holds still when somebody inserts a step
    upstream. A positional id would shift every operation after the new one, so no
    id would survive an edit to the pipeline. Comparing an operation across runs
    goes through ``name``, since ``op_id`` carries ``run_id`` and so differs every
    run by design.

    ``physical_source`` and ``physical_dest`` name locations an operation read and
    wrote, one entry per dataset root. They are how intermediate scratch stays
    visible: something materialised halfway through a job and read back later is an
    operation with a destination and no effect on any identity. Which of these
    locations are boundaries is not cairn's to say, since it has no idea what lies
    outside a run.
    """

    op_id: str
    run_id: str
    name: str
    reason: str
    records_captured: RecordsCaptured
    parent_op_ids: Tuple[str, ...] = ()
    recording_method: Optional[RecordingMethod] = None
    output_id_cols: Optional[IdSpec] = None
    physical_source: Optional[Tuple[str, ...]] = None
    physical_dest: Optional[Tuple[str, ...]] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "parent_op_ids", tuple(self.parent_op_ids))
        for name in ("physical_source", "physical_dest"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, tuple(value))


#: Which ends an edge of each kind carries, as ``(input, output)``.
ENDPOINTS: Mapping[EdgeKind, Tuple[bool, bool]] = {
    EdgeKind.DROP: (True, False),
    EdgeKind.MINT: (False, True),
    EdgeKind.DERIVED_FROM: (True, True),
    EdgeKind.CONTENT_CHANGE: (True, True),
    EdgeKind.FLAGGED: (True, True),
}

#: Kinds that move values, and so must say how in ``column_change`` whenever they
#: name columns. A flag can name the column it is about without one, because
#: nothing about that column changed.
VALUE_MOVING: Tuple[EdgeKind, ...] = (EdgeKind.CONTENT_CHANGE, EdgeKind.DERIVED_FROM)


@dataclass(frozen=True)
class Column:
    """One column of one cairn table.

    ``type`` is drawn from a small vocabulary, ``string``, ``int``, and
    ``string[]``, which an adapter maps onto its own type system.
    """

    name: str
    type: str
    nullable: bool
    doc: str


OP_COLUMNS: Tuple[Column, ...] = (
    Column(
        "run_id",
        "string",
        False,
        "One writer's execution. Several runs make up a larger unit of work, and"
        " concatenating their records is how that unit gets assembled.",
    ),
    Column(
        "op_id",
        "string",
        False,
        "Derived from run_id and name, so it is unique across runs and records"
        " concatenate safely. Compare an operation between runs on name.",
    ),
    Column(
        "parent_op_ids",
        "string[]",
        True,
        "The operations whose output this one consumed.",
    ),
    Column(
        "name", "string", False, "What the operation is called. Unique within a run."
    ),
    Column("reason", "string", False, "What the operation is for, in prose."),
    Column(
        "records_captured",
        "string",
        False,
        "Whether the edges account for every identity this operation touched.",
    ),
    Column(
        "recording_method",
        "string",
        True,
        "Where the account came from. Null when there was nothing to capture.",
    ),
    Column(
        "output_id_cols",
        "string[]",
        True,
        "Columns identifying the records this operation emits.",
    ),
    Column(
        "physical_source",
        "string[]",
        True,
        "Locations this operation read, one entry per dataset root and never per"
        " partition. Which records it took from them is what the edges are for.",
    ),
    Column(
        "physical_dest",
        "string[]",
        True,
        "Locations this operation wrote, one entry per dataset root.",
    ),
)

EDGE_COLUMNS: Tuple[Column, ...] = (
    Column("op_id", "string", False, "The operation this edge belongs to."),
    Column(
        "input_op_id",
        "string",
        True,
        "The operation that produced this edge's input record. One of the operation's"
        " parents, and what says how to read input_id.",
    ),
    Column("kind", "string", False, "What became of the identity."),
    Column("input_id", "string[]", True, "Identity on the way in, absent for a mint."),
    Column(
        "output_id", "string[]", True, "Identity on the way out, absent for a drop."
    ),
    Column(
        "columns",
        "string[]",
        True,
        "Which columns the edge is about. Null means the whole record, and null is"
        " right unless more than one donor could have supplied the value.",
    ),
    Column(
        "column_change",
        "string",
        True,
        "How those columns changed. Set with columns for a kind that moves values,"
        " and null for a flag, which names a column without changing it.",
    ),
    Column("detail", "string", True, "Why, for this particular record."),
)


def op_row(op: Op) -> Dict[str, Any]:
    """Flatten an operation into the row shape ``OP_COLUMNS`` describes.

    An adapter renders every operation through this, so two adapters writing the
    same run produce the same rows.
    """
    return {
        "run_id": op.run_id,
        "op_id": op.op_id,
        "parent_op_ids": list(op.parent_op_ids),
        "name": op.name,
        "reason": op.reason,
        "records_captured": op.records_captured.value,
        "recording_method": op.recording_method.value if op.recording_method else None,
        "output_id_cols": list(op.output_id_cols.columns)
        if op.output_id_cols
        else None,
        "physical_source": list(op.physical_source) if op.physical_source else None,
        "physical_dest": list(op.physical_dest) if op.physical_dest else None,
    }
