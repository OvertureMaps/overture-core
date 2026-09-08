"""The rules a Cairn has to satisfy.

Operation rules run here, over objects, because one run holds few operations.

Every row detail rule holds or fails for one entry on its own, so an adapter can
express the whole set as a filter over its table without gathering or counting
anything. :func:`check_row_detail_row` is the reference semantics, written against
a plain mapping so an adapter can either port it to column expressions or call it
directly on a small set. Whether a group of entries is a merge, a split, or a
many-to-many is read off by grouping them, and since nobody declares those shapes,
nobody can declare them wrongly.

Keeping the checks out of the dataclasses lets one code path raise in a test run
and report in a production one.
"""

from __future__ import annotations

from typing import Any, Collection, Iterable, List, Mapping, Optional

from overture_cairn.core.errors import Problems
from overture_cairn.core.model import (
    ENDPOINTS,
    SAME_ID_KINDS,
    WHOLE_RECORD_KINDS,
    ColumnChange,
    Kind,
    Op,
    OpType,
    op_type_of,
)


def check_operation(op: Op, problems: Problems) -> None:
    """Check one operation on its own.

    Every operation needs a description, since an entry nobody can read is not
    lineage. The rest of these tie an operation's shape to its inputs: a read
    starts a branch and so consumes no operation, while a write or a copy carries
    exactly the one input whose output it is putting somewhere.
    """
    if not op.description or not op.description.strip():
        problems.report(
            "op.description",
            "every operation needs a description, including one that touches no records",
            op.op_id,
        )

    shape = op_type_of(op)
    if shape is OpType.READ and op.input_op_ids:
        problems.report(
            "op.input_op_ids",
            "a read draws from a location, so it consumes no operation",
            op.op_id,
        )
    if shape in (OpType.WRITE, OpType.COPY) and len(op.input_op_ids) != 1:
        problems.report(
            "op.input_op_ids",
            f"a {shape.value} puts one operation's output somewhere, so it takes"
            f" exactly one input. This one has {len(op.input_op_ids)}.",
            op.op_id,
        )
    if shape is not OpType.TRANSFORM and op.has_row_detail:
        problems.report(
            "op.has_row_detail",
            f"a {shape.value} moves records without touching their contents, so it"
            " has no row detail",
            op.op_id,
        )


def check_operations(ops: Iterable[Op], problems: Problems) -> Problems:
    """Check a whole operations table.

    Beyond the per-operation rules, this covers the three that need the rest of the
    table: ids are unique, every input names an operation the table holds, and an
    operation that wrote somewhere is a leaf. That last one is what makes a
    materialized handoff visible. Anything downstream of a write reaches it by
    reading the same location back, which is the same move a Cairn in another
    bundle makes, so one rule covers a round trip inside a run and a handoff
    between runs.

    Cycles are checked separately by :func:`check_acyclic`.
    """
    ops = list(ops)
    known = set()
    for op in ops:
        if op.op_id in known:
            problems.report(
                "op.op_id",
                f"another operation is already keyed {op.op_key!r}; the key is the id",
                op.op_id,
            )
        known.add(op.op_id)

    wrote = {op.op_id for op in ops if op.physical_dest is not None}
    for op in ops:
        check_operation(op, problems)
        for parent in op.input_op_ids:
            if parent not in known:
                problems.report(
                    "op.input_op_ids",
                    f"input {parent} is not an operation in this Cairn",
                    op.op_id,
                )
            elif parent in wrote:
                problems.report(
                    "op.input_op_ids",
                    f"input {parent} wrote to a location, so read that location back"
                    " to reach its data",
                    op.op_id,
                )
    return problems


_WHITE, _GREY, _BLACK = 0, 1, 2


def check_acyclic(ops: Iterable[Op], problems: Problems) -> Problems:
    """Check that the input links form a DAG.

    Keyed by op_key, an operation can name an input declared after it, so nothing
    rules a cycle out structurally. A self-reference is a cycle of one and gets
    reported here, so this check stands on its own.

    Inputs naming an operation outside the Cairn are skipped, since
    :func:`check_operations` reports those.
    """
    inputs = {op.op_id: tuple(op.input_op_ids) for op in ops}
    state = dict.fromkeys(inputs, _WHITE)
    reported: set = set()

    for root in inputs:
        if state[root] != _WHITE:
            continue
        state[root] = _GREY
        path = [root]
        walking = [iter(inputs[root])]
        while walking:
            descended = False
            for parent in walking[-1]:
                if parent not in state:
                    continue
                if state[parent] == _GREY:
                    _report_cycle(path[path.index(parent) :], problems, reported)
                elif state[parent] == _WHITE:
                    state[parent] = _GREY
                    path.append(parent)
                    walking.append(iter(inputs[parent]))
                    descended = True
                    break
            if not descended:
                state[path.pop()] = _BLACK
                walking.pop()
    return problems


def _report_cycle(cycle: List[str], problems: Problems, reported: set) -> None:
    """Report a cycle once, however many entry points reach it.

    Rotating to start at the lowest id gives one spelling per cycle, so the same
    defect reads the same way from run to run.
    """
    start = cycle.index(min(cycle))
    canonical = tuple(cycle[start:] + cycle[:start])
    if canonical in reported:
        return
    reported.add(canonical)
    trail = " -> ".join(canonical + (canonical[0],))
    problems.report(
        "op.input_op_ids", f"input links form a cycle: {trail}", canonical[0]
    )


def check_row_detail_row(
    row: Mapping[str, Any], input_op_ids: Optional[Collection[str]] = None
) -> List[str]:
    """Return the names of the rules one row detail entry breaks.

    Reference semantics for the row rules, over one entry as a mapping keyed the
    way ``ROW_DETAIL_COLUMNS`` names things. An empty list means the entry is well
    formed.

    Pass ``input_op_ids`` from the entry's operation to also check that the entry
    came in on a stream that operation consumed. It is a handful of values, so an
    adapter can broadcast it and keep this a row rule.
    """
    broken: List[str] = []

    kind = _as_kind(row.get("kind"))
    if kind is None:
        return ["row.kind"]

    input_id, output_id = row.get("input_id"), row.get("output_id")
    wants_input, wants_output = ENDPOINTS[kind]
    if (input_id is not None) is not wants_input:
        broken.append("row.input_id")
    if (output_id is not None) is not wants_output:
        broken.append("row.output_id")

    if input_id is not None and output_id is not None:
        same = list(input_id) == list(output_id)
        if kind in SAME_ID_KINDS and not same:
            broken.append("row.same_ids")
        if kind is Kind.DERIVED_FROM and same:
            broken.append("row.derived_from_ids")

    broken.extend(_check_input_op_id(row, kind, input_op_ids))
    broken.extend(_check_columns(row, kind))
    return broken


def _check_input_op_id(
    row: Mapping[str, Any], kind: Kind, input_op_ids: Optional[Collection[str]]
) -> List[str]:
    """Check the pointer that says which input an entry's input_id belongs to.

    A mint has no input side to attribute, so it carries no pointer. Otherwise the
    pointer is present exactly when the operation had more than one input, since
    one input needs no disambiguating and several are ambiguous without it.
    """
    broken: List[str] = []
    input_op_id = row.get("input_op_id")

    if kind is Kind.MINTED and input_op_id is not None:
        broken.append("row.input_op_id")
        return broken

    if input_op_ids is None:
        return broken

    if input_op_id is not None and input_op_id not in input_op_ids:
        broken.append("row.input_op_id")
        return broken

    if kind is not Kind.MINTED:
        needed = len(input_op_ids) > 1
        if (input_op_id is not None) is not needed:
            broken.append("row.input_op_id")
    return broken


def _check_columns(row: Mapping[str, Any], kind: Kind) -> List[str]:
    broken: List[str] = []
    columns = row.get("affected_output_columns")
    change = row.get("column_change")

    if columns is not None and not columns:
        broken.append("row.affected_output_columns")
    if kind in WHOLE_RECORD_KINDS and columns is not None:
        broken.append("row.affected_output_columns")

    if change is not None:
        if kind is not Kind.CONTENT_CHANGED:
            broken.append("row.column_change")
        elif _as_column_change(change) is None:
            broken.append("row.column_change")
    return broken


def _as_kind(value: Any) -> Optional[Kind]:
    if isinstance(value, Kind):
        return value
    try:
        return Kind(value)
    except ValueError:
        return None


def _as_column_change(value: Any) -> Optional[ColumnChange]:
    if isinstance(value, ColumnChange):
        return value
    try:
        return ColumnChange(value)
    except ValueError:
        return None
