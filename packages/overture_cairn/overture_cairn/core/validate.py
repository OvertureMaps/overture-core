"""The rules a cairn record has to satisfy.

Operation rules run here, over objects, because a run has few operations.

Every edge rule holds or fails for one edge on its own, so an adapter can express
the whole set as a filter over its table and never gather or count anything.
:func:`check_edge_row` is the reference semantics, written against a plain mapping
so an adapter can either port it to column expressions or call it directly on a
small set. Whether a set of edges is a merge, a split, or a many-to-many is read
off by grouping them, and since nobody declares those shapes nobody can declare
them wrongly.

Keeping the checks out of the dataclasses lets one code path raise in a test run
and report in a production one.
"""

from __future__ import annotations

from typing import Any, Collection, Iterable, List, Mapping, Optional

from overture_cairn.core.errors import Problems
from overture_cairn.core.model import (
    ENDPOINTS,
    VALUE_MOVING,
    ColumnChange,
    EdgeKind,
    Op,
    RecordsCaptured,
)


def check_op(op: Op, problems: Problems) -> None:
    """Check one operation.

    An operation needs a reason, including one that touches no identities. A method
    is present exactly when something was captured. Nothing is its own parent.
    """
    if not op.reason or not op.reason.strip():
        problems.report(
            "op.reason",
            "every operation needs a reason, including one that touches no identities",
            op.op_id,
        )
    method = op.recording_method
    if op.records_captured is RecordsCaptured.NOT_APPLICABLE and method is not None:
        problems.report(
            "op.recording_method",
            f"no records were captured, so there is no account for {method.value} to describe",
            op.op_id,
        )
    if op.records_captured is not RecordsCaptured.NOT_APPLICABLE and method is None:
        problems.report(
            "op.recording_method",
            f"a {op.records_captured.value} operation must say where its account came from",
            op.op_id,
        )
    if op.op_id in op.parent_op_ids:
        problems.report(
            "op.parent_op_ids", "an operation cannot be its own parent", op.op_id
        )


def check_run(ops: Iterable[Op], problems: Problems) -> Problems:
    """Check a whole run: every operation on its own, distinct names, and parents
    that point at operations the run actually contains.

    Cycles are not checked here. See :func:`check_acyclic`.
    """
    ops = list(ops)
    known = set()
    for op in ops:
        if op.op_id in known:
            problems.report(
                "run.name",
                f"another operation is already called {op.name!r}; names are the id",
                op.op_id,
            )
        known.add(op.op_id)

    for op in ops:
        check_op(op, problems)
        for parent in op.parent_op_ids:
            if parent not in known:
                problems.report(
                    "op.parent_op_ids",
                    f"parent {parent} is not an operation in this run",
                    op.op_id,
                )
    return problems


_WHITE, _GREY, _BLACK = 0, 1, 2


def check_acyclic(ops: Iterable[Op], problems: Problems) -> Problems:
    """Check that the parent links form a DAG.

    Keyed by name, an operation can name a parent declared after it, so nothing
    rules a cycle out structurally. Intended for test runs, where a cycle should
    stop the build.

    Parents that name an operation outside the run are skipped, since
    :func:`check_run` reports those. A self-parent is a cycle of one and is
    reported here too, so this stands on its own.
    """
    parents = {op.op_id: tuple(op.parent_op_ids) for op in ops}
    state = dict.fromkeys(parents, _WHITE)
    reported: set = set()

    for root in parents:
        if state[root] != _WHITE:
            continue
        state[root] = _GREY
        path = [root]
        walking = [iter(parents[root])]
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
                    walking.append(iter(parents[parent]))
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
        "op.parent_op_ids", f"parent links form a cycle: {trail}", canonical[0]
    )


def check_edge_row(
    row: Mapping[str, Any], parent_op_ids: Optional[Collection[str]] = None
) -> List[str]:
    """Return the names of the row rules a single edge breaks.

    Reference semantics for the row rules, over one edge as a mapping keyed the way
    ``EDGE_COLUMNS`` names things. An empty list means the edge is well formed.

    Pass ``parent_op_ids`` from the edge's operation to also check that the edge
    came in on a stream that operation actually consumed. It is a handful of values,
    so an adapter can broadcast it and keep this a row rule.
    """
    broken: List[str] = []

    kind = _as_kind(row.get("kind"))
    if kind is None:
        return ["edge.kind"]

    input_op_id = row.get("input_op_id")
    if parent_op_ids is not None and input_op_id is not None:
        if input_op_id not in parent_op_ids:
            broken.append("edge.input_op_id")

    input_id, output_id = row.get("input_id"), row.get("output_id")
    wants_input, wants_output = ENDPOINTS[kind]
    if (input_id is not None) is not wants_input:
        broken.append("edge.input_id")
    if (output_id is not None) is not wants_output:
        broken.append("edge.output_id")

    if input_id is not None and output_id is not None:
        same = list(input_id) == list(output_id)
        if kind in (EdgeKind.CONTENT_CHANGE, EdgeKind.FLAGGED) and not same:
            broken.append("edge.same_ids")
        if kind is EdgeKind.DERIVED_FROM and same:
            broken.append("edge.derived_from_ids")

    columns = row.get("columns")
    change = row.get("column_change")
    if kind in VALUE_MOVING and (columns is None) is not (change is None):
        broken.append("edge.column_change")
    if kind not in VALUE_MOVING and change is not None:
        broken.append("edge.column_change")
    if columns is not None and not columns:
        broken.append("edge.columns")
    if change is not None and _as_column_change(change) is None:
        broken.append("edge.column_change")

    # A content change with nothing named and nothing said asserts that something
    # moved without saying what, which is not worth a row. A flag with no detail is
    # a complete statement, because the operation is the finding.
    if kind is EdgeKind.CONTENT_CHANGE and columns is None and not row.get("detail"):
        broken.append("edge.empty_content_change")

    return broken


def _as_kind(value: Any) -> Optional[EdgeKind]:
    if isinstance(value, EdgeKind):
        return value
    try:
        return EdgeKind(value)
    except ValueError:
        return None


def _as_column_change(value: Any) -> Optional[ColumnChange]:
    if isinstance(value, ColumnChange):
        return value
    try:
        return ColumnChange(value)
    except ValueError:
        return None
