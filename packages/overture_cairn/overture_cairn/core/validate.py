"""The rules a Cairn has to satisfy.

Operation rules run here, over objects, because one run holds few operations.

Most row rules check one entry at a time. Conflicting outcomes require grouping
entries by operation, input operation, and input ID. Adapters can port these
checks to their own table expressions.

Keeping the checks out of the dataclasses lets one code path raise in a test run
and report in a production one.
"""

from __future__ import annotations

from typing import Any, Collection, Iterable, List, Mapping, Optional, Sequence

from overture_cairn.core.errors import Problems
from overture_cairn.core.model import (
    ENDPOINTS,
    SAME_ID_KINDS,
    WHOLE_RECORD_KINDS,
    ColumnChange,
    IdentityCaptureStatus,
    Kind,
    Op,
    OpType,
    op_type_of,
    resolve_input_op_id,
)

_OutcomeGroup = tuple[str, str, tuple[str, ...]]


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
    if op.identity_capture_status not in tuple(IdentityCaptureStatus):
        problems.report(
            "op.identity_capture_status",
            "identity capture status must be partial or complete",
            op.op_id,
        )

    keys = op.output_key_columns
    if keys is not None:
        if not keys:
            problems.report(
                "op.output_key_columns",
                "null already means this operation keys nothing, so an empty list"
                " says nothing",
                op.op_id,
            )
        if any(not k or not k.strip() for k in keys):
            problems.report(
                "op.output_key_columns", "a key column needs a name", op.op_id
            )
        if len(set(keys)) != len(keys):
            problems.report(
                "op.output_key_columns",
                f"{list(keys)} names a column twice, so an id read against it would"
                " be ambiguous",
                op.op_id,
            )
    elif op.identity_capture_status == IdentityCaptureStatus.COMPLETE:
        problems.report(
            "op.output_key_columns",
            "complete identity capture needs keys that identify output records",
            op.op_id,
        )

    _check_passthrough(op, shape, problems)


def _check_passthrough(op: Op, shape: OpType, problems: Problems) -> None:
    """Check which inputs an operation lets an absent entry speak for.

    Only structural rules live here. Whether records genuinely survive an operation
    is the author's claim, and nothing in the record can confirm it.

    A write and a copy carry every record they handle through to a location and can
    hold no row detail, so their one input is always the answer and the helpers
    fill it in. A read draws from a location and consumes no operation, so it has
    nothing to name.
    """
    named = op.passthrough_input_op_ids
    if len(set(named)) != len(named):
        problems.report(
            "op.passthrough_input_op_ids",
            f"{list(named)} names an input twice",
            op.op_id,
        )
    for input_op_id in named:
        if input_op_id not in op.input_op_ids:
            problems.report(
                "op.passthrough_input_op_ids",
                f"{input_op_id} is not an input of this operation, so records"
                " cannot pass through from it",
                op.op_id,
            )

    if shape in (OpType.WRITE, OpType.COPY) and set(named) != set(op.input_op_ids):
        problems.report(
            "op.passthrough_input_op_ids",
            f"a {shape.value} carries every record it handles to a location, so it"
            " passes its one input through",
            op.op_id,
        )
    if shape is OpType.READ and named:
        problems.report(
            "op.passthrough_input_op_ids",
            "a read consumes no operation, so it has no input to pass records"
            " through from",
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
    by_id = {op.op_id: op for op in ops}
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
            if parent in by_id:
                _check_input_keys(op, by_id[parent], problems)
    return problems


def _check_input_keys(op: Op, source: Op, problems: Problems) -> None:
    if (
        op.identity_capture_status == IdentityCaptureStatus.COMPLETE
        and not source.output_key_columns
    ):
        problems.report(
            "op.input_key_columns",
            f"complete identity capture needs record keys on input {source.op_id}",
            op.op_id,
        )
    if (
        op.physical_dest is not None
        and op.output_key_columns != source.output_key_columns
    ):
        problems.report(
            "op.output_key_columns",
            "a write or copy must keep its input's key columns",
            op.op_id,
        )


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


def check_row_detail(
    rows: Iterable[Mapping[str, Any]], ops: Iterable[Op], problems: Problems
) -> Problems:
    """Check row detail entries against the operations they name.

    IDs need keys on their source and output operations. A dropped record cannot
    also survive under the same ID in this operation.

    simplification: Outcome groups must fit in memory. Adapters use joins and
    grouped table checks for larger datasets.
    """
    by_id = {op.op_id: op for op in ops}
    outcomes: dict[_OutcomeGroup, set[Kind]] = {}
    derived_inputs: set[_OutcomeGroup] = set()

    for row in rows:
        op_id = row.get("op_id")
        op = by_id.get(op_id)
        if op is None:
            problems.report(
                "row.op_id", f"no operation in this Cairn is {op_id}", str(op_id)
            )
            continue

        shape = op_type_of(op)
        if shape is not OpType.TRANSFORM:
            problems.report(
                "row.op_id",
                f"{op.op_key} is a {shape.value}, which touches no record contents,"
                " so it has no row detail",
                op.op_id,
            )
        broken = check_row_detail_row(row, op.input_op_ids, op.output_key_columns)
        for rule in broken:
            problems.report(
                rule,
                f"a {row.get('kind')} entry for {row.get('input_id')} to"
                f" {row.get('output_id')} breaks this rule",
                op.op_id,
            )

        if row.get("output_id") is not None and op.output_key_columns is None:
            problems.report(
                "op.output_key_columns",
                "an output ID needs key columns on its operation",
                op.op_id,
            )

        if broken:
            continue
        _check_input_key(row, op, by_id, problems)
        source_id = resolve_input_op_id(row, op)
        if source_id is not None:
            kind = Kind(row["kind"])
            group = (op.op_id, source_id, tuple(row["input_id"]))
            if kind is Kind.DERIVED_FROM:
                derived_inputs.add(group)
            if kind is Kind.DROPPED or tuple(row["input_id"]) == tuple(
                row["output_id"]
            ):
                outcomes.setdefault(group, set()).add(kind)

    _check_grouped_outcomes(outcomes, derived_inputs, by_id, problems)
    return problems


def _check_grouped_outcomes(
    outcomes: Mapping[_OutcomeGroup, set[Kind]],
    derived_inputs: Collection[_OutcomeGroup],
    by_id: Mapping[str, Op],
    problems: Problems,
) -> None:
    for group, kinds in outcomes.items():
        op_id, source_id, input_id = group
        if Kind.DROPPED in kinds and len(kinds) > 1:
            problems.report(
                "row.conflicting_outcomes",
                f"input {source_id} record {list(input_id)} is both dropped and"
                " recorded as surviving under the same ID",
                op_id,
            )
        elif (
            by_id[op_id].identity_capture_status == IdentityCaptureStatus.COMPLETE
            and group in derived_inputs
            and Kind.DERIVED_FROM not in kinds
            and any(kind in kinds for kind in SAME_ID_KINDS)
        ):
            problems.report(
                "row.missing_retained_link",
                f"input {source_id} record {list(input_id)} survives and has"
                " contribution links, so complete capture needs its same-ID"
                " derived_from entry too",
                op_id,
            )


def _check_input_key(
    row: Mapping[str, Any],
    op: Op,
    by_id: Mapping[str, Op],
    problems: Problems,
) -> None:
    """Check an entry's input_id against the key of the operation that produced it.

    Resolve the input operation before checking its key columns.
    """
    source_id = resolve_input_op_id(row, op)
    if source_id is None:
        return
    source = by_id.get(source_id)
    if source is None:
        problems.report(
            "row.input_op_id",
            f"input {source_id} is not an operation in this Cairn",
            op.op_id,
        )
        return
    if source.output_key_columns is None:
        problems.report(
            "op.input_key_columns",
            f"an input ID needs key columns on input {source_id}",
            op.op_id,
        )
        return

    input_id = row["input_id"]
    if len(input_id) != len(source.output_key_columns):
        problems.report(
            "row.input_id",
            f"{list(input_id)} has {len(input_id)} part(s), and"
            f" {source.op_key} keys its output on"
            f" {list(source.output_key_columns)}",
            op.op_id,
        )


def check_row_detail_row(
    row: Mapping[str, Any],
    input_op_ids: Optional[Collection[str]] = None,
    output_key_columns: Optional[Collection[str]] = None,
) -> List[str]:
    """Return the names of the rules one row detail entry breaks.

    Reference semantics for the row rules, over one entry as a mapping keyed the
    way ``ROW_DETAIL_COLUMNS`` names things. An empty list means the entry is well
    formed.

    Pass ``input_op_ids`` from the entry's operation to also check that the entry
    came in on a stream that operation consumed, and ``output_key_columns`` to
    check that its ids have as many parts as the key they are read against. Both
    are a handful of values, so an adapter can broadcast them and keep this a row
    rule.
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
    for name, value in (("input_id", input_id), ("output_id", output_id)):
        if value is not None and not _is_record_id(value):
            broken.append(f"row.{name}")
    if broken:
        return broken

    if input_id is not None and output_id is not None:
        same = list(input_id) == list(output_id)
        if kind in SAME_ID_KINDS and not same:
            broken.append("row.same_ids")

    # An operation consuming nothing has no input records, so an id on the way in
    # would name a row from no dataset.
    if input_id is not None and input_op_ids is not None and not input_op_ids:
        broken.append("row.input_id")

    if output_key_columns is not None and output_id is not None:
        if len(output_id) != len(output_key_columns):
            broken.append("row.output_id")

    broken.extend(_check_input_op_id(row, kind, input_op_ids))
    broken.extend(_check_columns(row, kind))
    return broken


def _is_record_id(value: Any) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and bool(value)
        and all(isinstance(part, str) for part in value)
    )


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
    if input_op_id is not None and not isinstance(input_op_id, str):
        return ["row.input_op_id"]

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

    # A content change naming no columns and giving no detail claims that something
    # about the record moved without saying what. A flag needs neither, since the
    # operation it belongs to is the finding.
    if kind is Kind.CONTENT_CHANGED and columns is None and not row.get("detail"):
        broken.append("row.empty_content_change")
    return broken


def _as_kind(value: Any) -> Optional[Kind]:
    if isinstance(value, Kind):
        return value
    try:
        return Kind(value)
    except (TypeError, ValueError):
        return None


def _as_column_change(value: Any) -> Optional[ColumnChange]:
    if isinstance(value, ColumnChange):
        return value
    try:
        return ColumnChange(value)
    except (TypeError, ValueError):
        return None
