"""Read immediate record links for the contract examples, entirely in memory."""

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from overture_cairn import (
    OP_FIELD_NAMES,
    IdentityCaptureStatus,
    Kind,
    Op,
    Problems,
    check_acyclic,
    check_operations,
    check_row_detail,
    resolve_input_op_id,
)

Record = tuple[str, tuple[str, ...]]
Gap = tuple[str, str | None, str]


@dataclass
class Answer:
    """Keep known endpoints, missing evidence, and terminal facts together."""

    links: dict[Record, str] = field(default_factory=dict)
    gaps: set[Gap] = field(default_factory=set)
    facts: set[str] = field(default_factory=set)


class RecordReader:
    """Follow one operation at a time, without inferring column sources.

    Membership maps individual dataset identities to known presence or absence.
    Omitted identities have unknown membership, not known absence.

    simplification: Backward queries take O(n^2) time in row count. Index outcome
    groups for larger datasets.
    """

    def __init__(
        self,
        operations: Iterable[Op | Mapping[str, Any]],
        rows: Iterable[Mapping[str, Any]] = (),
        membership: Mapping[Record, bool] | None = None,
    ):
        ops = [
            op
            if isinstance(op, Op)
            else Op(**{name: op[name] for name in OP_FIELD_NAMES if name in op})
            for op in operations
        ]
        rows = list(rows)
        problems = Problems(strict=True)
        check_operations(ops, problems)
        check_acyclic(ops, problems)
        check_row_detail(rows, ops, problems)
        self.ops = {op.op_id: op for op in ops}
        self.rows = {op.op_id: [] for op in ops}
        self.membership = dict(membership or {})
        for record, present in self.membership.items():
            self._check_record(record, require_present=False)
            if type(present) is not bool:
                raise ValueError("Membership must be True or False.")
        for row in rows:
            row = dict(row, kind=Kind(row["kind"]).value)
            op = self.ops[row["op_id"]]
            self.rows[op.op_id].append(row)
            source = resolve_input_op_id(row, op)
            endpoints = []
            if source is not None:
                endpoints.append((source, tuple(row["input_id"])))
            if row["output_id"] is not None:
                endpoints.append((op.op_id, tuple(row["output_id"])))
            for record in endpoints:
                self._check_record(record)
                self.membership[record] = True

    def _check_record(self, record: Record, *, require_present: bool = True) -> None:
        op_id, record_id = record
        op = self.ops[op_id]
        if (
            not isinstance(record_id, tuple)
            or not record_id
            or not all(isinstance(part, str) for part in record_id)
            or not op.output_key_columns
            or len(record_id) != len(op.output_key_columns)
        ):
            raise ValueError(f"Invalid record identity: {record!r}.")
        if require_present and self.membership.get(record) is False:
            raise ValueError(f"Record is known absent: {record!r}.")

    def forward(self, op_id: str, record: Record) -> Answer:
        """Find outcomes for a record known to exist in a declared input."""
        self._check_record(record)
        op = self.ops[op_id]
        source, record_id = record
        if source not in op.input_op_ids:
            raise ValueError(f"{source!r} is not an input of {op_id!r}.")
        group = [
            row
            for row in self.rows[op_id]
            if resolve_input_op_id(row, op) == source
            and tuple(row["input_id"]) == record_id
        ]
        answer = Answer(
            links={
                (op_id, tuple(row["output_id"])): "recorded"
                for row in group
                if row["output_id"] is not None
            }
        )
        if any(row["kind"] == Kind.DROPPED.value for row in group):
            answer.facts.add("Dropped under the input identity.")
        if op.identity_capture_status == IdentityCaptureStatus.PARTIAL:
            answer.gaps.add((op_id, source, "Capture is partial."))
        elif not group:
            if source in op.passthrough_input_op_ids:
                answer.links[(op_id, record_id)] = "inferred"
            else:
                answer.facts.add("No direct contribution.")
        for output in answer.links:
            self._check_record(output)
        return answer

    def backward(self, record: Record) -> Answer:
        """Find predecessors for a record known to exist in an output."""
        self._check_record(record)
        op_id, record_id = record
        op = self.ops[op_id]
        answer = Answer()
        if op.physical_source is not None and not op.input_op_ids:
            answer.facts.add(f"Physical source: {op.physical_source}")
            answer.gaps.add((op_id, None, "External version link is missing."))
            return answer

        minted = False
        for row in self.rows[op_id]:
            if row["output_id"] is None or tuple(row["output_id"]) != record_id:
                continue
            source = resolve_input_op_id(row, op)
            if source is None:
                minted = True
                answer.facts.add("Minted by this operation.")
            else:
                predecessor = (source, tuple(row["input_id"]))
                if record in self.forward(op_id, predecessor).links:
                    answer.links[predecessor] = "recorded"

        if op.identity_capture_status == IdentityCaptureStatus.PARTIAL:
            answer.gaps.add((op_id, None, "Capture is partial."))
            return answer

        candidates = []
        for source in op.passthrough_input_op_ids:
            predecessor = (source, record_id)
            if self.membership.get(predecessor) is False:
                continue
            if record in self.forward(op_id, predecessor).links:
                candidates.append(predecessor)

        sole_origin = not minted and not answer.links and len(candidates) == 1
        for predecessor in candidates:
            if predecessor in answer.links:
                continue
            if self.membership.get(predecessor) is True or sole_origin:
                answer.links[predecessor] = "inferred"
            else:
                answer.gaps.add((op_id, predecessor[0], "Input membership is unknown."))
        if not minted and not answer.links and not answer.gaps:
            raise ValueError(f"Complete operation {op_id!r} cannot explain {record!r}.")
        return answer
