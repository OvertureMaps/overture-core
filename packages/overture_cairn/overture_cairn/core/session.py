"""The run: what pipeline code holds while it records what it is doing.

A run holds operations and nothing else, so its memory cost tracks how many steps
a job has and stays flat as the data grows. Row detail never reaches here, and
neither does any writing. An adapter reads the finished operations off a run and
puts both tables wherever it keeps them.

The four recording methods correspond to the four operation shapes, and each one
accepts only the arguments its shape allows. A read takes a location and no input
operation, a write takes one input operation and a location, and a transform takes
inputs and no location. Going through them makes the shape rules in
:mod:`overture_cairn.core.validate` hard to break by accident, while the table
itself stays one flat schema.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from overture_cairn.core.errors import Problems
from overture_cairn.core.model import IdentityCaptureStatus, Op, op_id_for, op_row, slug
from overture_cairn.core.validate import check_acyclic, check_operations

#: An input may be given as an operation or as its id.
InputOp = Union[Op, str]

#: Tells a caller who said nothing about pass-through apart from one who said
#: nothing survives. The two want different answers, and an empty list means the
#: second.
UNSET: Any = object()


def _op_id(value: InputOp) -> str:
    return value.op_id if isinstance(value, Op) else value


def _caller_code_ref() -> Optional[str]:
    """Name the function that asked for an operation to be recorded.

    Walks out past Cairn's own frames, so the reference lands on pipeline code
    whichever recording method it arrived through.
    """
    frame = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals.get("__name__", "")
        if not module.startswith("overture_cairn"):
            return f"{module}.{frame.f_code.co_qualname}"
        frame = frame.f_back
    return None


class Run:
    """One writer's operations, held until the run is finished.

    A run is one execution of one writer, which is finer than the unit of work it
    belongs to. A bundle or a pipeline is several runs whose Cairns get collected,
    and bundle metadata links the exact data versions between runs. Every
    operation passes through this object, so a duplicate key
    cannot slip past in another process.

    Operations number in the hundreds at most, so keeping all of them costs
    nothing.
    """

    def __init__(
        self,
        run_id: str,
        *,
        strict: bool = False,
        code_version: Optional[str] = None,
        output_key_columns: Optional[Sequence[str]] = None,
    ) -> None:
        self.run_id = run_id
        self.problems = Problems(strict=strict)
        #: The revision recorded on operations that name none of their own. Most
        #: runs are one deployed commit, so naming it here keeps it off every call.
        self.code_version = code_version
        #: What operations are keyed on unless one names its own grain. Most runs
        #: work at one grain throughout.
        self.output_key_columns = (
            tuple(output_key_columns) if output_key_columns is not None else None
        )
        self.ops: List[Op] = []
        self._keys: Dict[str, Op] = {}

    def read(
        self,
        op_key: str,
        description: str,
        source: str,
        *,
        output_key_columns: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> Op:
        """Record taking records out of a location.

        A read starts a branch, so it consumes no operation. Reading a dataset
        whose records already carry ids leaves those identities alone, and the
        first operation to assert a key over them is what says what they are.
        """
        return self._record(
            op_key,
            description,
            physical_source=source,
            output_key_columns=output_key_columns,
            passthrough=(),
            **kwargs,
        )

    def write(
        self,
        op_key: str,
        description: str,
        dest: str,
        *,
        input: InputOp,
        **kwargs: Any,
    ) -> Op:
        """Record putting one operation's output in a location.

        Nothing may consume a write. Whatever comes next reaches this data by
        reading the location back, which is what keeps a materialized handoff
        visible in the record.

        A write carries every record it handles to the location and can hold no row
        detail, so its input always passes through and this fills that in.
        """
        return self._record(
            op_key,
            description,
            inputs=[input],
            passthrough=[input],
            physical_dest=dest,
            **kwargs,
        )

    def copy(
        self,
        op_key: str,
        description: str,
        source: str,
        dest: str,
        *,
        input: InputOp,
        **kwargs: Any,
    ) -> Op:
        """Record moving bytes from one location to another.

        Mirroring a release to a second bucket is a copy. The grain carries over
        from the input, since relocating records leaves them as they were, which is
        also why its input always passes through.
        """
        return self._record(
            op_key,
            description,
            inputs=[input],
            passthrough=[input],
            physical_source=source,
            physical_dest=dest,
            **kwargs,
        )

    def transform(
        self,
        op_key: str,
        description: str,
        *,
        inputs: Sequence[InputOp],
        output_key_columns: Optional[Sequence[str]] = None,
        identity_capture_status: IdentityCaptureStatus = IdentityCaptureStatus.PARTIAL,
        passthrough: Sequence[InputOp] = UNSET,
        **kwargs: Any,
    ) -> Op:
        """Record work done on records between a read and a write.

        This covers filters, joins, merges, enrichments, and checks. Use complete
        identity capture only when entries and pass-through rules account for all
        input fates and output origins.

        ``passthrough`` names the inputs whose records survive this operation under
        the same identity. With complete capture, an absent entry means the record
        came through, though its values may have changed. Pass every input for a
        filter or a union, the base table for an enrichment, the feed for a match
        against a reference set, and nothing for an aggregate.

        Saying nothing leaves it to :meth:`_default_passthrough`, which answers only
        the unambiguous case. Pass an empty list to say that nothing survives, which
        is what a group-by keyed on the column it grouped needs.
        """
        return self._record(
            op_key,
            description,
            inputs=inputs,
            output_key_columns=output_key_columns,
            identity_capture_status=identity_capture_status,
            passthrough=passthrough,
            **kwargs,
        )

    def _default_passthrough(
        self, inputs: Sequence[InputOp], output_key_columns: Optional[Tuple[str, ...]]
    ) -> Tuple[InputOp, ...]:
        """Work out which inputs survive when the caller did not say.

        One input keyed the way the output is keyed is a filter or a value rewrite
        almost every time, which is the bulk of the transforms in a pipeline and
        not worth making every author restate. Anything else returns nothing,
        because no other shape is safe to guess: two inputs keyed alike routinely
        play different roles, and an operation that rekeys its output is usually an
        aggregate.

        A group-by keyed on the column it grouped slips through this, since its key
        columns are unchanged while each output record means something new. Such an
        operation passes an empty list.
        """
        if len(inputs) != 1 or output_key_columns is None:
            return ()
        source = self._keys.get(_op_id(inputs[0]))
        if source is None or source.output_key_columns != output_key_columns:
            return ()
        return (inputs[0],)

    def _record(
        self,
        op_key: str,
        description: str,
        *,
        inputs: Sequence[InputOp] = (),
        output_key_columns: Optional[Sequence[str]] = None,
        identity_capture_status: IdentityCaptureStatus = IdentityCaptureStatus.PARTIAL,
        passthrough: Sequence[InputOp] = UNSET,
        physical_source: Optional[str] = None,
        physical_dest: Optional[str] = None,
        code_ref: Optional[str] = None,
        code_version: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> Op:
        """Build an operation, register it, and hand it back.

        Registering happens the moment the calling code says so, so the order
        operations arrive in is the order the code composed them, whatever a
        compute engine does with the work later.
        """
        # Slugged on the way in, so the stored key matches the one embedded in
        # op_id and two spellings of the same name cannot both be recorded.
        op_key = slug(op_key)
        keys = (
            tuple(output_key_columns)
            if output_key_columns is not None
            else self.output_key_columns
        )
        if physical_dest is not None and len(inputs) == 1:
            source = (
                inputs[0]
                if isinstance(inputs[0], Op)
                else self._keys.get(_op_id(inputs[0]))
            )
            if source is not None and output_key_columns is None:
                keys = source.output_key_columns
        # Resolved here rather than stored as a sentinel, so the table always says
        # outright which inputs survive and no reader has to work it out.
        if passthrough is UNSET:
            passthrough = self._default_passthrough(inputs, keys)
        op = Op(
            op_id=op_id_for(self.run_id, op_key),
            run_id=self.run_id,
            op_key=op_key,
            description=description,
            timestamp=timestamp or datetime.now(timezone.utc),
            code_ref=code_ref or _caller_code_ref(),
            code_version=code_version or self.code_version,
            output_key_columns=keys,
            identity_capture_status=identity_capture_status,
            physical_source=physical_source,
            physical_dest=physical_dest,
            input_op_ids=tuple(_op_id(i) for i in inputs),
            passthrough_input_op_ids=tuple(_op_id(i) for i in passthrough),
        )

        clash = self._keys.get(op.op_id)
        if clash is not None:
            self.problems.report(
                "op.op_key",
                f"{op_key!r} is already the key of an operation in this run; give"
                " each one a key that says which invocation it is",
                op.op_id,
            )
        self._keys[op.op_id] = op
        self.ops.append(op)
        return op

    def operation_rows(self) -> List[Dict[str, Any]]:
        """Flatten every operation into rows for an adapter to write."""
        return [op_row(op) for op in self.ops]

    def validate(self) -> Problems:
        """Check the operations table, including that its links form a DAG."""
        check_operations(self.ops, self.problems)
        check_acyclic(self.ops, self.problems)
        return self.problems

    def __len__(self) -> int:
        return len(self.ops)

    def __iter__(self) -> Iterable[Op]:
        return iter(self.ops)
