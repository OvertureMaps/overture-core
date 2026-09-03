"""The run: what pipeline code holds while it reports what it is doing.

A run holds operations and nothing else, so its memory cost does not depend on how
much data passed through it. Edges never reach it, and neither does any writing:
an adapter reads the finished operations off a run and puts both tables wherever
it keeps them.

Outline only. Bodies are not written yet.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from overture_cairn.core.errors import Problems
from overture_cairn.core.model import IdSpec, Op, RecordingMethod, RecordsCaptured

#: Tells the difference between a caller saying nothing about parents and a caller
#: saying this operation has none. The first inherits the previous operation, so a
#: plain sequence of steps forms a chain without anyone naming anything. The second
#: starts a branch.
INHERIT = object()


class Run:
    """One writer's operations, held until the run is finished.

    A run is one execution of one writer, which is finer than whatever unit of work
    it belongs to. Anything bigger, a bundle or a pipeline, is several runs whose
    records get concatenated, and their operations link through matching locations
    because no writer can see another's ids. Scoping a run this way is what makes
    the name check below sound: every operation in a run passes through this object,
    so a duplicate name cannot slip past in another process.

    Operations number in the hundreds at most, so keeping them all costs nothing.

    Three helpers register one, and between them they reach every combination of
    ``records_captured`` and ``recording_method`` the record allows. An operation
    that touched no identities goes to :meth:`not_applicable`. The rest go to
    :meth:`declared` or :meth:`compared`, depending on how you found out what
    happened. Completeness is an argument on those two, because it varies
    independently of both.
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        *,
        strict: bool = False,
        output_id_cols: Optional[IdSpec] = None,
    ) -> None:
        self.run_id: str = run_id or ""
        self.problems = Problems(strict=strict)
        #: What operations emit unless one names its own grain. Most runs work at
        #: one grain throughout, so naming it here keeps it off every call.
        self.output_id_cols = output_id_cols
        self.ops: List[Op] = []

    def record(
        self,
        name: str,
        reason: str,
        *,
        records_captured: RecordsCaptured,
        recording_method: Optional[RecordingMethod] = None,
        parents: Any = INHERIT,
        output_id_cols: Optional[IdSpec] = None,
        physical_source: Optional[Sequence[str]] = None,
        physical_dest: Optional[Sequence[str]] = None,
    ) -> Op:
        """Register an operation and return it.

        Callers go through one of the three helpers below, which is what stops an
        operation reaching the record without saying how well it is accounted for.

        ``parents`` accepts operations or their ids. Pass it when a step consumes
        more than one lineage, and pass nothing where a step follows the one before
        it.
        """
        raise NotImplementedError

    def declared(
        self, name: str, reason: str, *, complete: bool = True, **kwargs: Any
    ) -> Op:
        """The operation says what it did to record identities.

        ``complete=False`` marks an operation that reported some of what it did and
        left the rest unaccounted for, which is how a black box gets opened a piece
        at a time. Finishing it later flips this one argument.
        """
        raise NotImplementedError

    def compared(
        self, name: str, reason: str, *, complete: bool = True, **kwargs: Any
    ) -> Op:
        """Something worked out what the operation did by comparing its ends.

        Yields what changed and never why.
        """
        raise NotImplementedError

    def not_applicable(self, name: str, reason: str, **kwargs: Any) -> Op:
        """The operation handles no records at all.

        Checking that a location exists, listing what is in one, comparing a
        schema. Each of these is a finished statement, and the record should read
        that way. There is no method to give, because nothing was captured either
        way.

        The line is whether records passed through, not whether any identity
        changed. A write handles every record it persists, so it belongs in
        :meth:`declared` with no edges, and so does a filter that dropped nothing
        this time. Both of those could have had something to say; these cannot.
        """
        raise NotImplementedError

    def validate(self) -> Problems:
        raise NotImplementedError

    def finish(self) -> Problems:
        """Check the record and close it to further operations.

        Writing is the adapter's job, so this leaves the operations on the run for
        it to collect.
        """
        raise NotImplementedError
