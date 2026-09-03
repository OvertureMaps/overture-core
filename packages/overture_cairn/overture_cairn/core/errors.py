"""Cairn's exception types and failure points.

Cairn watches a pipeline and must never be why one fails, so by default a problem
cairn finds with its own record is collected and logged. Strict mode raises failures
for development and tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger("overture_cairn")


class CairnError(Exception):
    """Base for anything cairn raises."""


class InvariantViolation(CairnError):
    """A rule about cairn's own record was broken. Only raised under strict mode."""


@dataclass
class Problem:
    rule: str
    message: str
    subject: str = ""

    def __str__(self) -> str:
        where = f" [{self.subject}]" if self.subject else ""
        return f"{self.rule}{where}: {self.message}"


@dataclass
class Problems:
    """Everything cairn found wrong with its own record during a run."""

    strict: bool = False
    items: List[Problem] = field(default_factory=list)

    def report(self, rule: str, message: str, subject: str = "") -> None:
        problem = Problem(rule=rule, message=message, subject=subject)
        self.items.append(problem)
        if self.strict:
            raise InvariantViolation(str(problem))
        logger.warning("cairn: %s", problem)

    def __bool__(self) -> bool:
        return bool(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def summary(self) -> str:
        if not self.items:
            return "no problems"
        return "\n".join(str(item) for item in self.items)
