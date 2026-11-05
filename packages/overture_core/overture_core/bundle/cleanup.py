"""Retention cleanup reports and the `TableDropper` hook."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


class TableDropper(Protocol):
    """Drops catalogue tables registered for the bundle at *base_prefix*.

    Returns the (fully qualified) table names dropped, or that would have
    been dropped when *dry_run* is set.
    """

    def __call__(self, bucket: str, base_prefix: str, dry_run: bool) -> list[str]: ...


@dataclass
class BundleDecision:
    """A single (kept | would_delete | error) decision entry for a bundle."""

    bundle_uri: str
    reason: str


@dataclass
class DeleteResult:
    """Result of a single `Bundle.delete()` call."""

    bundle_uri: str
    deleted_object_count: int
    deleted_tables: list[str]
    dry_run: bool


@dataclass
class CleanupReport:
    """Aggregated report from `Bundle.cleanup_all()`."""

    bundle_type: str
    dry_run: bool
    kept: list[BundleDecision] = field(default_factory=list)
    would_delete: list[BundleDecision] = field(default_factory=list)
    deleted: list[DeleteResult] = field(default_factory=list)
    errors: list[BundleDecision] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "bundle_type": self.bundle_type,
            "dry_run": self.dry_run,
            "kept": len(self.kept),
            "would_delete": len(self.would_delete),
            "deleted": len(self.deleted),
            "errors": len(self.errors),
            "objects_deleted": sum(r.deleted_object_count for r in self.deleted),
        }
