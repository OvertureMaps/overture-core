"""`BundleComponent` and the standard components bundle types declare."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

__all__ = [
    "ATTRIBUTION",
    "BRIDGEFILES",
    "CHANGELOG",
    "CHANGELOG_INTERNAL",
    "DATA",
    "DIFF_REPORT",
    "LICENSE",
    "METADATA",
    "METRICS",
    "METRICS_SUMMARY",
    "PARTITION_BBOXES",
    "PMTILES",
    "PROPAGATE",
    "REGISTRY",
    "RELEASED",
    "SUCCESS",
    "SUMMARY",
    "VALIDATION",
    "BundleComponent",
    "ComponentKind",
]


class ComponentKind(StrEnum):
    """What kind of entry a `BundleComponent` is."""

    DIRECTORY = "directory"
    FILE = "file"
    MARKER = "marker"  # an empty file whose presence records a state


@dataclass(frozen=True)
class BundleComponent:
    """An entry directly under a bundle's base path, e.g. ``data/`` or ``success``.

    Each type declares its standard components in ``COMPONENTS``; whether one
    exists depends on the bundle.
    """

    name: str
    kind: ComponentKind = ComponentKind.DIRECTORY

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name:
            raise ValueError(
                f"Component name must be one non-empty path segment, got {self.name!r}"
            )
        object.__setattr__(self, "kind", ComponentKind(self.kind))

    @property
    def is_directory(self) -> bool:
        """Whether the component is a directory."""
        return self.kind is ComponentKind.DIRECTORY

    @property
    def listing_name(self) -> str:
        """The name as a delimited listing shows it, e.g. ``"data/"``."""
        return f"{self.name}/" if self.is_directory else self.name


DATA = BundleComponent("data")
METADATA = BundleComponent("metadata.json", ComponentKind.FILE)
SUCCESS = BundleComponent("success", ComponentKind.MARKER)
RELEASED = BundleComponent("released", ComponentKind.MARKER)
CHANGELOG = BundleComponent("changelog")
CHANGELOG_INTERNAL = BundleComponent("changelog_internal")
METRICS = BundleComponent("metrics")
METRICS_SUMMARY = BundleComponent("metrics_summary")
BRIDGEFILES = BundleComponent("bridgefiles")
PMTILES = BundleComponent("pmtiles")
PARTITION_BBOXES = BundleComponent("partition_bboxes")
PROPAGATE = BundleComponent("propagate")  # directories copied from input bundles
VALIDATION = BundleComponent("validation")
REGISTRY = BundleComponent("registry")
SUMMARY = BundleComponent("summary.md", ComponentKind.FILE)
DIFF_REPORT = BundleComponent("bundle_diff_report.md", ComponentKind.FILE)
LICENSE = BundleComponent("license.json", ComponentKind.FILE)
ATTRIBUTION = BundleComponent("attribution.mdx", ComponentKind.FILE)
