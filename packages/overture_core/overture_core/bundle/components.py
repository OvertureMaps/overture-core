"""`BundleComponent`, the standard components and their typed accessors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


class ComponentKind(StrEnum):
    """Whether a `BundleComponent` is a directory or a file."""

    DIRECTORY = "directory"
    FILE = "file"


@dataclass(frozen=True)
class BundleComponent:
    """An entry directly under a bundle's base path, e.g. ``data/`` or ``success``.

    Each type lists its components in ``COMPONENTS``; whether one exists
    depends on the bundle.
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


_DATA = BundleComponent("data")
_METADATA = BundleComponent("metadata.json", ComponentKind.FILE)
_SUCCESS = BundleComponent("success", ComponentKind.FILE)
_RELEASED = BundleComponent("released", ComponentKind.FILE)
_CHANGELOG = BundleComponent("changelog")
_METRICS = BundleComponent("metrics")
_METRICS_SUMMARY = BundleComponent("metrics_summary")
_BRIDGEFILES = BundleComponent("bridgefiles")
_PMTILES = BundleComponent("pmtiles")
_PARTITION_BBOXES = BundleComponent("partition_bboxes")
_PROPAGATE = BundleComponent("propagate")  # dirs copied from input bundles
_CHANGELOG_INTERNAL = BundleComponent("changelog_internal")
_VALIDATION = BundleComponent("validation")
_REGISTRY = BundleComponent("registry")
_DIFF_REPORT = BundleComponent("bundle_diff_report.md", ComponentKind.FILE)
# Written on promoted themes and release candidates.
_PUBLISHING = (
    _BRIDGEFILES,
    _PMTILES,
    _CHANGELOG,
    _METRICS,
    _METRICS_SUMMARY,
    _PARTITION_BBOXES,
    _PROPAGATE,
    BundleComponent("license.json", ComponentKind.FILE),
    BundleComponent("attribution.mdx", ComponentKind.FILE),
)


class _HasComponents(Protocol):
    def component_uri(self, name: str) -> str: ...
    def component_prefix(self, name: str) -> str: ...


class _MetricsAccessors:
    """Accessors for the ``changelog`` and metrics directories."""

    @property
    def changelog_uri(self: _HasComponents) -> str:
        return self.component_uri(_CHANGELOG.name)

    @property
    def changelog_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_CHANGELOG.name)

    @property
    def metrics_uri(self: _HasComponents) -> str:
        return self.component_uri(_METRICS.name)

    @property
    def metrics_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_METRICS.name)

    @property
    def metrics_summary_uri(self: _HasComponents) -> str:
        return self.component_uri(_METRICS_SUMMARY.name)

    @property
    def metrics_summary_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_METRICS_SUMMARY.name)


class _PublishingAccessors(_MetricsAccessors):
    """Accessors for the directories in ``_PUBLISHING``."""

    @property
    def bridgefiles_uri(self: _HasComponents) -> str:
        return self.component_uri(_BRIDGEFILES.name)

    @property
    def bridgefiles_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_BRIDGEFILES.name)

    @property
    def pmtiles_uri(self: _HasComponents) -> str:
        return self.component_uri(_PMTILES.name)

    @property
    def pmtiles_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_PMTILES.name)

    @property
    def partition_bboxes_uri(self: _HasComponents) -> str:
        return self.component_uri(_PARTITION_BBOXES.name)

    @property
    def partition_bboxes_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_PARTITION_BBOXES.name)

    @property
    def propagate_uri(self: _HasComponents) -> str:
        return self.component_uri(_PROPAGATE.name)

    @property
    def propagate_prefix(self: _HasComponents) -> str:
        return self.component_prefix(_PROPAGATE.name)
