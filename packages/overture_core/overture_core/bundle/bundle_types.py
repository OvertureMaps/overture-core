"""Concrete bundle types, one per pipeline stage."""

from __future__ import annotations

import datetime
import json
from collections.abc import Collection
from dataclasses import replace
from typing import Self
from urllib.parse import urlparse

from overture_core.bundle.base import Bundle, _with_store
from overture_core.bundle.components import (
    _CHANGELOG,
    _CHANGELOG_INTERNAL,
    _DIFF_REPORT,
    _METRICS,
    _METRICS_SUMMARY,
    _PUBLISHING,
    _REGISTRY,
    _RELEASED,
    _SUCCESS,
    _VALIDATION,
    BundleComponent,
    ComponentKind,
    _MetricsAccessors,
    _PublishingAccessors,
)
from overture_core.bundle.partitions import _compose_root, _resolve_partitions
from overture_core.bundle.status import BundleStatus, BundleStatusReport
from overture_core.bundle.store import BundleStore
from overture_core.cloud.aws.object import parse_s3_uri


class _ProviderResourceBundle(Bundle):
    """Source data partitioned by provider, resource, version and run.

    Path structure: ``{ROOT_PATH}/provider={provider}/resource={resource}/version={version}/run={run}``

    Parameters
    ----------
    provider : str
        Data provider name (e.g. ``"esri"``, ``"microsoft"``).
    resource : str
        Resource type (e.g. ``"building"``).
    bucket : str
        S3 bucket name.
    version : str
        Snapshot datestamp (e.g. ``"2026-01-01"``). May be set later.
    run : str
        Run identifier. May be set later.
    namespace : str
        Optional prefix prepended to the root path.
    root_path : str | None
        Override for the root path (defaults to ``ROOT_PATH``).
    """

    PARTITION_KEYS = ("provider", "resource", "version", "run")

    def __init__(
        self,
        provider: str,
        resource: str,
        bucket: str,
        version: str = "",
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {
            "provider": provider,
            "resource": resource,
            "version": version,
            "run": run,
        }
        super().__init__(
            bucket=bucket,
            root_path=self.ROOT_PATH if root_path is None else root_path,
            partitions=partitions,
            namespace=namespace,
            store=store,
        )
        self.provider = provider
        self.resource = resource


class SourceRawBundle(_ProviderResourceBundle):
    """Raw data collected from a provider, rooted at ``datasets``."""

    BUNDLE_TYPE = "source_raw"
    ROOT_PATH = "datasets"


class SourceIngestBundle(_MetricsAccessors, _ProviderResourceBundle):
    """Ingested / normalised source data, rooted at ``feeds``."""

    BUNDLE_TYPE = "source_ingest"
    ROOT_PATH = "feeds"
    COMPONENTS = (*Bundle.COMPONENTS, _CHANGELOG, _METRICS, _METRICS_SUMMARY)

    @classmethod
    def from_path(
        cls,
        base_path: str,
        partitions: dict[str, str] | None = None,
        *,
        provider: str = "",
        resource: str = "",
        version: str = "",
        run: str = "",
        store: BundleStore | None = None,
    ) -> Self:
        """Create from a bundle-root ``s3://`` URI plus source identifiers.

        The URI path is the bundle root; unlike `Bundle.from_path`, an empty
        path falls back to ``ROOT_PATH``.  Non-empty identifier keywords
        override the same keys in *partitions*.
        """
        values = dict(partitions or {})
        identifiers = {
            "provider": provider,
            "resource": resource,
            "version": version,
            "run": run,
        }
        values.update({key: value for key, value in identifiers.items() if value})
        parsed = urlparse(base_path)
        root = parsed.path.strip("/") or cls.ROOT_PATH
        return cls._from_root(parsed.netloc, root, values, store=store)


class ThemeAssembleBundle(_MetricsAccessors, Bundle):
    """Assembled theme data from multiple corpus sources."""

    BUNDLE_TYPE = "theme_assemble"
    ROOT_PATH = "theme_stage"
    PARTITION_KEYS = ("theme", "run")
    COMPONENTS = (
        *Bundle.COMPONENTS,
        _CHANGELOG,
        _METRICS,
        _METRICS_SUMMARY,
        BundleComponent("summary.md", ComponentKind.FILE),
        _DIFF_REPORT,
    )

    def __init__(
        self,
        theme: str,
        bucket: str,
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {"theme": theme, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=self.ROOT_PATH if root_path is None else root_path,
            partitions=partitions,
            namespace=namespace,
            store=store,
        )


class ThemePromoteBundle(_PublishingAccessors, Bundle):
    """Promoted theme with supplemental artifacts."""

    BUNDLE_TYPE = "theme_promote"
    ROOT_PATH = "theme_promote"
    PARTITION_KEYS = ("theme", "schema", "run")
    COMPONENTS = (
        *Bundle.COMPONENTS,
        *_PUBLISHING,
        _CHANGELOG_INTERNAL,
        _VALIDATION,
        _DIFF_REPORT,
    )

    def __init__(
        self,
        theme: str,
        bucket: str,
        schema: str = "",
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {"theme": theme, "schema": schema, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=self.ROOT_PATH if root_path is None else root_path,
            partitions=partitions,
            namespace=namespace,
            store=store,
        )

    @property
    def changelog_internal_uri(self) -> str:
        return self.component_uri(_CHANGELOG_INTERNAL.name)

    @property
    def changelog_internal_prefix(self) -> str:
        return self.component_prefix(_CHANGELOG_INTERNAL.name)

    @property
    def validation_uri(self) -> str:
        return self.component_uri(_VALIDATION.name)

    @property
    def validation_prefix(self) -> str:
        return self.component_prefix(_VALIDATION.name)


class ReleaseCandidateBundle(_PublishingAccessors, Bundle):
    """Release candidate with supplemental artifacts.

    A published release is represented by its candidate, which then has a
    ``released`` marker and reports `BundleStatus.RELEASED`.
    """

    BUNDLE_TYPE = "release_candidate"
    ROOT_PATH = "release_candidate"
    PARTITION_KEYS = ("release", "run")
    RELEASED_MARKERS = (_SUCCESS.name, _RELEASED.name)
    COMPONENTS = (
        *Bundle.COMPONENTS,
        _RELEASED,
        *_PUBLISHING,
        _REGISTRY,
        _DIFF_REPORT,
    )

    def __init__(
        self,
        bucket: str,
        release: str = "",
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {"release": release, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=self.ROOT_PATH if root_path is None else root_path,
            partitions=partitions,
            namespace=namespace,
            store=store,
        )

    @classmethod
    def resolve_latest_released(
        cls,
        release: str,
        bucket: str,
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> ReleaseCandidateBundle:
        """Return the latest released run for *release*.

        Scans S3 for ``run=`` partitions under ``release={release}``,
        filtering to those that have both a ``success`` and a ``released``
        marker file.  Raises `PartitionResolutionError` when none is found.
        An explicit *store* is used for the scan and attached to the result.
        """
        if root_path is None:
            root_path = cls.ROOT_PATH
        scan_root = _compose_root(root_path, namespace)
        result = _resolve_partitions(
            bucket=bucket,
            root_path=scan_root,
            partition_keys=list(cls.PARTITION_KEYS),
            overrides={"release": release},
            required_markers=cls.RELEASED_MARKERS,
            store=store,
        )
        bundle = cls(
            bucket=bucket,
            release=result["release"],
            run=result["run"],
            namespace=namespace,
            root_path=root_path,
        )
        return _with_store(bundle, store)

    @property
    def release(self) -> str:
        """Convenience accessor for the ``release`` partition."""
        return self._partitions.get("release", "")

    @property
    def released_uri(self) -> str:
        """S3 URI of the ``released`` marker."""
        return self.component_uri(_RELEASED.name)

    @property
    def registry_uri(self) -> str:
        return self.component_uri(_REGISTRY.name)

    @property
    def registry_prefix(self) -> str:
        return self.component_prefix(_REGISTRY.name)

    def is_deletable(
        self,
        min_age_days: float,
        siblings: list[Bundle],
        now: datetime.datetime | None = None,
        *,
        successful: Collection[Bundle] | None = None,
    ) -> tuple[bool, str]:
        """Like `Bundle.is_deletable`, but a published candidate is always kept."""
        deletable, reason = super().is_deletable(
            min_age_days, siblings, now, successful=successful
        )
        if deletable and self._open_store().exists(*self._object_key(_RELEASED.name)):
            return False, "is a published release (has a released marker)"
        return deletable, reason

    def _status_report(self, store: BundleStore) -> BundleStatusReport:
        """Report `BundleStatus.RELEASED` for a successful, published candidate."""
        report = super()._status_report(store)
        if report.status is BundleStatus.SUCCESS and store.exists(
            *self._object_key(_RELEASED.name)
        ):
            return replace(report, status=BundleStatus.RELEASED)
        return report


class ScratchBundle(Bundle):
    """Ephemeral debug or intermediate output, labeled by purpose.

    Path structure: ``scratch/purpose={purpose}/run={run}``

    Parameters
    ----------
    purpose : str
        Short label for what this data is (e.g. ``"quality_score_debug"``).
    bucket : str
        S3 bucket name.
    run : str
        Run identifier (e.g. a timestamp) distinguishing this write from
        others with the same purpose.
    namespace : str
        Optional prefix prepended to the root path.
    root_path : str | None
        Override for the root path (defaults to ``"scratch"``).
    """

    BUNDLE_TYPE = "scratch"
    ROOT_PATH = "scratch"
    PARTITION_KEYS = ("purpose", "run")

    def __init__(
        self,
        purpose: str,
        bucket: str,
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {"purpose": purpose, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=self.ROOT_PATH if root_path is None else root_path,
            partitions=partitions,
            namespace=namespace,
            store=store,
        )


class ModelBundle(Bundle):
    """A trained model, one bundle per training run, kept indefinitely.

    Path structure: ``{root_path}/model={model}/run={run}``, with
    ``model_pointer.json`` under ``model={model}`` naming the default run.
    There is no standard root, so `discover_all` and `cleanup_all` don't
    apply.

    Parameters
    ----------
    model : str
        Model name (e.g. ``"places_quality"``).
    bucket : str
        S3 bucket name.
    root_path : str
        Root path the models are stored under.
    run : str
        Training run identifier. May be set later.
    namespace : str
        Optional prefix prepended to the root path.
    """

    BUNDLE_TYPE = "model"
    PARTITION_KEYS = ("model", "run")

    def __init__(
        self,
        model: str,
        bucket: str,
        root_path: str,
        run: str = "",
        namespace: str = "",
        *,
        store: BundleStore | None = None,
    ) -> None:
        super().__init__(
            bucket=bucket,
            root_path=root_path,
            partitions={"model": model, "run": run},
            namespace=namespace,
            store=store,
        )

    @property
    def pointer_uri(self) -> str:
        """URI of ``model_pointer.json``, which names the model's default run."""
        return f"{self.uri_through_partition('model')}/model_pointer.json"

    def read_pointer(self) -> str | None:
        """Return the run the model pointer names, or ``None`` without a pointer."""
        body = self._open_store().read(*parse_s3_uri(self.pointer_uri))
        return json.loads(body)["run"] if body else None

    def write_pointer(self) -> None:
        """Point the model at this bundle's run."""
        self._open_store().write(
            *parse_s3_uri(self.pointer_uri), json.dumps({"run": self.run}).encode()
        )
