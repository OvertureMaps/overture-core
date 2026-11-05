"""The `Bundle` base class: paths, lifecycle, partition resolution and retention."""

from __future__ import annotations

import copy
import datetime
import logging
from collections.abc import Callable, Collection
from concurrent.futures import ThreadPoolExecutor
from os import PathLike
from typing import Any, ClassVar, Self, TypeVar
from urllib.parse import urlparse

from overture_core.bundle.cleanup import (
    BundleDecision,
    CleanupReport,
    DeleteResult,
    TableDropper,
)
from overture_core.bundle.components import (
    DATA,
    METADATA,
    SUCCESS,
    BundleComponent,
)
from overture_core.bundle.metadata import (
    BundleMetadata,
    _enrich_inputs_from_dataset,
    provenance_sections,
)
from overture_core.bundle.partitions import (
    PartitionResolutionError,
    _compose_root,
    _dir_prefix,
    _natural_key,
    _NaturalKey,
    _resolve_partitions,
    _split_run_uri,
    _walk_partitions,
)
from overture_core.bundle.store import BundleStore, S3BundleStore, _store_or_default
from overture_core.bundle.status import (
    BundleStatus,
    BundleStatusReport,
    _derive_status,
    _metadata_section_statuses,
)
from overture_core.cloud.cloud import Partition
from overture_core.dataset.dataset import Dataset

log = logging.getLogger(__name__)

_BundleT = TypeVar("_BundleT", bound="Bundle")


def _with_store(bundle: _BundleT, store: BundleStore | None) -> _BundleT:
    """Attach *store* to *bundle* when given, and return the bundle.

    Assigned as an attribute so it works for subclasses whose ``__init__``
    has no ``store`` parameter.
    """
    if store is not None:
        bundle.store = store
    return bundle


_RETENTION_ORDER_PARTITION_KEYS = ("version", "release", "schema", "run")


def _run_batch(
    action: str,
    fn: Callable[[int, Bundle], None],
    bundles: list[Bundle],
    max_workers: int,
    store: BundleStore | None = None,
) -> None:
    """Run ``fn(i, target)`` for each ``bundles[i]`` across *max_workers* threads.

    Every bundle uses *store* when given, else its own; bundles without one
    share one `S3BundleStore` for the batch.  *target* is ``bundles[i]``
    when it keeps its own store, else a shallow copy carrying the batch's,
    so concurrent batches over the same bundles stay independent.  Every
    bundle is attempted.  Each failure is logged, then the first one in
    *bundles* order is re-raised.
    """
    shared = _store_or_default(store)

    def _target(b: Bundle) -> Bundle:
        if store is None and b.store is not None:
            return b
        return _with_store(copy.copy(b), shared)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [(b, pool.submit(fn, i, _target(b))) for i, b in enumerate(bundles)]
    failures = [(b, exc) for b, f in futures if (exc := f.exception()) is not None]
    for b, exc in failures:
        log.error("%s failed for %r", action, b, exc_info=exc)
    if failures:
        log.error("%s failed for %d/%d bundle(s)", action, len(failures), len(bundles))
        raise failures[0][1]


class Bundle:
    """Base data bundle on S3.

    Every bundle has a ``data`` directory, a ``metadata.json`` file, and a
    ``success`` marker.  Subclasses add components (``COMPONENTS``).

    The path is composed of ``root_path`` followed by Hive-style
    ``key=value`` segments for each entry in the ``partitions`` dict.
    Partition path properties raise ValueError while any value is empty;
    call `resolve_partitions` first.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    root_path : str
        S3 key prefix before any partition segments
        (e.g. ``"datasets"``, ``"feeds"``).
    partitions : dict[str, str] | None
        Ordered mapping of partition key to value.  Each entry becomes a
        ``key=value`` path segment.
    namespace : str
        Optional prefix prepended to ``root_path`` (``<namespace>/<root>``),
        e.g. to isolate a developer's bundles.  Empty means no prefix.
    store : BundleStore | None
        Storage backend; ``None`` opens a fresh `S3BundleStore` per operation.
    """

    BUNDLE_TYPE: str = "bundle"

    # Class-level default for subclasses that skip ``Bundle.__init__``.
    store: BundleStore | None = None

    # Bundle root before any namespace; needed for discovery, cleanup, registry.
    ROOT_PATH: str = ""

    # Partition key names, in path order.
    PARTITION_KEYS: tuple[str, ...] = ()

    # In display order; subclasses extend their parent's tuple.
    COMPONENTS: ClassVar[tuple[BundleComponent, ...]] = (DATA, METADATA, SUCCESS)

    def __init__(
        self,
        bucket: str,
        root_path: str,
        partitions: dict[str, str] | None = None,
        namespace: str = "",
        *,
        store: BundleStore | None = None,
    ) -> None:
        self.bucket = bucket
        self._partitions: dict[str, str] = dict(partitions) if partitions else {}
        self._root_path = _compose_root(root_path, namespace)
        self.store = store

    def _open_store(self) -> BundleStore:
        """This bundle's store, or a fresh `S3BundleStore` for one operation."""
        return _store_or_default(self.store)

    # ------------------------------------------------------------------
    # Partition accessors
    # ------------------------------------------------------------------

    @property
    def partitions(self) -> dict[str, str]:
        """Return a copy of the partitions dict."""
        return dict(self._partitions)

    @property
    def version(self) -> str:
        """Convenience accessor for the ``version`` partition."""
        return self._partitions.get("version", "")

    @version.setter
    def version(self, value: str) -> None:
        self._partitions["version"] = value

    @property
    def run(self) -> str:
        """Convenience accessor for the ``run`` partition."""
        return self._partitions.get("run", "")

    @run.setter
    def run(self, value: str) -> None:
        self._partitions["run"] = value

    @classmethod
    def from_path(
        cls,
        base_path: str,
        partitions: dict[str, str] | None = None,
        *,
        store: BundleStore | None = None,
    ) -> Self:
        """Create a bundle from a full ``s3://bucket/root_path`` URI.

        The URI path is the bundle root as-is (any namespace included); see
        `_from_root` for how subclasses take *partitions*.

        Parameters
        ----------
        base_path : str
            Full S3 URI of the bundle root.
        partitions : dict[str, str] | None
            Optional partition key/value pairs.
        store : BundleStore | None
            Optional storage backend for the new bundle.
        """
        parsed = urlparse(base_path)
        return cls._from_root(
            parsed.netloc, parsed.path.strip("/"), partitions, store=store
        )

    @classmethod
    def from_base_uri(cls, base_uri: str, *, store: BundleStore | None = None) -> Self:
        """Create a Bundle by parsing a full partitioned ``s3://`` URI.

        Splits Hive-style ``key=value`` segments off the end of the path
        into partitions; everything before them is the bundle root.
        ``s3://b/theme_promote/theme=places/run=X`` gives a Bundle with
        ``root_uri`` ``s3://b/theme_promote`` and partitions
        ``{"theme": "places", "run": "X"}``.

        Raises ValueError for a non-partition segment after partition
        segments (an ambiguous root), a repeated key, or keys outside the
        subclass's ``PARTITION_KEYS`` or out of their order.
        """
        parsed = urlparse(base_uri.rstrip("/"))
        root_segments: list[str] = []
        partitions: dict[str, str] = {}
        for segment in parsed.path.lstrip("/").split("/"):
            if "=" in segment:
                key, _, value = segment.partition("=")
                if key in partitions:
                    raise ValueError(f"Repeated partition '{key}' in {base_uri}")
                partitions[key] = value
            elif partitions:
                raise ValueError(
                    f"Non-partition segment '{segment}' after partition "
                    f"segments in {base_uri}"
                )
            else:
                root_segments.append(segment)
        known = [key for key in partitions if key in cls.PARTITION_KEYS]
        if known != [key for key in cls.PARTITION_KEYS if key in partitions]:
            raise ValueError(
                f"Partitions in {base_uri} must follow the order "
                f"{list(cls.PARTITION_KEYS)}"
            )
        return cls._from_root(
            parsed.netloc, "/".join(root_segments), partitions, store=store
        )

    @classmethod
    def _from_root(
        cls,
        bucket: str,
        root_path: str,
        partitions: dict[str, str] | None,
        *,
        store: BundleStore | None = None,
    ) -> Self:
        """Build an instance of *cls* rooted exactly at *root_path*.

        Subclasses receive partitions as ``PARTITION_KEYS`` keyword
        arguments; any other key raises ValueError.
        """
        partitions = dict(partitions or {})
        if not cls.PARTITION_KEYS:
            bundle = cls(
                bucket=bucket, root_path=root_path, partitions=partitions, namespace=""
            )
        else:
            unknown = sorted(set(partitions) - set(cls.PARTITION_KEYS))
            if unknown:
                raise ValueError(
                    f"{cls.__name__} has no partition(s) {unknown}; "
                    f"expected {list(cls.PARTITION_KEYS)}"
                )
            values = {key: partitions.get(key, "") for key in cls.PARTITION_KEYS}
            bundle = cls(bucket=bucket, root_path=root_path, namespace="", **values)
        return _with_store(bundle, store)

    # ------------------------------------------------------------------
    # Path properties
    # ------------------------------------------------------------------

    @property
    def root_prefix(self) -> str:
        """S3 key prefix before any partition segments."""
        return self._root_path

    @property
    def root_uri(self) -> str:
        """S3 URI before any partition segments."""
        return (
            f"s3://{self.bucket}/{self._root_path}" if self.bucket else self._root_path
        )

    def _prefix(self, last_partition: str | None = None) -> str:
        """Key prefix of the root plus partition segments up to *last_partition*."""
        segments = [self._root_path] if self._root_path else []
        for key, value in self._partitions.items():
            if not value:
                raise ValueError(
                    f"Partition '{key}' has no value — call resolve_partitions() "
                    f"before accessing path properties on {type(self).__name__!r}."
                )
            segments.append(f"{key}={value}")
            if key == last_partition:
                break
        return "/".join(segments)

    def prefix_through_partition(self, last_partition: str) -> str:
        """S3 key prefix through (up to and including) the specified partition."""
        if last_partition not in self._partitions.keys():
            raise ValueError(f"Partition '{last_partition}' is not a partition")
        return self._prefix(last_partition)

    def uri_through_partition(self, last_partition: str) -> str:
        """S3 URI through (up to and including) the specified partition."""
        return f"s3://{self.bucket}/{self.prefix_through_partition(last_partition)}"

    @property
    def base_uri(self) -> str:
        """Full ``s3://`` URI for this bundle, including partition segments.

        When ``bucket`` is empty (e.g. constructed via ``from_path`` with a
        templated URI), ``root_path`` is used as-is so the caller can render
        it later.
        """
        prefix = self._prefix()
        return f"s3://{self.bucket}/{prefix}" if self.bucket else prefix

    @property
    def base_prefix(self) -> str:
        """S3 key prefix (no ``s3://bucket/``) including partition segments."""
        return self._prefix()

    @property
    def data_uri(self) -> str:
        """S3 URI of the ``data/`` directory."""
        return self.component_uri(DATA.name)

    @property
    def data_prefix(self) -> str:
        """S3 key prefix (no ``s3://bucket/``) for the ``data/`` directory."""
        return self.component_prefix(DATA.name)

    @property
    def metadata_uri(self) -> str:
        """S3 URI of ``metadata.json``."""
        return self.component_uri(METADATA.name)

    @property
    def success_uri(self) -> str:
        """S3 URI of the ``success`` marker."""
        return self.component_uri(SUCCESS.name)

    def sub_directory(self, name: str) -> str:
        """S3 URI of a named sub-directory inside this bundle."""
        return f"{self.base_uri}/{name.strip('/')}"

    # ------------------------------------------------------------------
    # Components
    # ------------------------------------------------------------------

    @classmethod
    def get_component(cls, name: str) -> BundleComponent:
        """Return the component named *name*, which may end in ``/`` as listed.

        Raises KeyError for a name not in ``COMPONENTS``.
        """
        wanted = name.rstrip("/")
        for component in cls.COMPONENTS:
            if component.name == wanted:
                return component
        raise KeyError(
            f"{cls.__name__} has no component {name!r}; expected one of "
            f"{[c.name for c in cls.COMPONENTS]}"
        )

    def component_uri(self, name: str) -> str:
        """S3 URI of the component *name* (see `get_component`)."""
        return f"{self.base_uri}/{self.get_component(name).name}"

    def component_prefix(self, name: str) -> str:
        """S3 key prefix (no ``s3://bucket/``) of the component *name*."""
        return f"{self.base_prefix}/{self.get_component(name).name}"

    # ------------------------------------------------------------------
    # Static path helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_path(base_path: str, *segments: str) -> str:
        """Join a base path with additional path segments.

        Parameters
        ----------
        base_path : str
            Root S3 URI (e.g. ``"s3://bucket/feeds"``).
        *segments : str
            Path components to append.

        Returns
        -------
        str
            The joined path with no trailing slash.
        """
        path = base_path.rstrip("/")
        for segment in segments:
            path = f"{path}/{segment.strip('/')}"
        return path

    # ------------------------------------------------------------------
    # Lifecycle methods
    # ------------------------------------------------------------------

    def _input_bundle_info(
        self, input_bundles: list[Bundle] | None
    ) -> list[dict[str, str]] | None:
        if not input_bundles:
            return None
        input_bundle_info: list[dict[str, str]] = []
        for b in input_bundles:
            info: dict[str, str] = {"path": b.base_uri}
            # Partitions first so bundles rehydrated via from_base_uri carry provenance.
            partitions = b.partitions
            for key in ("provider", "resource", "version"):
                value = partitions.get(key) or str(getattr(b, key, "") or "")
                if value:
                    info[key] = value
            input_bundle_info.append(info)
        return input_bundle_info

    def initialize(
        self,
        input_bundles: list[Bundle] | None = None,
        capture_variables: bool = True,
        custom_metadata: dict[str, Any] | None = None,
        *,
        metadata: BundleMetadata,
        datasets_dir: str | PathLike[str] | None = None,
        source_dataset: Dataset | None = None,
    ) -> None:
        """Initialize the bundle: delete the success file and write metadata.

        *metadata* is the `BundleMetadata` writer bound to this bundle's
        ``base_uri``.  When *datasets_dir* is given, input bundles that carry
        ``provider``/``resource`` are enriched with their Dataset catalogue
        sections.  *source_dataset*, when given, is written as top-level
        ``provider``/``resource`` sections so license artifacts credit it.
        """
        bucket, key = self._success_marker()
        store = self._open_store()
        if store.exists(bucket, key):
            store.delete(bucket, key)
            log.info("Deleted success file s3://%s/%s", bucket, key)
        else:
            log.info("No success file at s3://%s/%s; nothing to delete", bucket, key)

        input_bundle_info = self._input_bundle_info(input_bundles)
        input_paths = (
            [b["path"] for b in input_bundle_info] if input_bundle_info else None
        )

        metadata.init(input_paths=input_paths, capture_variables=capture_variables)
        metadata.add("bundle_type", self.BUNDLE_TYPE)

        if input_bundle_info and datasets_dir is not None:
            _enrich_inputs_from_dataset(metadata, input_bundle_info, datasets_dir)

        if source_dataset is not None:
            for key, value in provenance_sections(source_dataset).items():
                metadata.add(key, value)

        if custom_metadata:
            for key, value in custom_metadata.items():
                metadata.add(key, value)

    def finalize(
        self,
        status: str = "success",
        *,
        metadata: BundleMetadata,
    ) -> None:
        """Stamp metadata with *status*; the marker exists only for ``"success"``.

        *metadata* is the `BundleMetadata` writer bound to this bundle's
        ``base_uri``.  ``"success"`` writes the marker after the metadata.
        Any other status deletes an existing marker before the metadata, so a
        failed rerun never reads as successful.  Partition resolution,
        `needs_processing` and retention treat only bundles with the marker
        as complete.
        """
        bucket, key = self._success_marker()
        if status != BundleStatus.SUCCESS:
            self._open_store().delete(bucket, key)
            metadata.finalize(status=status)
            log.info(
                "Finalized %s with status %s; removed any success file",
                self.base_uri,
                status,
            )
            return
        metadata.finalize(status=status)
        self._open_store().write(bucket, key)
        log.info("Wrote success file s3://%s/%s", bucket, key)

    def _success_marker(self) -> tuple[str, str]:
        """``(bucket, key)`` of the ``success`` marker."""
        return self._object_key(SUCCESS.name)

    def _object_key(self, name: str) -> tuple[str, str]:
        """``(bucket, key)`` of the object *name* directly under `base_prefix`."""
        if not self.bucket:
            raise ValueError(f"Not an S3 URI: {self.base_uri!r}")
        return self.bucket, _dir_prefix(self.base_prefix) + name

    def resolve_partitions(
        self,
        overrides: dict[str, str] | None = None,
        required_markers: tuple[str, ...] = ("success",),
    ) -> None:
        """Resolve partition values by scanning S3 and update this bundle in place.

        Every partition is resolved from the root down.  A set value is used
        as-is but still has to lead to a bundle with *required_markers*; an
        unset value resolves to the newest one that does.  When resolution
        fails, `PartitionResolutionError` names the path where it stopped.

        Parameters
        ----------
        overrides : dict[str, str] | None
            Explicit partition values, taking precedence over the bundle's
            own.  ``"latest"`` (or an empty value) triggers automatic S3
            resolution even for a partition the bundle has set.
        required_markers : tuple[str, ...]
            Marker files the resolved directory must contain.  Defaults to
            ``("success",)``.  Pass ``()`` to require only existence.
        """
        if not self._partitions:
            return

        overrides = overrides or {}
        resolve_overrides = {
            key: overrides.get(key, value or "latest")
            for key, value in self._partitions.items()
        }

        result = _resolve_partitions(
            bucket=self.bucket,
            root_path=self._root_path,
            partition_keys=list(self._partitions),
            overrides=resolve_overrides,
            required_markers=required_markers,
            store=self.store,
        )

        for key, val in result.items():
            self._partitions[key] = val

    def resolve_previous_partition(
        self,
        partition_key: str = "version",
        required_markers: tuple[str, ...] = ("success",),
        same_value_fallback: bool = False,
    ) -> str | None:
        """Return the ``data_uri`` from `find_previous_partition`, or ``None``.

        Use `find_previous_partition` to tell "no prior partition" from "none
        usable", or to get the baseline kind.
        """
        return self.find_previous_partition(
            partition_key=partition_key,
            required_markers=required_markers,
            same_value_fallback=same_value_fallback,
        )["data_uri"]

    def find_previous_partition(
        self,
        partition_key: str = "version",
        required_markers: tuple[str, ...] = ("success",),
        same_value_fallback: bool = False,
    ) -> dict:
        """Find the most recent usable prior value for *partition_key*.

        Prior values under the current bundle's prefix are tried newest
        first; the first whose later partitions (e.g. ``run``) resolve with
        all *required_markers* wins.  An unusable value is skipped and the
        search continues with older ones.

        With *same_value_fallback*, if no older value is usable, the newest
        usable earlier sub-partition under the **current** value is used
        (e.g. an earlier ``run=`` of the same ``version=``).  That compares
        one source snapshot across two pipeline runs, so it is reported as
        ``baseline_kind="same"`` and callers should label it as such.  It
        covers buckets whose expiration has removed the older values.

        Returns a dict with:

        ``data_uri``
            ``data/`` URI of the resolved bundle, or ``None``.
        ``prior_values``
            Every prior value found, newest first; empty for a first partition.
        ``rejected``
            Prior values skipped for lack of a marker or a resolvable run.
            ``data_uri is None`` with a non-empty ``rejected`` means prior
            data exists but none succeeded: an anomaly, not a first run.
        ``baseline_kind``
            ``"previous"`` (older *partition_key* value), ``"same"``
            (same-value fallback), or ``None`` when nothing was found.

        Parameters
        ----------
        partition_key : str
            The partition to find the previous value for (default ``"version"``).
        required_markers : tuple[str, ...]
            Marker files the resolved directory must contain.  Defaults to
            ``("success",)``.  Pass ``()`` to require only existence.
        same_value_fallback : bool
            Enable the same-value fallback.  Opt-in because it measures a
            different thing from a previous-value baseline.

        Raises
        ------
        ValueError
            If *partition_key* is not a partition, or it or an earlier
            partition has no value.
        """
        partition_keys = list(self._partitions.keys())
        if partition_key not in partition_keys:
            raise ValueError(
                f"Partition '{partition_key}' not found in {type(self).__name__}. "
                f"Available: {partition_keys}"
            )

        current_value = self._partitions.get(partition_key, "")
        if not current_value:
            raise ValueError(
                f"Partition '{partition_key}' has no value — resolve it before "
                f"calling resolve_previous_partition()."
            )

        scan_prefix = _dir_prefix(self._root_path)
        for key in partition_keys:
            if key == partition_key:
                break
            val = self._partitions.get(key, "")
            if not val:
                raise ValueError(
                    f"Partition '{key}' has no value — resolve partitions before "
                    f"the target partition '{partition_key}'."
                )
            scan_prefix = f"{scan_prefix}{key}={val}/"

        store = self._open_store()  # one client for the whole scan

        def _list_values(prefix: str, key: str) -> list[str]:
            """Non-empty ``key=`` values immediately under *prefix*."""
            values: list[str] = []
            for name in store.list_children(self.bucket, prefix):
                if name.startswith(f"{key}="):
                    val = name.split("=", 1)[1]
                    if val:
                        values.append(val)
            return values

        remaining_keys = partition_keys[partition_keys.index(partition_key) + 1 :]

        def _resolve_leaf(
            base_prefix: str, key: str, value: str, keys_after: list[str]
        ) -> str | None:
            """Resolved data URI for one candidate value, or None if unusable."""
            if not keys_after:
                leaf_prefix = f"{base_prefix}{key}={value}/"
                for marker in required_markers:
                    leaf_key = f"{leaf_prefix}{marker}"
                    if not store.exists(self.bucket, leaf_key):
                        log.info("Marker %r absent for %s=%s.", marker, key, value)
                        return None
                return f"s3://{self.bucket}/{leaf_prefix}data"

            resolve_prefix = f"{base_prefix}{key}={value}"
            try:
                result = _resolve_partitions(
                    bucket=self.bucket,
                    root_path=resolve_prefix,
                    partition_keys=keys_after,
                    overrides={k: "latest" for k in keys_after},
                    required_markers=required_markers,
                    store=store,
                )
            except PartitionResolutionError as exc:
                log.info("No successful run under %s=%s (%s).", key, value, exc)
                return None

            suffix = "/".join(
                Partition(key=k, delimiter="=", value=v).build()
                for k, v in result.items()
            )
            return f"s3://{self.bucket}/{resolve_prefix}/{suffix}/data"

        def _same_value_baseline() -> str | None:
            """Data URI of the newest usable earlier sibling under *current_value*."""
            if not remaining_keys:
                return None
            sibling_key = remaining_keys[0]
            deeper_keys = remaining_keys[1:]
            current_prefix = f"{scan_prefix}{partition_key}={current_value}/"
            # Earlier siblings only: the current bundle is the one being written.
            current_sibling = self._partitions.get(sibling_key, "")
            siblings = sorted(
                (
                    v
                    for v in _list_values(current_prefix, sibling_key)
                    if not current_sibling
                    or _natural_key(v) < _natural_key(current_sibling)
                ),
                key=_natural_key,
                reverse=True,
            )
            for sibling in siblings:
                data_uri = _resolve_leaf(
                    current_prefix, sibling_key, sibling, deeper_keys
                )
                if data_uri:
                    log.info(
                        "No usable previous %s — falling back to %s=%s under the "
                        "current %s=%s (same-snapshot comparison).",
                        partition_key,
                        sibling_key,
                        sibling,
                        partition_key,
                        current_value,
                    )
                    return data_uri
            return None

        candidates = _list_values(scan_prefix, partition_key)
        current_order = _natural_key(current_value)
        prior_values = sorted(
            (v for v in candidates if _natural_key(v) < current_order),
            key=_natural_key,
            reverse=True,
        )

        rejected: list[str] = []
        for previous in prior_values:
            data_uri = _resolve_leaf(
                scan_prefix, partition_key, previous, remaining_keys
            )
            if data_uri:
                if rejected:
                    log.info(
                        "Skipped %d incomplete %s(s) before reaching %s=%s: %s",
                        len(rejected),
                        partition_key,
                        partition_key,
                        previous,
                        ", ".join(rejected),
                    )
                log.info("Previous %s: %s", partition_key, previous)
                return {
                    "data_uri": data_uri,
                    "prior_values": prior_values,
                    "rejected": rejected,
                    "baseline_kind": "previous",
                }
            rejected.append(previous)

        if not prior_values:
            log.info("No previous %s found before '%s'.", partition_key, current_value)
        else:
            log.info(
                "Found %d previous %s(s) but none usable: %s",
                len(prior_values),
                partition_key,
                ", ".join(prior_values),
            )

        if same_value_fallback:
            data_uri = _same_value_baseline()
            if data_uri:
                return {
                    "data_uri": data_uri,
                    "prior_values": prior_values,
                    "rejected": rejected,
                    "baseline_kind": "same",
                }

        return {
            "data_uri": None,
            "prior_values": prior_values,
            "rejected": rejected,
            "baseline_kind": None,
        }

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> BundleStatusReport:
        """Read this bundle's `BundleStatus` and related facts from storage.

        Subclasses extend `_status_report`, so their requests share the one
        store opened here.

        Raises:
            ValueError: If a partition has no value (call `resolve_partitions`
                first), or the bundle has no bucket.
        """
        return self._status_report(self._open_store())

    def _status_report(self, store: BundleStore) -> BundleStatusReport:
        """Build `status`'s report, making every request on *store*."""
        bucket, directory = self._object_key("")  # "" at the bucket root
        _, metadata_key = self._object_key(METADATA.name)
        has_success = store.exists(*self._success_marker())
        body = store.read(bucket, metadata_key)
        has_metadata = body is not None
        sections = (
            _metadata_section_statuses(body, f"s3://{bucket}/{metadata_key}")
            if body is not None
            else {}
        )
        exists = has_success or has_metadata or store.prefix_exists(bucket, directory)
        return BundleStatusReport(
            status=_derive_status(exists, has_success, sections),
            exists=exists,
            has_success=has_success,
            has_metadata=has_metadata,
            sections=sections,
        )

    # ------------------------------------------------------------------
    # Cleanup / retention
    # ------------------------------------------------------------------

    def is_successful(self) -> bool:
        """Return True if the bundle has a success marker file."""
        bucket, key = self._success_marker()
        return self._open_store().exists(bucket, key)

    def age_days(self, now: datetime.datetime | None = None) -> float | None:
        """Age in days from S3 LastModified of the success (or metadata) file.

        Returns None if neither file exists.  *now* defaults to the current
        UTC time.
        """
        store = self._open_store()
        for bucket, key in (self._success_marker(), self._object_key(METADATA.name)):
            last_modified = store.last_modified(bucket, key)
            if last_modified is None:
                continue
            now = now or datetime.datetime.now(datetime.timezone.utc)
            return (now - last_modified).total_seconds() / 86400.0
        return None

    def is_deletable(
        self,
        min_age_days: float,
        siblings: list[Bundle],
        now: datetime.datetime | None = None,
        *,
        successful: Collection[Bundle] | None = None,
    ) -> tuple[bool, str]:
        """Decide whether this bundle can be deleted, given peers in its partition group.

        ``siblings`` should be all bundles sharing this bundle's
        ``retention_group_key()``; it may include this bundle.

        ``successful`` is an optional snapshot of the group's bundles that
        have a success file; when omitted, each is checked with an S3 HEAD.
        Share one snapshot across a group, as `cleanup_all` does, for
        consistent decisions.

        Partition values are ordered naturally (`_natural_key`), so schema
        ``v1.16.0`` is newer than ``v1.9.0``.

        Rules:

        1. Identify ``latest_successful`` = highest ``retention_order_key()``
           bundle in the group that has a success file.  If none exists, nothing
           is deletable.
        2. The ``latest_successful`` bundle itself is never deletable.
        3. Any other bundle ordered before the latest successful bundle is
           deletable iff ``age_days >= min_age_days``.  Age comes from the
           ``success`` file, or from ``metadata.json`` for a bundle without
           one, so a rerun that `initialize` just restarted is kept while it
           writes.  A bundle with neither file is kept, because its age is
           unknown.

        Returns ``(deletable, reason)``.
        """
        group = [b for b in siblings if b is not self] + [self]
        if successful is None:
            successful = [b for b in group if b.is_successful()]
        successful_keys = {b._identity() for b in successful}
        successful_in_group = [b for b in group if b._identity() in successful_keys]
        if not successful_in_group:
            return False, "no successful bundle exists in partition group"
        latest_successful = max(
            successful_in_group, key=lambda b: b._retention_sort_key()
        )
        latest_key = latest_successful._retention_sort_key()
        latest_desc = latest_successful.retention_order_key()
        self_key = self._retention_sort_key()
        self_successful = self._identity() in successful_keys

        if self is latest_successful or (self_key == latest_key and self_successful):
            return False, "is the latest successful bundle"

        if self_key >= latest_key:
            return False, (
                f"partition order {self.retention_order_key()!r} is not older than "
                f"latest successful {latest_desc!r}"
            )

        age = self.age_days(now=now)
        if age is None:
            return False, "cannot determine bundle age"
        if age < min_age_days:
            return False, (f"age {age:.1f}d < min_age_days {min_age_days}d")
        state = "successful" if self_successful else "unsuccessful"
        return True, (
            f"{state}; superseded by {latest_desc!r}; age {age:.1f}d >= {min_age_days}d"
        )

    def retention_order_key(self) -> tuple[str, ...]:
        """Return partition values used to decide which bundle is latest.

        Compare each value with `_natural_key`.
        """
        return tuple(
            self._partitions[k]
            for k in _RETENTION_ORDER_PARTITION_KEYS
            if k in self._partitions
        )

    def _identity(self) -> tuple[str, str]:
        return (self.bucket, self.base_prefix)

    def _retention_sort_key(self) -> tuple[_NaturalKey, ...]:
        """`retention_order_key` with each value in natural order."""
        return tuple(_natural_key(v) for v in self.retention_order_key())

    def retention_group_key(self) -> tuple[tuple[str, str], ...]:
        """Return stable partition values used to group cleanup candidates."""
        return tuple(
            sorted(
                (
                    (key, value)
                    for key, value in self._partitions.items()
                    if key not in _RETENTION_ORDER_PARTITION_KEYS
                ),
                key=lambda kv: kv[0],
            )
        )

    def delete(
        self,
        dry_run: bool = False,
        *,
        table_dropper: TableDropper | None = None,
    ) -> DeleteResult:
        """Delete this bundle's S3 objects and optionally its catalogue tables.

        Idempotent: safe to call on an already-deleted bundle.  *table_dropper*
        runs first and is best-effort: a failure is logged and the S3 delete
        proceeds.

        Args:
            dry_run: If True, log actions without executing them.
            table_dropper: Optional `TableDropper` for registered tables.

        Raises:
            ValueError: If the bundle has no partitions, or any declared
                partition (``PARTITION_KEYS``, else every given partition)
                has no value, because the delete would then cover more than
                this one bundle.
            DeletePrefixError: If an `S3BundleStore` failed to delete some
                objects.
        """
        declared = self.PARTITION_KEYS or tuple(self._partitions)
        if not declared:
            raise ValueError(
                f"Refusing to delete {self.root_uri}: the bundle has no "
                "partitions, so this would delete every bundle under it"
            )
        missing = [key for key in declared if not self._partitions.get(key)]
        if missing:
            raise ValueError(
                f"Refusing to delete under {self.root_uri}: partition(s) "
                f"{', '.join(missing)} have no value, so this would delete more "
                "than one bundle. Resolve the partitions first."
            )
        base_prefix = self.base_prefix.strip("/")
        prefix = f"{base_prefix}/"
        store = self._open_store()

        deleted_tables: list[str] = []
        if table_dropper is not None:
            try:
                deleted_tables = table_dropper(
                    bucket=self.bucket,
                    base_prefix=self.base_prefix,
                    dry_run=dry_run,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                log.warning(
                    "Failed to drop tables for s3://%s/%s: %s",
                    self.bucket,
                    self.base_prefix,
                    exc,
                )

        if dry_run:
            count = sum(1 for _ in store.iter_keys(self.bucket, prefix))
            log.info(
                "[dry_run] would delete %d objects under s3://%s/%s (tables: %s)",
                count,
                self.bucket,
                prefix,
                deleted_tables,
            )
            return DeleteResult(
                bundle_uri=self.base_uri,
                deleted_object_count=count,
                deleted_tables=deleted_tables,
                dry_run=True,
            )

        count = store.delete_prefix(self.bucket, base_prefix)
        log.info(
            "Deleted %d objects under s3://%s/%s (tables: %s)",
            count,
            self.bucket,
            prefix,
            deleted_tables,
        )
        return DeleteResult(
            bundle_uri=self.base_uri,
            deleted_object_count=count,
            deleted_tables=deleted_tables,
            dry_run=False,
        )

    # ------------------------------------------------------------------
    # Discovery / cleanup (classmethods)
    # ------------------------------------------------------------------

    @classmethod
    def discover_all(
        cls,
        bucket: str,
        namespace: str = "",
        *,
        store: BundleStore | None = None,
    ) -> list[Self]:
        """Discover all bundles of this type under ``<namespace>/<ROOT_PATH>``.

        Returns one ``cls`` instance per ``key=value`` combination of
        ``PARTITION_KEYS``, with or without a success marker.

        The walk uses *store*, or a fresh `S3BundleStore`; only an explicit
        *store* is attached to the returned bundles.

        Raises NotImplementedError unless ``ROOT_PATH`` and ``PARTITION_KEYS``
        are set; such subclasses must override this method.
        """
        if not cls.ROOT_PATH or not cls.PARTITION_KEYS:
            raise NotImplementedError(
                f"{cls.__name__} does not define ROOT_PATH / PARTITION_KEYS "
                "and must override discover_all()."
            )

        scan_root = _compose_root(cls.ROOT_PATH, namespace)
        return [
            cls._from_root(bucket, scan_root, partitions, store=store)
            for partitions in _walk_partitions(
                bucket=bucket,
                root_prefix=scan_root,
                partition_keys=list(cls.PARTITION_KEYS),
                store=store,
            )
        ]

    @classmethod
    def cleanup_all(
        cls,
        bucket: str,
        min_age_days: float = 30,
        dry_run: bool = True,
        namespace: str = "",
        table_dropper: TableDropper | None = None,
        now: datetime.datetime | None = None,
        *,
        store: BundleStore | None = None,
    ) -> CleanupReport:
        """Discover, evaluate, and (optionally) delete superseded bundles.

        See `is_deletable` for the retention rules.  *table_dropper* is
        passed through to `delete` for each deleted bundle.

        An explicit *store* is used for discovery and every check and delete,
        so an override of `discover_all` must then accept ``store``; without
        one, the checks and deletes share one default store.
        """
        # Overrides of discover_all may have no ``store`` parameter.
        discover_kwargs = {} if store is None else {"store": store}
        bundles = cls.discover_all(
            bucket=bucket, namespace=namespace, **discover_kwargs
        )
        shared = _store_or_default(store)
        for b in bundles:
            if store is not None or b.store is None:
                b.store = shared
        log.info(
            "Discovered %d bundles of type %s under s3://%s",
            len(bundles),
            cls.__name__,
            bucket,
        )

        # Versions/runs of the same logical bundle share a group.
        groups: dict[tuple[tuple[str, str], ...], list[Bundle]] = {}
        for b in bundles:
            groups.setdefault(b.retention_group_key(), []).append(b)

        report = CleanupReport(bundle_type=cls.BUNDLE_TYPE, dry_run=dry_run)
        for key, group in groups.items():
            # One success snapshot per group, shared by every decision in it.
            successful = [b for b in group if b.is_successful()]
            for b in group:
                deletable, reason = b.is_deletable(
                    min_age_days=min_age_days,
                    siblings=group,
                    now=now,
                    successful=successful,
                )
                log.info(
                    "%s -> deletable=%s (%s)",
                    b.base_uri,
                    deletable,
                    reason,
                )
                if not deletable:
                    report.kept.append(BundleDecision(b.base_uri, reason))
                    continue
                if dry_run:
                    report.would_delete.append(BundleDecision(b.base_uri, reason))
                    continue
                try:
                    result = b.delete(dry_run=False, table_dropper=table_dropper)
                    report.deleted.append(result)
                except Exception as exc:
                    log.exception("Delete failed for %s", b.base_uri)
                    report.errors.append(
                        BundleDecision(b.base_uri, f"delete failed: {exc}")
                    )
        return report

    def needs_processing(
        self,
        partitions: dict[str, str] | None = None,
        base_uri: str | None = None,
    ) -> bool:
        """Determine whether data processing should proceed for this bundle.

        Returns ``True`` (proceed) when any *partitions* value other than
        ``"latest"`` is set, when the current run already has a success
        marker (a rerun), or when no sibling ``run=`` has one.  Returns
        ``False`` (skip downstream) when another run already succeeded.
        Only the exact ``run=<id>/success`` key counts as a marker.

        Parameters
        ----------
        partitions : dict[str, str] | None
            Partition overrides; any value other than ``"latest"`` forces
            processing.
        base_uri : str | None
            Fully rendered bundle URI to check.  Defaults to
            ``self.base_uri``; pass an explicit value when ``base_uri``
            contains placeholders that the caller has rendered.
        """
        s3_uri = base_uri if base_uri is not None else self.base_uri

        for key, value in (partitions or {}).items():
            if value and str(value).lower() != "latest":
                log.info(
                    "Override %s='%s' set, forcing reprocessing of bundle at %s",
                    key,
                    value,
                    s3_uri,
                )
                return True

        # Check the current run first: its own success marker means a rerun.
        bucket, prefix, current_run = _split_run_uri(s3_uri)
        store = self._open_store()  # one client for the whole check
        if current_run and store.exists(bucket, f"{prefix}run={current_run}/success"):
            log.info(
                "Current run='%s' already has a success file — "
                "allowing rerun at s3://%s/%srun=%s",
                current_run,
                bucket,
                prefix,
                current_run,
            )
            return True

        # Any other run with a success file means the work is already done.
        for name in store.list_children(bucket, prefix):
            if not name.startswith("run="):
                continue
            child = f"{prefix}{name}"
            if store.exists(bucket, f"{child}/success"):
                log.info("Existing successful run found at s3://%s/%s", bucket, child)
                return False

        log.info(
            "No successful run found under s3://%s/%s, processing required",
            bucket,
            prefix,
        )
        return True

    @classmethod
    def resolve_partitions_batch(
        cls,
        provider: str,
        bucket: str,
        namespace: str = "",
        *,
        store: BundleStore | None = None,
    ) -> dict[str, Self]:
        """Return the latest successful bundle per resource under *provider*.

        Uses one paginated ``list_objects_v2`` over ``<root>/provider=<X>/``.
        Bundles are ordered by the
        partitions after ``resource`` (`_natural_key`).  Only keys with the
        exact bundle layout count as markers, e.g.
        ``resource=R/version=V/run=Z/success``.

        Returns a dict of resource to fully resolved bundle, carrying *store*
        when given.  Raises TypeError unless the class is partitioned by
        ``(provider, resource, ...)``.
        """
        if cls.PARTITION_KEYS[:2] != ("provider", "resource"):
            raise TypeError(
                f"{cls.__name__} is not partitioned by (provider, resource)"
            )

        root = _compose_root(cls.ROOT_PATH, namespace)
        prefix = f"{root}/provider={provider}/"
        keys = cls.PARTITION_KEYS[1:]

        best: dict[str, dict[str, str]] = {}
        best_order: dict[str, tuple[_NaturalKey, ...]] = {}
        for key in _store_or_default(store).iter_keys(bucket, prefix):
            segments = key[len(prefix) :].split("/")
            if len(segments) != len(keys) + 1 or segments[-1] != SUCCESS.name:
                continue
            values: dict[str, str] = {}
            for name, segment in zip(keys, segments[:-1], strict=True):
                seg_key, _, seg_value = segment.partition("=")
                if seg_key != name or not seg_value:
                    break
                values[name] = seg_value
            else:
                resource = values["resource"]
                order = tuple(_natural_key(values[k]) for k in keys[1:])
                if resource not in best_order or order > best_order[resource]:
                    best[resource] = values
                    best_order[resource] = order

        return {
            resource: _with_store(
                cls(provider=provider, bucket=bucket, namespace=namespace, **values),
                store,
            )
            for resource, values in best.items()
        }

    @classmethod
    def needs_processing_batch(
        cls,
        bundles: list[Bundle],
        partitions: dict[str, str] | None = None,
        *,
        store: BundleStore | None = None,
    ) -> list[Bundle]:
        """Return the subset of *bundles* for which `needs_processing` is ``True``.

        Bundles are grouped by a shared scan prefix (``<root>/provider=<X>/``
        for provider/resource bundles, else the parent of ``run=``) with one
        paginated ``list_objects_v2`` per group.  ``base_uri`` values must be
        fully rendered.

        Bundles are checked with *store* when given, else their own store;
        bundles without one share a fresh `S3BundleStore`.
        """
        partitions = partitions or {}
        for key, value in partitions.items():
            if value and str(value).lower() != "latest":
                log.info(
                    "Override %s='%s' set, forcing reprocessing of all %d "
                    "candidate bundle(s)",
                    key,
                    value,
                    len(bundles),
                )
                return list(bundles)

        if not bundles:
            return []

        def _scan_prefix(parent: str) -> str:
            # Cut at ``resource=`` so all of a provider's resources share one LIST.
            segments = parent.rstrip("/").split("/")
            for i, seg in enumerate(segments):
                if seg.startswith("resource="):
                    return _dir_prefix("/".join(segments[:i]))
            return parent

        shared = store if store is not None else S3BundleStore()
        info = []
        for b in bundles:
            s = b.store if store is None and b.store is not None else shared
            info.append((b, s, *_split_run_uri(b.base_uri)))
        # Keyed by id() so stores needn't be hashable; *info* keeps them alive.
        scans = {
            (id(s), bucket, _scan_prefix(parent)): s for _, s, bucket, parent, _ in info
        }

        # (store id, bucket, parent prefix) -> runs under it with a success file
        successful: dict[tuple[int, str, str], set[str]] = {}
        for (sid, bucket, prefix), s in scans.items():
            for k in s.iter_keys(bucket, prefix):
                if not k.endswith("/success"):
                    continue
                head, _, leaf = k[: -len("/success")].rpartition("/")
                if leaf.startswith("run="):
                    parent = f"{head}/" if head else ""
                    successful.setdefault((sid, bucket, parent), set()).add(leaf[4:])

        needs = [
            b
            for b, s, bucket, parent, run in info
            if not (runs := successful.get((id(s), bucket, parent))) or run in runs
        ]

        log.info(
            "needs_processing_batch: %d/%d bundle(s) need processing across "
            "%d scan group(s)",
            len(needs),
            len(bundles),
            len(scans),
        )
        return needs

    @classmethod
    def initialize_batch(
        cls,
        bundles: list[Bundle],
        *,
        metadata_factory: Callable[[Bundle], BundleMetadata],
        input_bundles: list[Bundle] | None = None,
        per_bundle_inputs: list[list[Bundle]] | None = None,
        capture_variables: bool = True,
        custom_metadata: dict[str, Any] | None = None,
        datasets_dir: str | PathLike[str] | None = None,
        source_dataset: Dataset | None = None,
        max_workers: int = 32,
        store: BundleStore | None = None,
    ) -> None:
        """Run `initialize` on every bundle across *max_workers* threads.

        *input_bundles* is shared: every bundle records the same inputs.
        *per_bundle_inputs* is positional: ``per_bundle_inputs[i]`` is recorded
        as the inputs of ``bundles[i]`` (``[]`` for none).  The two are
        mutually exclusive.  All bundles share the other arguments;
        *metadata_factory* builds each bundle's `BundleMetadata` writer.  See
        `_run_batch` for *store* and failures.
        """
        if not bundles:
            return
        if per_bundle_inputs is not None:
            if input_bundles is not None:
                raise ValueError(
                    "Pass either input_bundles or per_bundle_inputs, not both"
                )
            if len(per_bundle_inputs) != len(bundles):
                raise ValueError(
                    f"per_bundle_inputs has {len(per_bundle_inputs)} entries "
                    f"for {len(bundles)} bundles"
                )

        def _one(i: int, target: Bundle) -> None:
            target.initialize(
                input_bundles=(
                    input_bundles if per_bundle_inputs is None else per_bundle_inputs[i]
                ),
                capture_variables=capture_variables,
                custom_metadata=custom_metadata,
                metadata=metadata_factory(bundles[i]),
                datasets_dir=datasets_dir,
                source_dataset=source_dataset,
            )

        _run_batch("initialize", _one, bundles, max_workers, store)

    @classmethod
    def finalize_batch(
        cls,
        bundles: list[Bundle],
        *,
        metadata_factory: Callable[[Bundle], BundleMetadata],
        status: str = "success",
        max_workers: int = 32,
        store: BundleStore | None = None,
    ) -> None:
        """Run `finalize` on every bundle across *max_workers* threads.

        *metadata_factory* builds each bundle's `BundleMetadata` writer.  See
        `_run_batch` for *store* and failures.
        """
        if not bundles:
            return

        def _one(i: int, target: Bundle) -> None:
            target.finalize(status=status, metadata=metadata_factory(bundles[i]))

        _run_batch("finalize", _one, bundles, max_workers, store)

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        parts = [f"bucket={self.bucket!r}", f"root_prefix={self._root_path!r}"]
        for key, value in self._partitions.items():
            if value:
                parts.append(f"{key}={value!r}")
        return f"{type(self).__name__}({', '.join(parts)})"
