"""Data bundle model for pipeline directories.

A *bundle* is an S3 directory that contains:

- A ``data/`` subdirectory with the final output data for the bundle.
- A ``metadata.json`` file tracking pipeline execution.
- A ``success`` marker file indicating the bundle is complete.
- Optionally, additional named subdirectories for intermediate or
  supplemental artifacts.

Bundles follow a lifecycle:

1. **Initialize** -- delete the existing ``success`` file (signalling that the
   bundle is being rebuilt) and create a fresh ``metadata.json``.
2. **Process** -- the pipeline writes data into ``data/`` and any other
   subdirectories.
3. **Finalize** -- stamp ``metadata.json`` as complete and write a new
   ``success`` file.

Each bundle type defines an ordered ``partitions`` dict whose keys
become Hive-style ``key=value`` path segments appended to the root
path.

Class hierarchy
---------------
- `Bundle` -- base class with ``data`` directory, metadata, and success file.
- `SourceRawBundle` -- raw collected resource from a provider (a.k.a dataset).
- `SourceIngestBundle` -- ingested/normalised dataset, with changelog,
  metrics, and metrics_summary.
- `ThemeAssembleBundle` -- assembled theme data from multiple datasets, with
  changelog, metrics, and metrics_summary.
- `ThemePromoteBundle` -- promoted theme data with bridge files, pmtiles,
  changelog, metrics, metrics_summary, changelog_internal, and validation.
- `ReleaseCandidateBundle` -- release candidate with bridge files, pmtiles,
  changelog, metrics, and metrics_summary.
- `ScratchBundle` -- ephemeral debug/intermediate output, labeled by purpose.

Deployment-specific inputs
--------------------------
These are passed in explicitly; orchestrators can subclass the bundle
classes to supply defaults.

- ``bucket`` and ``namespace`` are constructor arguments.  A non-empty
  ``namespace`` is prepended to the root path (``<namespace>/<root>``).
- `Bundle.initialize` / `Bundle.finalize` take a `BundleMetadata` writer
  (the batch variants take a ``metadata_factory``).
- `Bundle.delete` / `Bundle.cleanup_all` take an optional `TableDropper`
  to drop any catalogue tables registered for a bundle.
- `Bundle.initialize` takes the ``datasets_dir`` used to enrich input
  provenance from the ``overture_core`` Dataset catalogue.
- ``store`` is the `BundleStore` used for S3 access; ``None`` opens a
  fresh `S3BundleStore` per operation.
"""

from __future__ import annotations

import datetime
import logging
import re
from collections.abc import Callable, Collection, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from os import PathLike
from typing import Any, Protocol, Self, TypeVar
from urllib.parse import urlparse

from overture_core.bundle.store import BundleStore, S3BundleStore
from overture_core.cloud.aws.object import parse_s3_uri, success_file_key
from overture_core.cloud.cloud import Partition
from overture_core.dataset.dataset import Dataset

log = logging.getLogger(__name__)

_BundleT = TypeVar("_BundleT", bound="Bundle")


class PartitionResolutionError(Exception):
    """No viable partition path was found during S3 partition resolution."""


class BundleMetadata(Protocol):
    """Writer for a bundle's ``metadata.json``, injected into the lifecycle methods.

    Each instance is bound to one bundle's ``base_uri``.
    """

    def init(
        self,
        input_paths: list[str] | None = None,
        capture_variables: bool = True,
    ) -> Any: ...

    def add(self, key: str, value: Any) -> Any: ...

    def read_dag_section(self) -> dict[str, Any]: ...

    def update_inputs(self, inputs: list[dict[str, Any]]) -> Any: ...

    def finalize(self, status: str = "success") -> Any: ...


class TableDropper(Protocol):
    """Drops catalogue tables registered for the bundle at *base_prefix*.

    Returns the (fully qualified) table names dropped, or that would have
    been dropped when *dry_run* is set.
    """

    def __call__(self, bucket: str, base_prefix: str, dry_run: bool) -> list[str]: ...


def _store_or_default(store: BundleStore | None) -> BundleStore:
    """Return *store*, or a fresh `S3BundleStore` for one operation.

    A per-operation store shares one boto3 client across a walk or check and
    picks up fresh credentials on the next operation.
    """
    return store if store is not None else S3BundleStore()


def _with_store(bundle: _BundleT, store: BundleStore | None) -> _BundleT:
    """Attach *store* to *bundle* when given, and return the bundle.

    Assigned as an attribute so it works for subclasses whose ``__init__``
    has no ``store`` parameter.
    """
    if store is not None:
        bundle.store = store
    return bundle


def _share_store(
    bundles: list[Bundle], store: BundleStore | None = None
) -> list[Bundle]:
    """Attach *store*, or one new `S3BundleStore`, to each storeless bundle.

    Returns the bundles it attached to.
    """
    storeless = [b for b in bundles if b.store is None]
    if storeless:
        shared = _store_or_default(store)
        for b in storeless:
            b.store = shared
    return storeless


def _compose_root(root_path: str, namespace: str = "") -> str:
    """Join an optional *namespace* and *root_path* into a bundle root prefix."""
    root = root_path.strip("/")
    namespace = (namespace or "").strip("/")
    return f"{namespace}/{root}" if namespace else root


def _dir_prefix(path: str) -> str:
    """*path* as a key prefix ending in ``/``; ``""`` for the bucket root."""
    path = path.rstrip("/")
    return f"{path}/" if path else ""


_DIGIT_RUNS = re.compile(r"(\d+)", re.ASCII)


_NaturalKey = tuple[tuple[tuple[int, int | str], ...], str]


def _natural_key(value: str) -> _NaturalKey:
    """Sort key ordering embedded integers numerically: ``v1.9.0 < v1.16.0``.

    Used wherever partition values are ordered.  Fixed-width values (dates,
    ``ts_nodash`` run ids) keep their plain-string order; the raw value breaks
    ties (``"01"`` vs ``"1"``).
    """
    tokens = tuple(
        (0, int(part)) if i % 2 else (1, part)
        for i, part in enumerate(_DIGIT_RUNS.split(value))
        if part
    )
    return tokens, value


def _split_run_uri(s3_uri: str) -> tuple[str, str, str | None]:
    """Split a rendered bundle URI into ``(bucket, parent_prefix, run)``.

    *parent_prefix* is the key prefix holding the ``run=`` directories, with
    a trailing ``/``; everything from the first ``run=`` segment on is
    dropped.  *run* is that segment's value, or ``None`` when the URI has no
    ``run=`` segment.
    """
    run_match = re.search(r"/run=([^/]+)", s3_uri)
    parsed = urlparse(re.sub(r"/run=[^/]*(?:/.*)?$", "", s3_uri))
    parent = parsed.path.strip("/")
    return (
        parsed.netloc,
        _dir_prefix(parent),
        run_match.group(1) if run_match else None,
    )


def _enrich_inputs_from_dataset(
    metadata: BundleMetadata,
    input_bundles: list[dict[str, str]],
    datasets_dir: str | PathLike[str],
) -> None:
    """Add Dataset catalogue ``provider``/``resource`` sections to metadata inputs.

    An input is enriched when it lacks either section and its *input_bundles*
    entry names a provider and resource.  A failed catalogue lookup is logged
    as a warning.
    """
    bundle_lookup = {b["path"]: b for b in input_bundles}

    section = metadata.read_dag_section()
    inputs = section.get("inputs", [])
    modified = False

    for entry in inputs:
        if "provider" in entry and "resource" in entry:
            continue
        path = entry.get("path", "")
        bundle_info = bundle_lookup.get(path, {})
        provider = bundle_info.get("provider", "")
        resource = bundle_info.get("resource", "")
        version = bundle_info.get("version", "")
        if not provider or not resource:
            continue
        try:
            ds = Dataset.from_name(provider, resource, datasets_dir)
            entry["provider"] = ds.provider_data
            resource_data = ds.resource_data
            if version:
                resource_data["version"] = version
            entry["resource"] = resource_data
            modified = True
            log.info(
                "Enriched input %s with Dataset catalogue for %s/%s",
                path,
                provider,
                resource,
            )
        except Exception as exc:
            log.warning("Could not load Dataset for %s/%s: %s", provider, resource, exc)

    if modified:
        metadata.update_inputs(inputs)


def _resolve_partitions(
    bucket: str,
    root_path: str,
    partition_keys: list[str],
    overrides: dict[str, str] | None = None,
    required_markers: tuple[str, ...] = ("success",),
    *,
    store: BundleStore | None = None,
) -> dict[str, str]:
    """Resolve partition values by scanning S3 directories under *root_path*.

    Keys in *partition_keys* are resolved in order.  An *overrides* value is
    used as-is; a missing or ``"latest"`` value picks the newest
    ``key=<value>`` directory, backtracking to older candidates when one has
    no viable deeper path.

    *required_markers* must all exist at the resolved leaf; ``()`` requires
    only that it has content.  *store* defaults to one fresh
    `S3BundleStore` for the walk.

    Returns a dict mapping each partition key to its resolved value.  Raises
    `PartitionResolutionError`, naming where the search stopped, when no
    viable path exists.
    """
    overrides = overrides or {}
    store = _store_or_default(store)

    log.info(
        "Resolving partitions %s under s3://%s/%s (overrides=%s, required_markers=%s)",
        partition_keys,
        bucket,
        root_path,
        overrides,
        required_markers,
    )

    def _list_subdirs(prefix: str) -> list[str]:
        """Return immediate subdirectory names under *prefix*."""
        dirs = store.list_children(bucket, prefix)
        log.debug(
            "Listed %d subdirs under s3://%s/%s: %s", len(dirs), bucket, prefix, dirs
        )
        return dirs

    def _is_valid_leaf(prefix: str) -> bool:
        """True if *prefix* has every required marker, or any content if none."""
        for marker in required_markers:
            if not store.exists(bucket, f"{prefix}{marker}"):
                log.debug("Marker %r not found at s3://%s/%s", marker, bucket, prefix)
                return False
        if not required_markers:
            exists = store.prefix_exists(bucket, prefix)
            log.debug("Directory exists at s3://%s/%s: %s", bucket, prefix, exists)
            return exists
        return True

    def _resolve(prefix: str, keys: list[str]) -> dict[str, str] | None:
        """Recursively resolve *keys* under *prefix*.

        Returns the resolved values, or ``None`` with *failure* set to where
        the search stopped: the deepest explicit value without a valid leaf,
        or the outermost ``latest`` level that ran out of candidates.
        """
        nonlocal failure
        if not keys:
            valid = _is_valid_leaf(prefix)
            log.debug("Leaf s3://%s/%s valid=%s", bucket, prefix, valid)
            if not valid:
                failure = (
                    f"No bundle with required markers [{markers_desc}] at "
                    f"s3://{bucket}/{prefix}"
                )
            return {} if valid else None

        key = keys[0]
        remaining = keys[1:]
        is_last = len(remaining) == 0
        override = overrides.get(key, "latest")

        if override and override.lower() != "latest":
            child_prefix = f"{prefix}{key}={override}/"
            log.info(
                "Using override %s='%s', descending into s3://%s/%s",
                key,
                override,
                bucket,
                child_prefix,
            )
            if is_last:
                if not _is_valid_leaf(child_prefix):
                    log.warning(
                        "Override path s3://%s/%s has no valid leaf",
                        bucket,
                        child_prefix,
                    )
                    failure = (
                        f"Override {key}='{override}' missing required markers "
                        f"[{markers_desc}] at s3://{bucket}/{child_prefix}"
                    )
                    return None
                return {key: override}
            deeper = _resolve(child_prefix, remaining)
            if deeper is None:
                log.warning(
                    "Override path s3://%s/%s led to no viable deeper path",
                    bucket,
                    child_prefix,
                )
                return None
            return {key: override, **deeper}

        # "latest": newest first; an empty value (``key=``) can't form a bundle path.
        candidates = [
            d
            for d in _list_subdirs(prefix)
            if d.startswith(f"{key}=") and len(d) > len(key) + 1
        ]
        log.info(
            "Scanning %d candidate(s) for %s= under s3://%s/%s",
            len(candidates),
            key,
            bucket,
            prefix,
        )
        for cand in sorted(
            candidates, key=lambda c: _natural_key(c.split("=", 1)[1]), reverse=True
        ):
            val = cand.split("=", 1)[1]
            child_prefix = f"{prefix}{cand}/"
            log.debug("Trying candidate %s='%s'", key, val)
            if is_last:
                if _is_valid_leaf(child_prefix):
                    log.info("Selected %s='%s'", key, val)
                    return {key: val}
            else:
                deeper = _resolve(child_prefix, remaining)
                if deeper is not None:
                    log.info("Selected %s='%s'", key, val)
                    return {key: val, **deeper}
            log.debug("Candidate %s='%s' skipped (no valid leaf)", key, val)

        log.warning(
            "No valid candidate found for %s= under s3://%s/%s", key, bucket, prefix
        )
        failure = (
            f"No {key}= directory with required markers [{markers_desc}] found "
            f"under s3://{bucket}/{prefix} ({len(candidates)} candidate(s) checked)"
        )
        return None

    markers_desc = ", ".join(repr(m) for m in required_markers) or "none"
    failure = ""
    base_prefix = _dir_prefix(root_path)
    result = _resolve(base_prefix, list(partition_keys))

    if result is None:
        raise PartitionResolutionError(failure)

    for key, val in result.items():
        log.info("Resolved %s='%s'", key, val)

    partition_suffix = "/".join(
        Partition(key=k, delimiter="=", value=v).build() for k, v in result.items()
    )
    path_uri = f"s3://{bucket}/{base_prefix}{partition_suffix}"
    log.info("Resolved bundle base path: %s", path_uri)

    return result


# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------


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


_RETENTION_ORDER_PARTITION_KEYS = ("version", "release", "schema", "run")


def _walk_partitions(
    bucket: str,
    root_prefix: str,
    partition_keys: list[str],
    *,
    store: BundleStore | None = None,
) -> Iterator[dict[str, str]]:
    """Yield every ``{key: value}`` combination present under *root_prefix*.

    Walks Hive-style ``key=value`` directories in *partition_keys* order and
    yields each full match whatever markers it has; `is_deletable` checks
    success.  *store* defaults to one fresh `S3BundleStore` for the walk.
    """
    store = _store_or_default(store)

    def _walk(prefix: str, remaining: list[str], acc: dict[str, str]):
        if not remaining:
            yield dict(acc)
            return
        key = remaining[0]
        rest = remaining[1:]
        for name in store.list_children(bucket, prefix):
            if not name.startswith(f"{key}="):
                continue
            value = name.split("=", 1)[1]
            if not value:
                continue
            child_prefix = f"{prefix}{name}/"
            acc[key] = value
            yield from _walk(child_prefix, rest, acc)
            acc.pop(key, None)

    yield from _walk(_dir_prefix(root_prefix), partition_keys, {})


def _run_batch(
    action: str,
    fn: Callable[[Bundle], None],
    bundles: list[Bundle],
    max_workers: int,
) -> None:
    """Run *fn* on every bundle across *max_workers* threads.

    Bundles without a store share one `S3BundleStore` for the batch.  Every
    bundle is attempted.  Each failure is logged, then the first one in
    *bundles* order is re-raised.
    """
    lent = _share_store(bundles)
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [(b, pool.submit(fn, b)) for b in bundles]
    finally:
        for b in lent:
            b.store = None
    failures = [(b, exc) for b, f in futures if (exc := f.exception()) is not None]
    for b, exc in failures:
        log.error("%s failed for %r", action, b, exc_info=exc)
    if failures:
        log.error("%s failed for %d/%d bundle(s)", action, len(failures), len(bundles))
        raise failures[0][1]


# ---------------------------------------------------------------------------
# Base Bundle class
# ---------------------------------------------------------------------------


class Bundle:
    """Base data bundle on S3.

    Every bundle has a ``data`` directory, a ``metadata.json`` file, and a
    ``success`` marker.  Subclasses add extra named directories.

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

    # Bundle root before any namespace; required by discover_all / cleanup_all.
    ROOT_PATH: str = ""

    # Partition key names, in path order.
    PARTITION_KEYS: tuple[str, ...] = ()

    # Bundle subdirectories besides ``data``.
    _EXTRA_DIRS: tuple[str, ...] = ()

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
        segments (an ambiguous root), or for a key outside the subclass's
        ``PARTITION_KEYS``.
        """
        parsed = urlparse(base_uri.rstrip("/"))
        root_segments: list[str] = []
        partitions: dict[str, str] = {}
        for segment in parsed.path.lstrip("/").split("/"):
            if "=" in segment:
                key, _, value = segment.partition("=")
                partitions[key] = value
            elif partitions:
                raise ValueError(
                    f"Non-partition segment '{segment}' after partition "
                    f"segments in {base_uri}"
                )
            else:
                root_segments.append(segment)
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
        return f"{self.base_uri}/data"

    @property
    def data_prefix(self) -> str:
        """S3 key prefix (no ``s3://bucket/``) for the ``data/`` directory."""
        return f"{self.base_prefix}/data"

    @property
    def metadata_uri(self) -> str:
        """S3 URI of ``metadata.json``."""
        return f"{self.base_uri}/metadata.json"

    @property
    def success_uri(self) -> str:
        """S3 URI of the ``success`` marker."""
        return f"{self.base_uri}/success"

    def sub_directory(self, name: str) -> str:
        """S3 URI of a named sub-directory inside this bundle."""
        return f"{self.base_uri}/{name.strip('/')}"

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
            p = getattr(b, "provider", "")
            r = getattr(b, "resource", "")
            v = getattr(b, "version", "")
            if p:
                info["provider"] = p
            if r:
                info["resource"] = r
            if v:
                info["version"] = v
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
    ) -> None:
        """Initialize the bundle: delete the success file and write metadata.

        *metadata* is the `BundleMetadata` writer bound to this bundle's
        ``base_uri``.  When *datasets_dir* is given, input bundles that carry
        ``provider``/``resource`` are enriched with their Dataset catalogue
        sections.
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

        if custom_metadata:
            for key, value in custom_metadata.items():
                metadata.add(key, value)

    def finalize(
        self,
        status: str = "success",
        *,
        metadata: BundleMetadata,
    ) -> None:
        """Finalize the bundle: update metadata and write the success file.

        *metadata* is the `BundleMetadata` writer bound to this bundle's
        ``base_uri``.
        """
        metadata.finalize(status=status)
        bucket, key = self._success_marker()
        self._open_store().write(bucket, key)
        log.info("Wrote success file s3://%s/%s", bucket, key)

    def _success_marker(self) -> tuple[str, str]:
        """``(bucket, key)`` of the ``success`` marker, parsed from `base_uri`."""
        bucket, prefix = parse_s3_uri(self.base_uri)
        return bucket, success_file_key(prefix)

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
        for uri in (self.success_uri, self.metadata_uri):
            _, key = parse_s3_uri(uri)
            last_modified = store.last_modified(self.bucket, key)
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

        An explicit *store* is used for discovery and every check and delete;
        without one, the checks and deletes share one default store.
        """
        # Overrides of discover_all may have no ``store`` parameter.
        discover_kwargs = {} if store is None else {"store": store}
        bundles = cls.discover_all(
            bucket=bucket, namespace=namespace, **discover_kwargs
        )
        _share_store(bundles, store)
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
            if value and value.lower() != "latest":
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
        current_marker = f"{prefix}run={current_run}/success"
        if current_run and store.exists(bucket, current_marker):
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
    ) -> dict[str, "Bundle"]:
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
            if len(segments) != len(keys) + 1 or segments[-1] != "success":
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
            if value and value.lower() != "latest":
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
                    return "/".join(segments[:i]) + "/"
            return parent

        shared = store
        stores: dict[int, BundleStore] = {}

        def _store_for(b: Bundle) -> int:
            nonlocal shared
            chosen = store if store is not None else b.store
            if chosen is None:
                if shared is None:
                    shared = S3BundleStore()
                chosen = shared
            stores[id(chosen)] = chosen
            return id(chosen)

        info = [(b, _store_for(b), *_split_run_uri(b.base_uri)) for b in bundles]
        scans = dict.fromkeys(
            (sid, bucket, _scan_prefix(parent)) for _, sid, bucket, parent, _ in info
        )

        # (store id, bucket, parent prefix) -> runs under it with a success file
        successful: dict[tuple[int, str, str], set[str]] = {}
        for sid, bucket, prefix in scans:
            for k in stores[sid].iter_keys(bucket, prefix):
                if not k.endswith("/success"):
                    continue
                head, _, leaf = k[: -len("/success")].rpartition("/")
                if leaf.startswith("run="):
                    parent = f"{head}/" if head else ""
                    successful.setdefault((sid, bucket, parent), set()).add(leaf[4:])

        needs = [
            b
            for b, sid, bucket, parent, run in info
            if not (runs := successful.get((sid, bucket, parent))) or run in runs
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
        capture_variables: bool = True,
        custom_metadata: dict[str, Any] | None = None,
        datasets_dir: str | PathLike[str] | None = None,
        max_workers: int = 32,
    ) -> None:
        """Run `initialize` on every bundle across *max_workers* threads.

        All bundles share the other arguments; *metadata_factory* builds each
        bundle's `BundleMetadata` writer.  See `_run_batch` for failures.
        """
        if not bundles:
            return

        def _one(b: Bundle) -> None:
            b.initialize(
                input_bundles=input_bundles,
                capture_variables=capture_variables,
                custom_metadata=custom_metadata,
                metadata=metadata_factory(b),
                datasets_dir=datasets_dir,
            )

        _run_batch("initialize", _one, bundles, max_workers)

    @classmethod
    def finalize_batch(
        cls,
        bundles: list[Bundle],
        *,
        metadata_factory: Callable[[Bundle], BundleMetadata],
        status: str = "success",
        max_workers: int = 32,
    ) -> None:
        """Run `finalize` on every bundle across *max_workers* threads.

        *metadata_factory* builds each bundle's `BundleMetadata` writer.  See
        `_run_batch` for failures.
        """
        if not bundles:
            return

        def _one(b: Bundle) -> None:
            b.finalize(status=status, metadata=metadata_factory(b))

        _run_batch("finalize", _one, bundles, max_workers)

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        parts = [f"bucket={self.bucket!r}", f"root_prefix={self._root_path!r}"]
        for key, value in self._partitions.items():
            if value:
                parts.append(f"{key}={value!r}")
        return f"{type(self).__name__}({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Concrete bundle types
# ---------------------------------------------------------------------------


class SourceRawBundle(Bundle):
    """Raw data collected from a provider.

    Path structure: ``datasets/provider={provider}/resource={resource}/version={version}/run={run}``

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

    BUNDLE_TYPE = "source_raw"
    ROOT_PATH = "datasets"
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


class SourceIngestBundle(Bundle):
    """Ingested / normalised source data.

    Path structure: ``feeds/provider={provider}/resource={resource}/version={version}/run={run}``

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

    BUNDLE_TYPE = "source_ingest"
    ROOT_PATH = "feeds"
    PARTITION_KEYS = ("provider", "resource", "version", "run")
    _EXTRA_DIRS = ("changelog", "metrics", "metrics_summary")

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

        As in `Bundle.from_path`, the URI path is the bundle root; an empty
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

    @property
    def changelog_uri(self) -> str:
        return f"{self.base_uri}/changelog"

    @property
    def changelog_prefix(self) -> str:
        return f"{self.base_prefix}/changelog"

    @property
    def metrics_uri(self) -> str:
        return f"{self.base_uri}/metrics"

    @property
    def metrics_prefix(self) -> str:
        return f"{self.base_prefix}/metrics"

    @property
    def metrics_summary_uri(self) -> str:
        return f"{self.base_uri}/metrics_summary"

    @property
    def metrics_summary_prefix(self) -> str:
        return f"{self.base_prefix}/metrics_summary"


class ThemeAssembleBundle(Bundle):
    """Assembled theme data from multiple corpus sources."""

    BUNDLE_TYPE = "theme_assemble"
    ROOT_PATH = "theme_stage"
    PARTITION_KEYS = ("theme", "run")
    _EXTRA_DIRS = ("changelog", "metrics", "metrics_summary")

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

    @property
    def changelog_uri(self) -> str:
        return f"{self.base_uri}/changelog"

    @property
    def changelog_prefix(self) -> str:
        return f"{self.base_prefix}/changelog"

    @property
    def metrics_uri(self) -> str:
        return f"{self.base_uri}/metrics"

    @property
    def metrics_prefix(self) -> str:
        return f"{self.base_prefix}/metrics"

    @property
    def metrics_summary_uri(self) -> str:
        return f"{self.base_uri}/metrics_summary"

    @property
    def metrics_summary_prefix(self) -> str:
        return f"{self.base_prefix}/metrics_summary"


class ThemePromoteBundle(Bundle):
    """Promoted theme with supplemental artifacts."""

    BUNDLE_TYPE = "theme_promote"
    ROOT_PATH = "theme_promote"
    PARTITION_KEYS = ("theme", "schema", "run")
    _EXTRA_DIRS = (
        "bridgefiles",
        "pmtiles",
        "changelog",
        "metrics",
        "metrics_summary",
        "changelog_internal",
        "validation",
        "partition_bboxes",
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
    def bridgefiles_uri(self) -> str:
        return f"{self.base_uri}/bridgefiles"

    @property
    def bridgefiles_prefix(self) -> str:
        return f"{self.base_prefix}/bridgefiles"

    @property
    def pmtiles_uri(self) -> str:
        return f"{self.base_uri}/pmtiles"

    @property
    def pmtiles_prefix(self) -> str:
        return f"{self.base_prefix}/pmtiles"

    @property
    def changelog_uri(self) -> str:
        return f"{self.base_uri}/changelog"

    @property
    def changelog_prefix(self) -> str:
        return f"{self.base_prefix}/changelog"

    @property
    def metrics_uri(self) -> str:
        return f"{self.base_uri}/metrics"

    @property
    def metrics_prefix(self) -> str:
        return f"{self.base_prefix}/metrics"

    @property
    def metrics_summary_uri(self) -> str:
        return f"{self.base_uri}/metrics_summary"

    @property
    def metrics_summary_prefix(self) -> str:
        return f"{self.base_prefix}/metrics_summary"

    @property
    def changelog_internal_uri(self) -> str:
        return f"{self.base_uri}/changelog_internal"

    @property
    def changelog_internal_prefix(self) -> str:
        return f"{self.base_prefix}/changelog_internal"

    @property
    def partition_bboxes_uri(self) -> str:
        return f"{self.base_uri}/partition_bboxes"

    @property
    def partition_bboxes_prefix(self) -> str:
        return f"{self.base_prefix}/partition_bboxes"

    @property
    def validation_uri(self) -> str:
        return f"{self.base_uri}/validation"

    @property
    def validation_prefix(self) -> str:
        return f"{self.base_prefix}/validation"


class ReleaseCandidateBundle(Bundle):
    """Release candidate with supplemental artifacts."""

    BUNDLE_TYPE = "release_candidate"
    ROOT_PATH = "release_candidate"
    PARTITION_KEYS = ("release", "run")
    _EXTRA_DIRS = (
        "bridgefiles",
        "pmtiles",
        "changelog",
        "metrics",
        "metrics_summary",
        "partition_bboxes",
    )

    def __init__(
        self,
        bucket: str,
        release: str = "",
        run: str = "",
        namespace: str = "",
        root_path: str = "release_candidate",
        *,
        store: BundleStore | None = None,
    ) -> None:
        partitions = {"release": release, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=root_path,
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
        root_path: str = "release_candidate",
        *,
        store: BundleStore | None = None,
    ) -> ReleaseCandidateBundle:
        """Return the latest released run for *release*.

        Scans S3 for ``run=`` partitions under ``release={release}``,
        filtering to those that have both a ``success`` and a ``released``
        marker file.  Raises `PartitionResolutionError` when none is found.
        An explicit *store* is used for the scan and attached to the result.
        """
        scan_root = _compose_root(root_path, namespace)
        result = _resolve_partitions(
            bucket=bucket,
            root_path=scan_root,
            partition_keys=list(cls.PARTITION_KEYS),
            overrides={"release": release},
            required_markers=("success", "released"),
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
    def bridgefiles_uri(self) -> str:
        return f"{self.base_uri}/bridgefiles"

    @property
    def bridgefiles_prefix(self) -> str:
        return f"{self.base_prefix}/bridgefiles"

    @property
    def pmtiles_uri(self) -> str:
        return f"{self.base_uri}/pmtiles"

    @property
    def pmtiles_prefix(self) -> str:
        return f"{self.base_prefix}/pmtiles"

    @property
    def changelog_uri(self) -> str:
        return f"{self.base_uri}/changelog"

    @property
    def changelog_prefix(self) -> str:
        return f"{self.base_prefix}/changelog"

    @property
    def metrics_uri(self) -> str:
        return f"{self.base_uri}/metrics"

    @property
    def metrics_prefix(self) -> str:
        return f"{self.base_prefix}/metrics"

    @property
    def metrics_summary_uri(self) -> str:
        return f"{self.base_uri}/metrics_summary"

    @property
    def metrics_summary_prefix(self) -> str:
        return f"{self.base_prefix}/metrics_summary"

    @property
    def partition_bboxes_uri(self) -> str:
        return f"{self.base_uri}/partition_bboxes"

    @property
    def partition_bboxes_prefix(self) -> str:
        return f"{self.base_prefix}/partition_bboxes"

    @property
    def registry_uri(self) -> str:
        return f"{self.base_uri}/registry"

    @property
    def registry_prefix(self) -> str:
        return f"{self.base_prefix}/registry"


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
