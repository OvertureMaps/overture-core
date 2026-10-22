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
- `ReleaseBundle` -- final published release.
- `ScratchBundle` -- ephemeral debug/intermediate output, labeled by purpose.

Framework-agnostic model
------------------------
This module has no dependency on Airflow, Jinja, environment variables, or
any deployment-specific bucket names; S3 access goes through
`overture_core.cloud.aws.object`.  Everything deployment-specific is passed
in explicitly:

- ``bucket`` and ``namespace`` are plain constructor arguments.  A non-empty
  ``namespace`` is prepended to the root path (``<namespace>/<root>``).
- `Bundle.initialize` / `Bundle.finalize` take a `BundleMetadata` writer
  (and the batch variants a ``metadata_factory``) instead of building one.
- `Bundle.delete` / `Bundle.cleanup_all` take an optional `TableDropper`
  to drop any catalogue tables registered for a bundle.
- `Bundle.initialize` takes the ``datasets_dir`` used to enrich input
  provenance from the ``overture_core`` Dataset catalogue.
- `Bundle.needs_processing` checks a rendered URI (``self.base_uri`` or an
  explicit ``base_uri``).

Orchestrator-specific adapters subclass these classes to supply default
buckets, a per-user namespace, a metadata writer, and a table dropper (e.g.
tf-data-platform's Airflow adapter, ``overture_airflow.bundle_model``).
"""

from __future__ import annotations

import datetime
import logging
import re
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from os import PathLike
from typing import Any, Protocol
from urllib.parse import urlparse

from overture_core.cloud.aws.object import (
    delete_prefix,
    delete_success_file,
    iter_keys,
    list_common_prefixes,
    object_exists,
    object_last_modified,
    parse_s3_uri,
    prefix_exists,
    write_success_file,
)
from overture_core.cloud.cloud import Partition
from overture_core.dataset.dataset import Dataset

log = logging.getLogger(__name__)


class PartitionResolutionError(Exception):
    """No viable partition path was found during S3 partition resolution."""


class BundleMetadata(Protocol):
    """Writer for a bundle's ``metadata.json``, injected into the lifecycle methods.

    Each instance is bound to one bundle's ``base_uri``.  tf-data-platform's
    Airflow adapter supplies ``overture_airflow.metadata.Metadata``.
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


def _compose_root(root_path: str, namespace: str = "") -> str:
    """Join an optional *namespace* and *root_path* into a bundle root prefix."""
    root = root_path.strip("/")
    namespace = (namespace or "").strip("/")
    return f"{namespace}/{root}" if namespace else root


def _enrich_inputs_from_dataset(
    metadata: BundleMetadata,
    input_bundles: list[dict[str, str]],
    datasets_dir: str | PathLike[str],
) -> None:
    """Enrich input entries with Dataset catalogue information.

    For every input that does not already carry top-level ``provider`` and
    ``resource`` sections and whose corresponding *input_bundles* entry
    carries ``provider`` and ``resource``, look up the Dataset under
    *datasets_dir* and store the full provider and resource sections so
    downstream consumers have provenance data.
    """
    # Build a lookup: path -> bundle info dict
    bundle_lookup = {b["path"]: b for b in input_bundles}

    section = metadata.read_dag_section()
    inputs = section.get("inputs", [])
    modified = False

    for entry in inputs:
        if "provider" in entry and "resource" in entry:
            continue  # already carries catalogue provenance — nothing to enrich
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
) -> dict[str, str]:
    """Resolve partition values by scanning S3 directories.

    For each key in *partition_keys* (processed in order), if the
    corresponding entry in *overrides* is missing or ``"latest"``,
    the latest ``key=<value>`` directory is discovered under the
    current scan prefix.  Otherwise the override value is used
    directly.

    *required_markers* names marker files that must all be present at the
    resolved leaf.  Defaults to ``("success",)``.  Pass ``()`` to require
    only that the directory exists.

    Non-terminal partitions use backtracking: if the latest candidate
    at one level leads to no viable path at deeper levels, older
    candidates are tried.

    Returns a dict mapping each partition key to its resolved value.
    """
    overrides = overrides or {}

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
        dirs = list_common_prefixes(bucket, prefix)
        log.debug(
            "Listed %d subdirs under s3://%s/%s: %s", len(dirs), bucket, prefix, dirs
        )
        return dirs

    def _is_valid_leaf(prefix: str) -> bool:
        """Check whether *prefix* is a valid resolved leaf."""
        for marker in required_markers:
            if not object_exists(bucket, f"{prefix}{marker}"):
                log.debug("Marker %r not found at s3://%s/%s", marker, bucket, prefix)
                return False
        if not required_markers:
            # No marker requirements — any existing content counts.
            exists = prefix_exists(bucket, prefix)
            log.debug("Directory exists at s3://%s/%s: %s", bucket, prefix, exists)
            return exists
        return True

    def _resolve(prefix: str, keys: list[str]) -> dict[str, str] | None:
        """Recursively resolve *keys* under *prefix*.

        Returns a dict of resolved values, or ``None`` if no viable
        path exists.
        """
        if not keys:
            # All keys resolved — verify the leaf is valid.
            valid = _is_valid_leaf(prefix)
            log.debug("Leaf s3://%s/%s valid=%s", bucket, prefix, valid)
            return {} if valid else None

        key = keys[0]
        remaining = keys[1:]
        is_last = len(remaining) == 0
        override = overrides.get(key, "latest")

        if override and override.lower() != "latest":
            # Explicit value — descend directly.
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

        # "latest" mode — try candidates from newest to oldest.
        candidates = [d for d in _list_subdirs(prefix) if d.startswith(f"{key}=")]
        log.info(
            "Scanning %d candidate(s) for %s= under s3://%s/%s",
            len(candidates),
            key,
            bucket,
            prefix,
        )
        for cand in sorted(candidates, reverse=True):
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
        return None  # nothing found at this level

    base_prefix = f"{root_path.rstrip('/')}/"
    result = _resolve(base_prefix, list(partition_keys))

    if result is None:
        # Build a human-readable error message.
        first_key = partition_keys[0]
        ov = overrides.get(first_key, "latest")
        markers_desc = ", ".join(repr(m) for m in required_markers) or "none"
        if ov and ov.lower() != "latest":
            raise PartitionResolutionError(
                f"Override {first_key}='{ov}' missing required markers [{markers_desc}] "
                f"in s3://{bucket}/{base_prefix}"
            )
        raise PartitionResolutionError(
            f"No {first_key}= directory with required markers [{markers_desc}] found under "
            f"s3://{bucket}/{base_prefix}"
        )

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
) -> Iterator[dict[str, str]]:
    """Yield every ``{key: value}`` combination present under *root_prefix*.

    Walks Hive-style ``key=value`` directories in ``partition_keys`` order.
    A bundle is considered present whenever *all* partition keys have been
    matched (regardless of marker files); success-status filtering happens
    in ``is_deletable``.
    """

    def _walk(prefix: str, remaining: list[str], acc: dict[str, str]):
        if not remaining:
            yield dict(acc)
            return
        key = remaining[0]
        rest = remaining[1:]
        for name in list_common_prefixes(bucket, prefix):
            if not name.startswith(f"{key}="):
                continue
            value = name.split("=", 1)[1]
            if not value:
                continue
            child_prefix = f"{prefix}{name}/"
            acc[key] = value
            yield from _walk(child_prefix, rest, acc)
            acc.pop(key, None)

    yield from _walk(f"{root_prefix.rstrip('/')}/", partition_keys, {})


# ---------------------------------------------------------------------------
# Base Bundle class
# ---------------------------------------------------------------------------


class Bundle:
    """Base data bundle on S3.

    Every bundle has a ``data`` directory, a ``metadata.json`` file, and a
    ``success`` marker.  Subclasses add extra named directories.

    The path is composed of ``root_path`` followed by Hive-style
    ``key=value`` segments for each entry in the ``partitions`` dict.
    Partition values that are empty strings are omitted from the path.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    root_path : str
        S3 key prefix before any partition segments
        (e.g. ``"datasets"``, ``"feeds"``).
    partitions : dict[str, str] | None
        Ordered mapping of partition key to value.  Each non-empty
        entry becomes a ``key=value`` path segment.
    namespace : str
        Optional prefix prepended to ``root_path`` (``<namespace>/<root>``),
        e.g. to isolate a developer's bundles.  Empty means no prefix.
    """

    BUNDLE_TYPE: str = "bundle"

    # S3 key prefix (before any namespace) where bundles of this type are rooted.
    # Subclasses override.  Empty means the subclass does not participate in
    # generic discovery / cleanup.
    ROOT_PATH: str = ""

    # Ordered tuple of partition key names appended as Hive segments.
    # Subclasses override.
    PARTITION_KEYS: tuple[str, ...] = ()

    # Additional directory names beyond ``data``.  Subclasses override this.
    _EXTRA_DIRS: tuple[str, ...] = ()

    def __init__(
        self,
        bucket: str,
        root_path: str,
        partitions: dict[str, str] | None = None,
        namespace: str = "",
    ) -> None:
        self.bucket = bucket
        self._partitions: dict[str, str] = dict(partitions) if partitions else {}
        self._root_path = _compose_root(root_path, namespace)

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
    ) -> Bundle:
        """Create a Bundle from a full ``s3://bucket/root_path`` URI.

        Parameters
        ----------
        base_path : str
            Full S3 URI of the bundle root.
        partitions : dict[str, str] | None
            Optional partition key/value pairs.
        """
        parsed = urlparse(base_path)
        bucket = parsed.netloc
        root_path = parsed.path.lstrip("/")
        return cls(
            bucket=bucket,
            root_path=root_path,
            partitions=partitions,
            namespace="",
        )

    @classmethod
    def from_base_uri(cls, base_uri: str) -> Bundle:
        """Create a Bundle by parsing a full partitioned ``s3://`` URI.

        Splits Hive-style ``key=value`` segments off the end of the path
        into partitions; everything before them is the bundle root.
        ``s3://b/theme_promote/theme=places/run=X`` gives a Bundle with
        ``root_uri`` ``s3://b/theme_promote`` and partitions
        ``{"theme": "places", "run": "X"}``.

        Raises ValueError for a non-partition segment after partition
        segments (an ambiguous root).
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
        return cls(
            bucket=parsed.netloc,
            root_path="/".join(root_segments),
            partitions=partitions,
            namespace="",
        )

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

    def prefix_through_partition(self, last_partition: str) -> str:
        """S3 key prefix through (up to and including) the specified partition."""
        if last_partition not in self._partitions.keys():
            raise ValueError(f"Partition '{last_partition}' is not a partition")
        prefix = self._root_path
        for key, value in self._partitions.items():
            if not value:
                raise ValueError(
                    f"Partition '{key}' has no value — call resolve_partitions() "
                    f"before accessing path properties on {type(self).__name__!r}."
                )
            prefix = f"{prefix}/{key}={value}"
            if key == last_partition:
                break
        return prefix

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
        prefix = (
            f"s3://{self.bucket}/{self._root_path}" if self.bucket else self._root_path
        )
        for key, value in self._partitions.items():
            if not value:
                raise ValueError(
                    f"Partition '{key}' has no value — call resolve_partitions() "
                    f"before accessing path properties on {type(self).__name__!r}."
                )
            prefix = f"{prefix}/{key}={value}"
        return prefix

    @property
    def base_prefix(self) -> str:
        """S3 key prefix (no ``s3://bucket/``) including partition segments."""
        prefix = self._root_path
        for key, value in self._partitions.items():
            if not value:
                raise ValueError(
                    f"Partition '{key}' has no value — call resolve_partitions() "
                    f"before accessing path properties on {type(self).__name__!r}."
                )
            prefix = f"{prefix}/{key}={value}"
        return prefix

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
    # Plain Python lifecycle methods
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
        delete_success_file(self.base_uri)

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
        write_success_file(self.base_uri)

    def resolve_partitions(
        self,
        overrides: dict[str, str] | None = None,
        required_markers: tuple[str, ...] = ("success",),
    ) -> None:
        """Resolve unset partition values by scanning S3 and update this bundle in place.

        After this call, ``self.base_uri`` reflects the fully-resolved path.

        Parameters
        ----------
        overrides : dict[str, str] | None
            Explicit partition values.  Missing keys or values equal to
            ``"latest"`` trigger automatic S3 resolution.
        required_markers : tuple[str, ...]
            Marker files the resolved directory must contain.  Defaults to
            ``("success",)``.  Pass ``()`` to require only existence.
        """
        partition_keys = list(self._partitions.keys())

        effective_overrides: dict[str, str] = {}
        for key in partition_keys:
            val = self._partitions.get(key, "")
            if val:
                effective_overrides[key] = val
        if overrides:
            effective_overrides.update(overrides)

        scan_prefix = self._root_path
        keys_to_resolve: list[str] = []
        for key in partition_keys:
            if key in effective_overrides:
                keys_to_resolve = partition_keys[partition_keys.index(key) :]
                break
            existing = self._partitions.get(key, "")
            if existing:
                scan_prefix = f"{scan_prefix}/{key}={existing}"
            else:
                keys_to_resolve = partition_keys[partition_keys.index(key) :]
                break

        if not keys_to_resolve:
            return

        resolve_overrides = {
            k: effective_overrides.get(k, "latest") for k in keys_to_resolve
        }

        result = _resolve_partitions(
            bucket=self.bucket,
            root_path=scan_prefix,
            partition_keys=keys_to_resolve,
            overrides=resolve_overrides,
            required_markers=required_markers,
        )

        for key, val in result.items():
            self._partitions[key] = val

    def resolve_previous_partition(
        self,
        partition_key: str = "version",
        required_markers: tuple[str, ...] = ("success",),
        same_value_fallback: bool = False,
    ) -> str | None:
        """Find the most recent usable prior *partition_key* and return its data URI.

        Thin wrapper over `find_previous_partition` for callers that only
        need the URI.  Returns ``None`` both when no prior partition exists and
        when none of them are usable — use `find_previous_partition` when
        those two cases must be told apart, or when the caller needs to know
        which kind of baseline it got back.
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

        Scans S3 for all values of *partition_key* under the current bundle's
        prefix and walks them newest-first, returning the first whose remaining
        partitions (e.g. ``run``) resolve and whose *required_markers* are all
        present.  A prior value missing its markers is skipped rather than
        ending the search, so one missing success marker doesn't hide an older
        good baseline behind it.

        When *same_value_fallback* is set and nothing older is usable, the
        search widens to the sub-partitions of the **current** value — the
        newest successful earlier ``run=`` under the current ``version=``, say.
        That is a different comparison (same source snapshot, different
        pipeline run), so it is reported back as ``baseline_kind="same"`` and
        callers are expected to label it rather than pass it off as a
        version-over-version diff.  Retention makes this worth having: bucket
        expiration removes older ``version=`` prefixes long before the current
        version's earlier runs age out.

        Returns a dict with:

        ``data_uri``
            ``data/`` URI of the resolved bundle, or ``None`` if nothing usable
            was found.
        ``prior_values``
            Every prior value found, newest first — empty only when this is
            genuinely the first partition.
        ``rejected``
            Prior values that were skipped for lack of a marker or a resolvable
            run.  ``data_uri is None`` with a non-empty ``rejected`` means prior
            data exists but none of it was marked successful, which is an
            anomaly rather than a first run.
        ``baseline_kind``
            ``"previous"`` when ``data_uri`` came from an older *partition_key*
            value, ``"same"`` when it came from the same-value fallback, and
            ``None`` when nothing was found.

        Parameters
        ----------
        partition_key : str
            The partition to find the previous value for (default ``"version"``).
        required_markers : tuple[str, ...]
            Marker files the resolved directory must contain.  Defaults to
            ``("success",)``.  Pass ``()`` to require only existence.
        same_value_fallback : bool
            When no older value is usable, fall back to the newest successful
            earlier sub-partition under the current value.  Off by default:
            the two baselines answer different questions, so a caller has to
            opt in and handle the distinction.
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

        # Build scan prefix up to (but not including) the target partition
        scan_prefix = f"{self._root_path}/"
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

        def _list_values(prefix: str, key: str) -> list[str]:
            """Every value present for ``key=`` immediately under *prefix*."""
            values: list[str] = []
            for name in list_common_prefixes(self.bucket, prefix):
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
                # Target is the last partition — just check for the markers.
                leaf_prefix = f"{base_prefix}{key}={value}/"
                for marker in required_markers:
                    if not object_exists(self.bucket, f"{leaf_prefix}{marker}"):
                        log.info("Marker %r absent for %s=%s.", marker, key, value)
                        return None
                return f"s3://{self.bucket}/{leaf_prefix}data"

            # Resolve remaining partitions using the standard resolver.
            # _resolve_partitions raises rather than returning None when nothing
            # under this value has the required markers; that's this value being
            # unusable, not a fatal error, so we swallow it and try an older one.
            resolve_prefix = f"{base_prefix}{key}={value}"
            try:
                result = _resolve_partitions(
                    bucket=self.bucket,
                    root_path=resolve_prefix,
                    partition_keys=keys_after,
                    overrides={k: "latest" for k in keys_after},
                    required_markers=required_markers,
                )
            except PartitionResolutionError as exc:
                log.info("No successful run under %s=%s (%s).", key, value, exc)
                return None
            if result is None:
                log.info("No successful run found for %s=%s.", key, value)
                return None

            suffix = "/".join(
                Partition(key=k, delimiter="=", value=v).build()
                for k, v in result.items()
            )
            return f"s3://{self.bucket}/{resolve_prefix}/{suffix}/data"

        def _same_value_baseline() -> str | None:
            """Newest usable earlier sub-partition under the *current* value.

            Only reached when nothing older than *current_value* survived —
            typically because bucket expiration removed it while the current
            value's earlier runs are still around.  Compares the same source
            snapshot across two pipeline runs, which is a different measurement
            from version-over-version churn; the caller labels it as such.
            """
            if not remaining_keys:
                return None
            sibling_key = remaining_keys[0]
            deeper_keys = remaining_keys[1:]
            current_prefix = f"{scan_prefix}{partition_key}={current_value}/"
            # Only look *behind* the current run — the bundle being written now
            # is not its own baseline.
            current_sibling = self._partitions.get(sibling_key, "")
            siblings = sorted(
                (
                    v
                    for v in _list_values(current_prefix, sibling_key)
                    if not current_sibling or v < current_sibling
                ),
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

        # Every prior value, newest first — the order we walk them in.
        candidates = _list_values(scan_prefix, partition_key)
        prior_values = sorted(
            (v for v in candidates if v < current_value), reverse=True
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
        _, key = parse_s3_uri(self.success_uri)
        return object_exists(self.bucket, key)

    def age_days(self, now: datetime.datetime | None = None) -> float | None:
        """Age in days from S3 LastModified of the success (or metadata) file.

        Returns None if neither file exists.  ``now`` is injectable for tests.
        """
        for uri in (self.success_uri, self.metadata_uri):
            _, key = parse_s3_uri(uri)
            last_modified = object_last_modified(self.bucket, key)
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
    ) -> tuple[bool, str]:
        """Decide whether this bundle can be deleted, given peers in its partition group.

        ``siblings`` should be all other bundles sharing this bundle's
        ``retention_group_key()``.  This bundle may also appear in the list
        (it is filtered out here).

        Rules (see OvertureMaps/tf-data-platform#4428):

        1. Identify ``latest_successful`` = highest ``retention_order_key()``
           bundle in the group that has a success file.  If none exists, nothing
           is deletable.
        2. The ``latest_successful`` bundle itself is never deletable.
        3. Successful bundle: deletable iff it is ordered before the latest
           successful bundle AND ``age_days >= min_age_days``.
        4. Non-successful bundle: deletable iff it is ordered before the latest
           successful bundle (age is ignored).
        """
        group = [b for b in siblings if b is not self] + [self]
        successful = [b for b in group if b.is_successful()]
        if not successful:
            return False, "no successful bundle exists in partition group"
        latest_successful = max(successful, key=lambda b: b.retention_order_key())
        latest_key = latest_successful.retention_order_key()
        self_key = self.retention_order_key()

        if self is latest_successful or (
            self_key == latest_key and self.is_successful()
        ):
            return False, "is the latest successful bundle"

        if self_key >= latest_key:
            return False, (
                f"partition order {self_key!r} is not older than "
                f"latest successful {latest_key!r}"
            )

        if not self.is_successful():
            return True, (
                f"unsuccessful; superseded by successful bundle {latest_key!r}"
            )

        age = self.age_days(now=now)
        if age is None:
            return False, "cannot determine bundle age"
        if age < min_age_days:
            return False, (f"age {age:.1f}d < min_age_days {min_age_days}d")
        return True, (
            f"successful but superseded by {latest_key!r}; "
            f"age {age:.1f}d >= {min_age_days}d"
        )

    def retention_order_key(self) -> tuple[str, ...]:
        """Return partition values used to decide which bundle is latest."""
        values = tuple(
            self._partitions[k]
            for k in _RETENTION_ORDER_PARTITION_KEYS
            if k in self._partitions
        )
        if values:
            return values
        # Fallback: deterministic order by partition key name
        return tuple(v for _k, v in sorted(self._partitions.items()))

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

        Idempotent: safe to call on an already-deleted bundle.

        When *table_dropper* is given it is called first to drop any tables
        registered for this bundle.  Table drops are best-effort: a failure
        is logged and the S3 delete still proceeds.

        Args:
            dry_run: If True, log actions without executing them.
            table_dropper: Optional `TableDropper` for registered tables.
        """
        base_prefix = self.base_prefix.strip("/")
        if not base_prefix:
            raise ValueError("Refusing to delete a bundle with an empty prefix")
        prefix = f"{base_prefix}/"

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
            count = sum(1 for _ in iter_keys(self.bucket, prefix))
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

        count = delete_prefix(self.bucket, base_prefix)
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
    ) -> list[Bundle]:
        """Discover all bundles of this type under the standard root prefix.

        Walks ``bucket/<namespace>/<ROOT_PATH>`` expanding every
        ``key=value`` combination for the ordered ``PARTITION_KEYS``.
        Returns instances constructed via `Bundle.from_path`, so the
        results are always concrete ``Bundle`` objects (subclass-specific
        constructor arguments are not required).

        Subclasses without a defined ``ROOT_PATH`` / ``PARTITION_KEYS``
        cannot be discovered generically and must override this method.
        """
        if not cls.ROOT_PATH or not cls.PARTITION_KEYS:
            raise NotImplementedError(
                f"{cls.__name__} does not define ROOT_PATH / PARTITION_KEYS "
                "and must override discover_all()."
            )

        scan_root = _compose_root(cls.ROOT_PATH, namespace)

        results: list[Bundle] = []
        root_uri = f"s3://{bucket}/{scan_root}"
        for partitions in _walk_partitions(
            bucket=bucket,
            root_prefix=scan_root,
            partition_keys=list(cls.PARTITION_KEYS),
        ):
            b = Bundle.from_path(root_uri, partitions=partitions)
            # Preserve the subclass BUNDLE_TYPE for logging / reporting.
            b.BUNDLE_TYPE = cls.BUNDLE_TYPE  # type: ignore[misc]
            results.append(b)
        return results

    @classmethod
    def cleanup_all(
        cls,
        bucket: str,
        min_age_days: float = 30,
        dry_run: bool = True,
        namespace: str = "",
        table_dropper: TableDropper | None = None,
        now: datetime.datetime | None = None,
    ) -> CleanupReport:
        """Discover, evaluate, and (optionally) delete superseded bundles.

        See `is_deletable` for the retention rules.  *table_dropper* is
        passed through to `delete` for each deleted bundle.
        """
        bundles = cls.discover_all(bucket=bucket, namespace=namespace)
        log.info(
            "Discovered %d bundles of type %s under s3://%s",
            len(bundles),
            cls.__name__,
            bucket,
        )

        # Group by stable partitions so different versions/runs of the same
        # logical bundle land together.
        groups: dict[tuple[tuple[str, str], ...], list[Bundle]] = {}
        for b in bundles:
            groups.setdefault(b.retention_group_key(), []).append(b)

        report = CleanupReport(bundle_type=cls.BUNDLE_TYPE, dry_run=dry_run)
        for key, group in groups.items():
            for b in group:
                deletable, reason = b.is_deletable(
                    min_age_days=min_age_days,
                    siblings=group,
                    now=now,
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

        Returns ``True`` (proceed) in any of these cases:

        - Any override value is not ``"latest"``.
        - The current run's directory already has a success file (this
          run_id was used before — allow reprocessing).
        - No ``run=`` directory under the bundle's version path has a
          success file.

        Returns ``False`` (skip downstream) only when all overrides are
        ``"latest"`` **and** a *different* run already completed
        successfully.

        Parameters
        ----------
        partitions : dict[str, str] | None
            Partition overrides.  If any value is not ``"latest"``,
            processing is forced regardless of S3 state.
        base_uri : str | None
            Fully rendered bundle URI to check.  Defaults to
            ``self.base_uri``; pass an explicit value when ``base_uri``
            contains placeholders that the caller has rendered.

        Returns
        -------
        bool
            ``True`` when processing should proceed.  ``False`` to
            short-circuit (skip) the downstream tasks.
        """
        s3_uri = base_uri if base_uri is not None else self.base_uri

        # If any override is not "latest", force reprocessing.
        for key, value in (partitions or {}).items():
            if value and value.lower() != "latest":
                log.info(
                    "Override %s='%s' set, forcing reprocessing of bundle at %s",
                    key,
                    value,
                    s3_uri,
                )
                return True

        # Extract the current run= value from the rendered URI so we can
        # detect a rerun of the same run_id.
        current_run_match = re.search(r"/run=([^/]+)", s3_uri)
        current_run = current_run_match.group(1) if current_run_match else None

        # Strip the run=<value> segment (and everything after) from the
        # URI so we can scan all run= directories.
        parent_uri = re.sub(r"/run=[^/]*(?:/.*)?$", "", s3_uri)

        parsed = urlparse(parent_uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/") + "/"

        # List all run= directories under the parent prefix and check each
        # for a success file.
        for name in list_common_prefixes(bucket, prefix):
            if not name.startswith("run="):
                continue
            child = f"{prefix}{name}"
            if prefix_exists(bucket, f"{child}/success"):
                run_val = name.split("=", 1)[1]
                if current_run and run_val == current_run:
                    log.info(
                        "Current run='%s' already has a success file — "
                        "allowing rerun at s3://%s/%s",
                        current_run,
                        bucket,
                        child,
                    )
                    return True
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
    ) -> dict[str, "Bundle"]:
        """Discover every resource under ``provider`` and return the latest bundle per resource.

        One paginated ``list_objects_v2`` at ``<root>/provider=<X>/`` walks all
        ``success`` markers, keeping the greatest ``(version, run)`` tuple per
        resource. Cheap alternative to instantiating one bundle per resource and
        calling `resolve_partitions` on each.

        Returned bundles are fully resolved (concrete ``version`` and ``run``).
        Only valid for bundle classes partitioned by ``(provider, resource, ...)``.
        """
        if cls.PARTITION_KEYS[:2] != ("provider", "resource"):
            raise TypeError(
                f"{cls.__name__} is not partitioned by (provider, resource)"
            )

        root = _compose_root(cls.ROOT_PATH, namespace)
        prefix = f"{root}/provider={provider}/"

        best: dict[str, tuple[str, str]] = {}
        for key in iter_keys(bucket, prefix):
            if not key.endswith("/success"):
                continue
            parts = {
                seg.split("=", 1)[0]: seg.split("=", 1)[1]
                for seg in key.split("/")
                if "=" in seg
            }
            resource = parts.get("resource")
            version = parts.get("version")
            run = parts.get("run")
            if not (resource and version and run):
                continue
            cur = best.get(resource)
            if cur is None or (version, run) > cur:
                best[resource] = (version, run)

        return {
            resource: cls(
                provider=provider,
                resource=resource,
                bucket=bucket,
                version=v,
                run=r,
                namespace=namespace,
            )
            for resource, (v, r) in best.items()
        }

    @classmethod
    def needs_processing_batch(
        cls,
        bundles: list[Bundle],
        partitions: dict[str, str] | None = None,
    ) -> list[Bundle]:
        """Batched equivalent of `needs_processing`.

        Returns the subset of ``bundles`` that need processing — same
        direction as the singular (bundles that would return ``True``).

        Instead of one S3 HEAD per bundle, groups candidates by their shared
        ``bucket + root + provider=<X>`` prefix and does one paginated
        ``list_objects_v2`` per group.

        Assumes ``base_uri`` values are already rendered (no unresolved
        placeholders). Callers constructing bundles with concrete
        ``bucket`` and ``version`` get this automatically.
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

        # Group candidates by (bucket, prefix-through-provider). Every
        # candidate in a group is a single paginated LIST.
        def _scan_root(b: Bundle) -> tuple[str, str]:
            parsed = urlparse(b.base_uri)
            key = parsed.path.lstrip("/")
            idx = key.find("resource=")
            prefix = key[:idx] if idx != -1 else key.rstrip("/") + "/"
            return parsed.netloc, prefix

        groups: dict[tuple[str, str], list[Bundle]] = {}
        for b in bundles:
            groups.setdefault(_scan_root(b), []).append(b)

        needs: list[Bundle] = []

        for (bucket, prefix), group in groups.items():
            completed: set[tuple[str, str]] = set()
            for k in iter_keys(bucket, prefix):
                if not k.endswith("/success"):
                    continue
                parts = {
                    seg.split("=", 1)[0]: seg.split("=", 1)[1]
                    for seg in k.split("/")
                    if "=" in seg
                }
                r, v = parts.get("resource"), parts.get("version")
                if r and v:
                    completed.add((r, v))
            for b in group:
                r = b._partitions.get("resource")
                v = b._partitions.get("version")
                if (r, v) not in completed:
                    needs.append(b)

        log.info(
            "needs_processing_batch: %d/%d bundle(s) need processing across "
            "%d scan group(s)",
            len(needs),
            len(bundles),
            len(groups),
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
        """Batched equivalent of `initialize`.

        Calls ``initialize`` on every bundle in ``bundles`` with the same
        shared arguments, parallelised across ``max_workers`` threads (each
        ``initialize`` is ~3 sequential S3 calls).  *metadata_factory* is
        called once per bundle to build its `BundleMetadata` writer.

        All bundles receive the same ``input_bundles``, ``capture_variables``,
        ``custom_metadata`` and ``datasets_dir``. Use the singular
        `initialize` when candidates need per-bundle metadata.
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

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_one, b) for b in bundles]
            for f in as_completed(futures):
                f.result()

    @classmethod
    def finalize_batch(
        cls,
        bundles: list[Bundle],
        *,
        metadata_factory: Callable[[Bundle], BundleMetadata],
        status: str = "success",
        max_workers: int = 32,
    ) -> None:
        """Batched equivalent of `finalize`.

        Calls ``finalize`` on every bundle in ``bundles`` with the same
        ``status``, parallelised across ``max_workers`` threads.
        *metadata_factory* is called once per bundle.  Each ``finalize`` is
        2 tiny S3 PUTs (metadata + success marker), so a large batch
        finishes in about the same time as one.
        """
        if not bundles:
            return

        def _one(b: Bundle) -> None:
            b.finalize(status=status, metadata=metadata_factory(b))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_one, b) for b in bundles]
            for f in as_completed(futures):
                f.result()

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
    ) -> None:
        partitions = {
            "provider": provider,
            "resource": resource,
            "version": version,
            "run": run,
        }
        super().__init__(
            bucket=bucket,
            root_path=root_path or self.ROOT_PATH,
            partitions=partitions,
            namespace=namespace,
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
    ) -> None:
        partitions = {
            "provider": provider,
            "resource": resource,
            "version": version,
            "run": run,
        }
        super().__init__(
            bucket=bucket,
            root_path=root_path or self.ROOT_PATH,
            partitions=partitions,
            namespace=namespace,
        )
        self.provider = provider
        self.resource = resource

    @classmethod
    def from_path(
        cls,
        base_path: str,
        provider: str,
        resource: str,
        version: str = "",
        run: str = "",
    ) -> SourceIngestBundle:
        """Create from a full ``s3://`` URI plus source identifiers."""
        parsed = urlparse(base_path)
        bucket = parsed.netloc
        return cls(
            bucket=bucket,
            provider=provider,
            resource=resource,
            version=version,
            run=run,
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
    ) -> None:
        partitions = {"theme": theme, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=root_path or self.ROOT_PATH,
            partitions=partitions,
            namespace=namespace,
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
    ) -> None:
        partitions = {"theme": theme, "schema": schema, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=root_path or self.ROOT_PATH,
            partitions=partitions,
            namespace=namespace,
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
    ) -> None:
        partitions = {"release": release, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=root_path,
            partitions=partitions,
            namespace=namespace,
        )

    @classmethod
    def resolve_latest_released(
        cls,
        release: str,
        bucket: str,
        namespace: str = "",
        root_path: str = "release_candidate",
    ) -> ReleaseCandidateBundle:
        """Return the latest released run for *release*.

        Scans S3 for ``run=`` partitions under ``release={release}``,
        filtering to those that have both a ``success`` and a ``released``
        marker file.  Raises `PartitionResolutionError` when none is found.
        """
        scan_root = _compose_root(root_path, namespace)
        result = _resolve_partitions(
            bucket=bucket,
            root_path=scan_root,
            partition_keys=["release", "run"],
            overrides={"release": release},
            required_markers=("success", "released"),
        )
        return cls(
            bucket=bucket,
            release=result["release"],
            run=result["run"],
            namespace=namespace,
            root_path=root_path,
        )

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


class ReleaseBundle(Bundle):
    """Final published release."""

    BUNDLE_TYPE = "release"


class ScratchBundle(Bundle):
    """Ephemeral debug or intermediate output, labeled by purpose and deletable any time.

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

    def __init__(
        self,
        purpose: str,
        bucket: str,
        run: str = "",
        namespace: str = "",
        root_path: str | None = None,
    ) -> None:
        partitions = {"purpose": purpose, "run": run}
        super().__init__(
            bucket=bucket,
            root_path=root_path or "scratch",
            partitions=partitions,
            namespace=namespace,
        )
