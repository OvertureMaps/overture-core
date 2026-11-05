"""Hive-style partition paths: composing, ordering, resolving and walking them."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from urllib.parse import urlparse

from overture_core.bundle.store import BundleStore, _store_or_default
from overture_core.cloud.cloud import Partition

log = logging.getLogger(__name__)


class PartitionResolutionError(Exception):
    """No viable partition path was found during S3 partition resolution."""


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
    # Natively rendered templates can pass non-string values such as ints.
    overrides = {k: v if v is None else str(v) for k, v in (overrides or {}).items()}
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
