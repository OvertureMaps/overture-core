"""Map S3 paths to bundle types.

`BundleRegistry` indexes `Bundle` subclasses by ``BUNDLE_TYPE`` and
``ROOT_PATH``, which differ for `ThemeAssembleBundle` (``theme_assemble``
under ``theme_stage``).  Published release paths (``release/<v>/``,
``changelog/<v>``, ``bridgefiles/<v>``, ``registry/<v>``) aren't bundles, so
`BundleRegistry.locate` returns ``None`` for them.

Example
-------
>>> from overture_core.bundle.registry import DEFAULT_REGISTRY
>>> loc = DEFAULT_REGISTRY.locate("s3://bkt/ns/theme_stage/theme=places")
>>> loc.bundle_type.BUNDLE_TYPE, loc.namespace, loc.next_partition_key
('theme_assemble', 'ns', 'run')
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field

from overture_core.bundle.bundle import (
    Bundle,
    BundleComponent,
    ReleaseCandidateBundle,
    ScratchBundle,
    SourceIngestBundle,
    SourceRawBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
    _compose_root,
)
from overture_core.bundle.store import BundleStore


def _segments(path: str) -> list[str]:
    return [segment for segment in path.split("/") if segment]


@dataclass(frozen=True)
class BundleLocation:
    """Where a path falls in a registered bundle type's layout.

    *Complete* when the path has every partition (it is at or inside a
    bundle); *partial* when it stops above one, e.g.
    ``theme_stage/theme=places``.
    """

    bundle_type: type[Bundle]
    bucket: str  # "" for a bare key
    namespace: str = ""  # key segments before ROOT_PATH
    partitions: Mapping[str, str] = field(default_factory=dict, hash=False)
    relative_path: str = ""  # path below a complete location's base path

    @property
    def root_prefix(self) -> str:
        """Key prefix of the type's root, including any namespace."""
        return _compose_root(self.bundle_type.ROOT_PATH, self.namespace)

    @property
    def root_uri(self) -> str:
        """URI of the type's root (the key prefix when there is no bucket)."""
        return self._uri(self.root_prefix)

    @property
    def uri(self) -> str:
        """URI of the location, including any relative path."""
        segments = [self.root_prefix]
        segments += [f"{key}={value}" for key, value in self.partitions.items()]
        if self.relative_path:
            segments.append(self.relative_path)
        return self._uri("/".join(segments))

    @property
    def is_complete(self) -> bool:
        return len(self.partitions) == len(self.bundle_type.PARTITION_KEYS)

    @property
    def next_partition_key(self) -> str | None:
        """The partition key one level down; ``None`` when complete."""
        keys = self.bundle_type.PARTITION_KEYS
        return None if self.is_complete else keys[len(self.partitions)]

    @property
    def component(self) -> BundleComponent | None:
        """The listed component ``relative_path`` is in, or ``None``."""
        name, _, rest = self.relative_path.partition("/")
        if not name:
            return None
        try:
            component = self.bundle_type.get_component(name)
        except KeyError:
            return None
        if rest and not component.is_directory:
            return None
        return component

    def bundle(self, *, store: BundleStore | None = None) -> Bundle:
        """Return the bundle at this location; raises ValueError if partial."""
        if not self.is_complete:
            raise ValueError(
                f"{self.uri} is above a {self.bundle_type.BUNDLE_TYPE} bundle: "
                f"it has no {self.next_partition_key!r} partition"
            )
        return self.bundle_type._from_root(
            self.bucket, self.root_prefix, dict(self.partitions), store=store
        )

    def _uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}" if self.bucket else key


class BundleRegistry:
    """An immutable set of bundle types, iterated in registration order.

    Each type needs a ``ROOT_PATH`` and ``PARTITION_KEYS``, and its
    ``BUNDLE_TYPE`` and ``ROOT_PATH`` must be unique in the registry.
    """

    def __init__(self, types: Iterable[type[Bundle]]) -> None:
        registered: list[type[Bundle]] = []
        by_name: dict[str, type[Bundle]] = {}
        for bundle_type in types:
            if not (isinstance(bundle_type, type) and issubclass(bundle_type, Bundle)):
                raise TypeError(f"Expected a Bundle subclass, got {bundle_type!r}")
            if bundle_type in registered:
                raise ValueError(f"{bundle_type.__name__} is registered twice")
            root = bundle_type.ROOT_PATH.strip("/")
            if not root or not bundle_type.PARTITION_KEYS:
                raise ValueError(
                    f"{bundle_type.__name__} needs a ROOT_PATH and PARTITION_KEYS "
                    "to be registered"
                )
            for name in {bundle_type.BUNDLE_TYPE, root}:
                other = by_name.setdefault(name, bundle_type)
                if other is not bundle_type:
                    raise ValueError(
                        f"{bundle_type.__name__} and {other.__name__} both use "
                        f"the name {name!r}"
                    )
            registered.append(bundle_type)
        self._types = tuple(registered)
        self._by_name = by_name
        # Longest root first, so "a/b" wins over "a" at the same position.
        self._by_root_length = sorted(
            self._types, key=lambda t: len(_segments(t.ROOT_PATH)), reverse=True
        )

    def __iter__(self) -> Iterator[type[Bundle]]:
        return iter(self._types)

    def __len__(self) -> int:
        return len(self._types)

    def __contains__(self, item: object) -> bool:
        """Whether *item*, a type or a ``BUNDLE_TYPE``/``ROOT_PATH``, is registered."""
        if isinstance(item, str):
            return item.strip("/") in self._by_name
        return item in self._types

    def __repr__(self) -> str:
        return f"{type(self).__name__}([{', '.join(t.__name__ for t in self)}])"

    def get(self, name: str) -> type[Bundle]:
        """Return the type whose ``BUNDLE_TYPE`` or ``ROOT_PATH`` is *name*.

        Raises KeyError if none matches.
        """
        try:
            return self._by_name[name.strip("/")]
        except KeyError:
            raise KeyError(
                f"No registered bundle type {name!r}; expected one of "
                f"{sorted(self._by_name)}"
            ) from None

    def locate(self, path: str) -> BundleLocation | None:
        """Place *path* in a registered type's hierarchy, or return ``None``.

        *path* is an ``s3://`` URI or a bare key at any depth: a root, a
        partial position, a bundle, or an object inside one.  Segments before
        the root are the namespace; so is a root followed by a non-partition
        segment before its last partition.  Raises ValueError for any scheme
        besides ``s3``.
        """
        bucket, key = "", path
        if "://" in path:
            # Split by hand: "#" and "?" are valid in S3 keys.
            scheme, _, rest = path.partition("://")
            if scheme.lower() != "s3":
                raise ValueError(f"Expected an s3:// URI or a key, got {path!r}")
            bucket, _, key = rest.partition("/")
        segments = _segments(key)
        for start in range(len(segments)):
            for bundle_type in self._by_root_length:
                root = _segments(bundle_type.ROOT_PATH)
                end = start + len(root)
                if segments[start:end] != root:
                    continue
                location = _match_partitions(
                    bundle_type, bucket, segments[:start], segments[end:]
                )
                if location is not None:
                    return location
        return None

    def bundle_from_uri(self, uri: str, *, store: BundleStore | None = None) -> Bundle:
        """Return the bundle *uri* is at or inside, as its registered type.

        Raises ValueError if *uri* is under no registered root or stops above
        a bundle.
        """
        location = self.locate(uri)
        if location is None:
            raise ValueError(
                f"{uri} is not under a registered bundle root; expected one of "
                f"{[t.ROOT_PATH for t in self]}"
            )
        return location.bundle(store=store)


def _match_partitions(
    bundle_type: type[Bundle],
    bucket: str,
    namespace: list[str],
    tail: list[str],
) -> BundleLocation | None:
    """Match *tail*, the segments after *bundle_type*'s root, to its partitions."""
    partitions: dict[str, str] = {}
    for key, segment in zip(bundle_type.PARTITION_KEYS, tail, strict=False):
        name, sep, value = segment.partition("=")
        if name != key or not sep or not value:
            break
        partitions[key] = value
    rest = tail[len(partitions) :]
    if rest and len(partitions) < len(bundle_type.PARTITION_KEYS):
        return None
    return BundleLocation(
        bundle_type=bundle_type,
        bucket=bucket,
        namespace="/".join(namespace),
        partitions=partitions,
        relative_path="/".join(rest),
    )


DEFAULT_REGISTRY = BundleRegistry(
    [
        SourceRawBundle,
        SourceIngestBundle,
        ThemeAssembleBundle,
        ThemePromoteBundle,
        ReleaseCandidateBundle,
        ScratchBundle,
    ]
)
"""Every bundle type in `overture_core.bundle.bundle`."""
