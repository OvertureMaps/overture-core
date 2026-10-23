"""Registry of bundle types, to go from an S3 path to a typed bundle.

Every registered type is a `Bundle` subclass whose class attributes
describe its layout:

- ``BUNDLE_TYPE`` -- the stage name recorded in ``metadata.json``.
- ``ROOT_PATH`` -- the key prefix its bundles live under.
- ``PARTITION_KEYS`` -- the ordered Hive partition keys below the root.
- ``COMPONENTS`` -- the `BundleComponent` entries under each bundle.

`BundleRegistry` indexes types by ``BUNDLE_TYPE`` and ``ROOT_PATH`` (they
differ for `ThemeAssembleBundle`: ``theme_assemble`` under ``theme_stage``).
`BundleRegistry.locate` places a path -- with or without a namespace
before the root, at any depth -- in a type's hierarchy as a
`BundleLocation`, and `BundleRegistry.bundle_from_uri` turns a bundle's
path into an instance of its type.

`DEFAULT_REGISTRY` holds the types in `overture_core.bundle.bundle` that
have a fixed root: `SourceRawBundle`, `SourceIngestBundle`,
`ThemeAssembleBundle`, `ThemePromoteBundle` and `ReleaseCandidateBundle`.
`ScratchBundle` has no ``ROOT_PATH`` and isn't registered.

Example
-------
>>> from overture_core.bundle.registry import DEFAULT_REGISTRY
>>> loc = DEFAULT_REGISTRY.locate("s3://bkt/ns/theme_stage/theme=places")
>>> loc.bundle_type.BUNDLE_TYPE, loc.namespace, dict(loc.partitions)
('theme_assemble', 'ns', {'theme': 'places'})
>>> loc.next_partition_key
'run'
>>> bundle = DEFAULT_REGISTRY.bundle_from_uri(
...     "s3://bkt/ns/theme_stage/theme=places/run=r1"
... )
>>> [c.listing_name for c in bundle.COMPONENTS]  # doctest: +NORMALIZE_WHITESPACE
['data/', 'metadata.json', 'success', 'changelog/', 'metrics/',
 'metrics_summary/', 'summary.md']
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from urllib.parse import urlparse

from overture_core.bundle.bundle import (
    Bundle,
    BundleComponent,
    ReleaseCandidateBundle,
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

    A location is *complete* when the path reaches a bundle (it has a value
    for every partition key) and *partial* when it stops above one, such as
    ``theme_stage/theme=places``.

    Attributes
    ----------
    bundle_type : type[Bundle]
        The registered type whose ``ROOT_PATH`` the path contains.
    bucket : str
        The S3 bucket, or ``""`` for a path given as a bare key.
    namespace : str
        The key segments before ``ROOT_PATH`` (``"ns"``, ``"a/b"``), or ``""``.
    partitions : Mapping[str, str]
        The partition values in the path, in ``PARTITION_KEYS`` order: the
        first few keys for a partial location, every key for a complete one.
    relative_path : str
        What follows a complete location's base path
        (``"data/part-0.parquet"``), or ``""``.
    """

    bundle_type: type[Bundle]
    bucket: str
    namespace: str = ""
    partitions: Mapping[str, str] = field(default_factory=dict, hash=False)
    relative_path: str = ""

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
        """URI of the location itself: root, partitions and relative path."""
        segments = [self.root_prefix]
        segments += [f"{key}={value}" for key, value in self.partitions.items()]
        if self.relative_path:
            segments.append(self.relative_path)
        return self._uri("/".join(segments))

    @property
    def is_complete(self) -> bool:
        """Whether the path has a value for every partition key."""
        return len(self.partitions) == len(self.bundle_type.PARTITION_KEYS)

    @property
    def next_partition_key(self) -> str | None:
        """The partition key one level down, or ``None`` when complete."""
        keys = self.bundle_type.PARTITION_KEYS
        return None if self.is_complete else keys[len(self.partitions)]

    @property
    def component(self) -> BundleComponent | None:
        """The component ``relative_path`` is in, or ``None``.

        ``None`` when there is no relative path, or when it isn't in one of
        the type's ``COMPONENTS`` (an unlisted entry, or a path below a
        file component).
        """
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
        """Return the bundle at this location, as its registered type.

        *store* is an optional storage backend for the bundle.  Raises
        ValueError for a partial location.
        """
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
    """An immutable set of bundle types, indexed by ``BUNDLE_TYPE`` and ``ROOT_PATH``.

    Iterating yields the types in registration order.

    Parameters
    ----------
    types : Iterable[type[Bundle]]
        `Bundle` subclasses, each with a ``ROOT_PATH`` and
        ``PARTITION_KEYS``.  Raises TypeError for anything else that isn't
        a `Bundle` subclass, and ValueError for a type without a root or
        partition keys, or for two types sharing a ``BUNDLE_TYPE`` or
        ``ROOT_PATH``.
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

    @property
    def types(self) -> tuple[type[Bundle], ...]:
        """The registered types, in registration order."""
        return self._types

    def get(self, name: str) -> type[Bundle]:
        """Return the type whose ``BUNDLE_TYPE`` or ``ROOT_PATH`` is *name*.

        Raises KeyError when no registered type has that name.
        """
        try:
            return self._by_name[name.strip("/")]
        except KeyError:
            raise KeyError(
                f"No registered bundle type {name!r}; expected one of "
                f"{sorted(self._by_name)}"
            ) from None

    def locate(self, path: str) -> BundleLocation | None:
        """Place *path* in a registered type's hierarchy.

        *path* is an ``s3://bucket/key`` URI or a bare key, at any depth:
        a type's root (``theme_stage``), a partial position
        (``theme_stage/theme=places``), a bundle, or an object inside one.
        Any segments before the root are the namespace.

        Scans the segments left to right for a registered root followed by
        that type's partitions, as ``key=value`` segments in
        ``PARTITION_KEYS`` order.  A root whose partitions stop short and
        are followed by other segments isn't a match (it is part of a
        namespace), and the scan moves on.  Returns ``None`` when no type
        matches.  Raises ValueError for a URI scheme other than ``s3``.
        """
        bucket, key = "", path
        if "://" in path:
            parsed = urlparse(path)
            if parsed.scheme != "s3":
                raise ValueError(f"Expected an s3:// URI or a key, got {path!r}")
            bucket, key = parsed.netloc, parsed.path
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
        """Return the bundle *uri* is in, as its registered type.

        *uri* is the bundle's base URI or any path inside the bundle (see
        `locate`).  *store* is an optional storage backend for the bundle.
        Raises ValueError when no registered type matches, or when *uri*
        stops above a bundle.
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
    ]
)
"""The bundle types in `overture_core.bundle.bundle` that have a fixed root."""
