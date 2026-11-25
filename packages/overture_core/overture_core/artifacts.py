"""Artifacts derived from bundle metadata trees.

A MetadataArtifact reads a bundle's metadata.json and writes one or more
derived files alongside the bundle. Each subclass defines its own generation
logic; the base class provides the interface and the module exposes
tree-traversal utilities that any artifact can use.

This package registers no artifacts. To add one, subclass MetadataArtifact,
implement generate(), and pass an instance to register_artifact. Artifacts run
in registration order, so register one that reads another's output after it.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterator, TypeVar
from urllib.parse import urlparse

from overture_core.cloud.aws.object import get_object_bytes, put_object

C = TypeVar("C")


def has_keys(*keys: str) -> Callable[[Any], bool]:
    """Return a predicate that checks a dict contains all *keys*."""
    return lambda v: isinstance(v, dict) and all(k in v for k in keys)


def match_node(node: Any, pattern: dict[str, Any]) -> bool:
    """Test whether *node* satisfies every constraint in *pattern*.

    Each pattern value is either a literal (compared with ``==``) or a
    callable predicate applied to the node's value for that key.
    """
    if not isinstance(node, dict):
        return False
    for key, constraint in pattern.items():
        if key not in node:
            return False
        if callable(constraint):
            if not constraint(node[key]):
                return False
        elif node[key] != constraint:
            return False
    return True


def find_nodes(tree: Any, pattern: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield every dict in *tree* (recursively) that matches *pattern*."""
    if isinstance(tree, dict):
        if match_node(tree, pattern):
            yield tree
        for v in tree.values():
            yield from find_nodes(v, pattern)
    elif isinstance(tree, list):
        for item in tree:
            yield from find_nodes(item, pattern)


def find_nodes_with_context(
    tree: Any,
    pattern: dict[str, Any],
    accumulate: Callable[[dict[str, Any], C], C],
    initial: C,
) -> Iterator[tuple[dict[str, Any], C]]:
    """Like find_nodes, but threads a context value through ancestor dicts.

    On each dict visited during descent, *accumulate* is called with the dict
    and the parent's context; its return value becomes the context applied to
    that dict's match check and propagated to its children. Lists pass the
    context through unchanged.
    """
    if isinstance(tree, dict):
        context = accumulate(tree, initial)
        if match_node(tree, pattern):
            yield tree, context
        for v in tree.values():
            yield from find_nodes_with_context(v, pattern, accumulate, context)
    elif isinstance(tree, list):
        for item in tree:
            yield from find_nodes_with_context(item, pattern, accumulate, initial)


class MetadataArtifact(ABC):
    """An artifact derived from a bundle's metadata tree.

    Subclasses set name and implement generate().
    """

    name: str

    @abstractmethod
    def generate(self, metadata: dict[str, Any], bundle_uri: str) -> None:
        """Read *metadata* and write artifact file(s) under *bundle_uri*."""

    @staticmethod
    def _bucket_key(bundle_uri: str, filename: str) -> tuple[str, str]:
        parsed = urlparse(bundle_uri.rstrip("/"))
        prefix = parsed.path.strip("/")
        key = f"{prefix}/{filename}" if prefix else filename
        return parsed.netloc, key

    @classmethod
    def _write_json(cls, bundle_uri: str, filename: str, data: Any) -> None:
        """Write *data* as JSON to ``<bundle_uri>/<filename>`` on S3."""
        bucket, key = cls._bucket_key(bundle_uri, filename)
        body = json.dumps(data, indent=2, default=str)
        put_object(bucket, key, body.encode("utf-8"), content_type="application/json")

    @classmethod
    def _read_json(cls, bundle_uri: str, filename: str) -> Any:
        """Read JSON from ``<bundle_uri>/<filename>`` on S3."""
        bucket, key = cls._bucket_key(bundle_uri, filename)
        body = get_object_bytes(bucket, key)
        if body is None:
            raise FileNotFoundError(f"s3://{bucket}/{key}")
        return json.loads(body.decode("utf-8"))

    @classmethod
    def _write_text(
        cls,
        bundle_uri: str,
        filename: str,
        text: str,
        content_type: str = "text/plain",
    ) -> None:
        """Write *text* to ``<bundle_uri>/<filename>`` on S3."""
        bucket, key = cls._bucket_key(bundle_uri, filename)
        put_object(bucket, key, text.encode("utf-8"), content_type=content_type)


ARTIFACT_REGISTRY: dict[str, MetadataArtifact] = {}
"""Registered artifacts by name, in the order they run."""


def register_artifact(artifact: MetadataArtifact) -> MetadataArtifact:
    """Add *artifact* to `ARTIFACT_REGISTRY` under its name and return it.

    Raises ``ValueError`` if a different artifact already holds the name;
    re-registering the same instance is a no-op.
    """
    existing = ARTIFACT_REGISTRY.get(artifact.name)
    if existing is not None and existing is not artifact:
        raise ValueError(f"Artifact {artifact.name!r} is already registered")
    ARTIFACT_REGISTRY[artifact.name] = artifact
    return artifact
