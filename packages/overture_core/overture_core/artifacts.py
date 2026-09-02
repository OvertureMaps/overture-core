"""Artifacts derived from bundle metadata trees.

A MetadataArtifact reads a bundle's metadata.json and writes one or more
derived files alongside the bundle. Each subclass defines its own generation
logic; the base class provides the interface and the module exposes
tree-traversal utilities that any artifact can use.

To add an artifact, subclass MetadataArtifact, implement generate(), and
register an instance in ARTIFACT_REGISTRY.
"""

from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterator, TypeVar
from urllib.parse import urlparse

from overture_core.cloud.aws.object import get_object_bytes, put_object

log = logging.getLogger(__name__)

_THEME_IN_PATH = re.compile(r"(?:^|/)theme=([^/]+)")

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


def _accumulate_themes(node: dict[str, Any], themes: frozenset[str]) -> frozenset[str]:
    """Augment *themes* with any ``theme=X`` segments in node's ``path``."""
    path = node.get("path")
    if isinstance(path, str):
        return themes | frozenset(_THEME_IN_PATH.findall(path))
    return themes


class LicenseArtifact(MetadataArtifact):
    """
    Extract license/attribution info from all input entries.
    The way these fields are represented + extracted is coupled to the structure
    of the sources in airflow/dags/configs/datasets/*.json
    """

    name = "license"

    _PATTERN = {
        "provider": has_keys("label", "name"),
        "resource": has_keys("label", "name"),
    }

    def generate(self, metadata: dict[str, Any], bundle_uri: str) -> None:
        # Merge entries with the same provider+resource so a license that
        # applies to multiple themes lists them all. Keyed on labels (the
        # stable Dataset.dataset_id components), not display names.
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for node, themes in find_nodes_with_context(
            metadata, self._PATTERN, _accumulate_themes, frozenset()
        ):
            entry = self._extract(node)
            key = (entry["provider_label"], entry["resource_label"])
            existing = by_key.get(key)
            if existing is None:
                entry["themes"] = sorted(themes)
                by_key[key] = entry
            else:
                existing["themes"] = sorted(set(existing["themes"]) | themes)

        entries = list(by_key.values())
        if not entries:
            log.info("No license data found in metadata for %s", bundle_uri)
            return

        self._write_json(bundle_uri, "license.json", entries)
        log.info(
            "Wrote %d license entries to %s/license.json", len(entries), bundle_uri
        )

    @staticmethod
    def _extract(node: dict[str, Any]) -> dict[str, Any]:
        provider = node["provider"]
        resource = node["resource"]
        collection = resource.get("collection", {})
        license_data = collection.get("license", {})
        coverage = collection.get("coverage", {})

        return {
            "provider_name": provider["name"],
            "provider_label": provider["label"],
            "provider_url": provider.get("url", {}).get("primary", ""),
            "resource_name": resource["name"],
            "resource_label": resource["label"],
            "license_type": license_data.get("type", ""),
            "license_url": license_data.get("url", {}).get("primary", ""),
            "requires_attribution": license_data.get("requires_attribution", False),
            "attribution": license_data.get("attribution", ""),
            "coverage_description": coverage.get("description", ""),
            "coverage_areas": [
                a.get("iso_3166_1", "") for a in coverage.get("areas", [])
            ],
        }


def render_attribution_mdx(entries: list[dict[str, Any]]) -> str:
    """Render license entries as the body of an attribution.mdx file.

    Produces one ``<details>`` block per theme, with a bullet list of
    sources inside. Entries with multiple themes appear under each.
    Entries with no themes are skipped.
    """
    by_theme: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        for theme in entry.get("themes") or []:
            by_theme.setdefault(theme, []).append(entry)

    sections: list[str] = []
    for theme in sorted(by_theme):
        title = theme.replace("_", " ").title()
        bullets = "\n".join(
            _render_attribution_bullet(e)
            for e in sorted(by_theme[theme], key=lambda e: e["resource_name"])
        )
        sections.append(
            "<details>\n"
            "    <summary>\n"
            f"### {title}\n"
            "    </summary>\n"
            f"{bullets}\n"
            "</details>"
        )
    return "\n\n".join(sections) + "\n" if sections else ""


def _render_attribution_bullet(entry: dict[str, Any]) -> str:
    if entry.get("requires_attribution") and entry.get("attribution"):
        name = entry["attribution"]
    else:
        name = entry.get("resource_name", "")
    url = entry.get("provider_url") or ""
    license_type = entry.get("license_type") or ""
    license_url = entry.get("license_url") or ""

    name_part = f"[{name}]({url})" if url else name
    if license_type and license_url:
        license_part = f" Available under [{license_type}]({license_url})."
    elif license_type:
        license_part = f" Available under {license_type}."
    else:
        license_part = ""
    return f"- {name_part}.{license_part}"


class AttributionArtifact(MetadataArtifact):
    """Render an attribution.mdx body from the bundle's license.json."""

    name = "attribution"

    def generate(self, metadata: dict[str, Any], bundle_uri: str) -> None:
        try:
            entries = self._read_json(bundle_uri, "license.json")
        except Exception as exc:
            log.info(
                "Skipping attribution.mdx for %s: cannot read license.json (%s)",
                bundle_uri,
                exc,
            )
            return

        body = render_attribution_mdx(entries or [])
        if not body:
            log.info("No themed entries; skipping attribution.mdx for %s", bundle_uri)
            return

        self._write_text(
            bundle_uri, "attribution.mdx", body, content_type="text/markdown"
        )
        log.info("Wrote attribution.mdx to %s", bundle_uri)


ARTIFACT_REGISTRY: dict[str, MetadataArtifact] = {
    "license": LicenseArtifact(),
    "attribution": AttributionArtifact(),
}
