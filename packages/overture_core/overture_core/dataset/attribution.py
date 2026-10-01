"""License / attribution entries derived from dataset config files.

A *license entry* is the flat dict shape consumed by
``overture_core.artifacts.LicenseArtifact``. This module owns how such an
entry is extracted from a provider/resource pair and how a list of entries is
rendered as plain text or Markdown, so artifacts and the ``overture-datasets``
CLI share one definition.
"""

from __future__ import annotations

from typing import Any

from overture_core.dataset.schema import DatasetFile


def license_entry(provider: dict[str, Any], resource: dict[str, Any]) -> dict[str, Any]:
    """Flatten a provider dict plus one of its resource dicts into a license entry.

    Both arguments follow the dataset file shape (see ``schema.DatasetFile``);
    missing optional sections degrade to empty strings / lists rather than raising.
    """
    collection = resource.get("collection") or {}
    license_data = collection.get("license") or {}
    coverage = collection.get("coverage") or {}

    return {
        "provider_name": provider["name"],
        "provider_label": provider["label"],
        "provider_url": (provider.get("url") or {}).get("primary", ""),
        "resource_name": resource["name"],
        "resource_label": resource["label"],
        "license_type": license_data.get("type") or "",
        "license_url": (license_data.get("url") or {}).get("primary", ""),
        "requires_attribution": license_data.get("requires_attribution", False),
        "attribution": license_data.get("attribution", ""),
        "coverage_description": coverage.get("description", ""),
        "coverage_areas": [a.get("iso_3166_1", "") for a in coverage.get("areas", [])],
    }


def entries_from_file(
    dataset: DatasetFile, resources: set[str] | None = None
) -> list[dict[str, Any]]:
    """License entries for a validated dataset file.

    ``resources`` restricts output to the given resource labels; ``None``
    means every resource in the file. Unknown labels raise ``KeyError``.
    """
    provider = dataset.provider.model_dump()
    available = {r.label: r for r in dataset.resources}
    if resources is None:
        selected = list(dataset.resources)
    else:
        missing = sorted(resources - available.keys())
        if missing:
            raise KeyError(
                f"resource(s) {missing} not found in provider '{dataset.provider.label}'; "
                f"available: {sorted(available)}"
            )
        selected = [available[label] for label in sorted(resources)]
    return [license_entry(provider, r.model_dump()) for r in selected]


def _display_name(entry: dict[str, Any]) -> str:
    if entry.get("requires_attribution") and entry.get("attribution"):
        return entry["attribution"]
    return entry.get("resource_name", "")


def render_attribution_bullet(entry: dict[str, Any]) -> str:
    """One Markdown bullet: ``- [name](url). Available under [SPDX](license_url).``"""
    name = _display_name(entry)
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


def _sorted(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        entries, key=lambda e: (e["provider_name"].lower(), e["resource_name"].lower())
    )


def render_markdown(
    entries: list[dict[str, Any]], title: str = "Data Attribution"
) -> str:
    """Render entries as a Markdown document grouped by provider.

    Resources from one provider that render to an identical attribution line
    collapse to one bullet.
    """
    if not entries:
        return ""
    lines = [f"# {title}", ""]
    current_provider = None
    seen: set[str] = set()
    for entry in _sorted(entries):
        if entry["provider_label"] != current_provider:
            current_provider = entry["provider_label"]
            seen = set()
            if len(lines) > 2:
                lines.append("")
            lines.append(f"## {entry['provider_name']}")
            lines.append("")
        bullet = render_attribution_bullet(entry)
        if bullet not in seen:
            seen.add(bullet)
            lines.append(bullet)
    return "\n".join(lines) + "\n"


def render_text(entries: list[dict[str, Any]], title: str = "Data Attribution") -> str:
    """Render entries as plain text, one block per resource."""
    if not entries:
        return ""
    blocks = [title, "=" * len(title)]
    for entry in _sorted(entries):
        block = [
            "",
            _display_name(entry),
            f"  Provider: {entry['provider_name']}"
            + (f" <{entry['provider_url']}>" if entry.get("provider_url") else ""),
            f"  Resource: {entry['resource_name']}",
        ]
        if entry.get("license_type"):
            block.append(
                f"  License:  {entry['license_type']}"
                + (f" <{entry['license_url']}>" if entry.get("license_url") else "")
            )
        if entry.get("coverage_description"):
            block.append(f"  Coverage: {entry['coverage_description']}")
        blocks.extend(block)
    return "\n".join(blocks) + "\n"


RENDERERS = {
    "md": render_markdown,
    "txt": render_text,
}
