"""The `BundleMetadata` writer protocol and input provenance from the Dataset catalogue."""

from __future__ import annotations

import logging
from os import PathLike
from typing import Any, Protocol

from overture_core.dataset.dataset import Dataset

log = logging.getLogger(__name__)


class BundleMetadata(Protocol):
    """Writer for a bundle's ``metadata.json``, injected into the lifecycle methods.

    Each instance is bound to one bundle's ``base_uri``.
    """

    def init(
        self,
        input_paths: list[str] | None = None,
        capture_variables: bool = True,
    ) -> Any:
        """Write ``metadata.json`` to the bundle; retention ages new runs by it."""

    def add(self, key: str, value: Any) -> Any: ...

    def read_dag_section(self) -> dict[str, Any]: ...

    def update_inputs(self, inputs: list[dict[str, Any]]) -> Any: ...

    def finalize(self, status: str = "success") -> Any: ...


def provenance_sections(
    dataset: Dataset, version: str = ""
) -> dict[str, dict[str, Any]]:
    """``provider`` and ``resource`` metadata sections for *dataset*.

    License artifacts credit any metadata dict that carries both.  A non-empty
    *version* is added to the resource section.
    """
    resource = dataset.resource_data
    if version:
        resource["version"] = version
    return {"provider": dataset.provider_data, "resource": resource}


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
            entry.update(provenance_sections(ds, version))
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
