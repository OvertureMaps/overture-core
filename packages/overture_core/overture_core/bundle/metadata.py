"""The `BundleMetadata` writer protocol and caller-supplied input provenance."""

from __future__ import annotations

import logging
from typing import Any, Callable, Protocol, TypedDict

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


class Provenance(TypedDict):
    """The ``provider`` and ``resource`` metadata sections that credit a dataset.

    Metadata artifacts (e.g. license files) credit any metadata dict carrying
    both.  Their contents are the caller's; this package does not interpret them.
    """

    provider: dict[str, Any]
    resource: dict[str, Any]


ProvenanceResolver = Callable[[str, str], Provenance]
"""Returns the `Provenance` for a ``(provider, resource)`` label pair, or raises."""


def _with_version(provenance: Provenance, version: str = "") -> Provenance:
    """Copy *provenance*, adding a non-empty *version* to the resource section."""
    resource = dict(provenance["resource"])
    if version:
        resource["version"] = version
    return {"provider": dict(provenance["provider"]), "resource": resource}


def _enrich_inputs(
    metadata: BundleMetadata,
    input_bundles: list[dict[str, str]],
    resolver: ProvenanceResolver,
) -> None:
    """Add ``provider``/``resource`` sections from *resolver* to metadata inputs.

    An input is enriched when it lacks either section and its *input_bundles*
    entry names a provider and resource.  The entry's version, if any, is added
    to the resource section.  A resolver error is logged as a warning.
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
            entry.update(_with_version(resolver(provider, resource), version))
            modified = True
            log.info(
                "Enriched input %s with provenance for %s/%s", path, provider, resource
            )
        except Exception as exc:
            log.warning(
                "Could not resolve provenance for %s/%s: %s", provider, resource, exc
            )

    if modified:
        metadata.update_inputs(inputs)
