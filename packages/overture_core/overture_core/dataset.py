"""Dataset definition loader for pipeline configuration.

A *dataset* represents a specific data resource from a provider.  The
provider together with the resource uniquely identifies a dataset (e.g.
``osm`` / ``planet``).  Each dataset may have multiple *snapshots* --
versioned deliveries of that data that are processed through three
pipelines:

1. **Collection** -- fetch data from the provider and stage it.
2. **Ingestion** -- transform/normalise the collected data.
3. **Matching** -- apply entity-matching rules against other datasets.

Dataset definitions are stored as JSON files named ``<provider_label>.json``
under a datasets directory that the caller supplies (this module makes no
assumption about where that directory lives).  Each JSON file contains a
``provider`` section and a ``resources`` array.  Every resource entry
carries its own ``collection``, ``ingestion``, and ``matching`` sections.

Usage
-----
::

    from overture_core.dataset import Dataset

    ds = Dataset.from_name("osm", "planet", datasets_dir="configs/datasets")

    collection_cfg = ds.collection
    ingestion_cfg  = ds.ingestion
    matching_cfg   = ds.matching

    print(ds.dataset_id)       # "osm_planet"
    print(ds.provider_name)    # "OpenStreetMap"
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class Dataset:
    """Dataset definition loaded from a provider JSON file for a specific resource.

    Parameters
    ----------
    provider_data : dict[str, Any]
        The ``provider`` section of the JSON file.
    resource_data : dict[str, Any]
        The matched entry from the ``resources`` array.
    dataset_id : str
        Identifier derived from ``<provider_label>_<resource>``.
    """

    def __init__(
        self,
        provider_data: dict[str, Any],
        resource_data: dict[str, Any],
        dataset_id: str,
    ) -> None:
        self._provider = provider_data
        self._resource_data = resource_data
        self._dataset_id = dataset_id

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_name(
        cls, provider_label: str, resource: str, datasets_dir: str | Path
    ) -> Dataset:
        """Load a dataset by provider label and resource label.

        Looks for ``<provider_label>.json`` inside *datasets_dir* and finds
        the resource entry whose ``label`` matches *resource*.

        Parameters
        ----------
        provider_label : str
            Provider file stem, e.g. ``"osm"``.
        resource : str
            Resource label within the provider, e.g. ``"planet"``.
        datasets_dir : str | Path
            Directory containing the provider JSON files. Callers own the
            actual location (e.g. a repo's ``configs/datasets`` directory);
            this module makes no assumption about it.

        Returns
        -------
        Dataset

        Raises
        ------
        FileNotFoundError
            If the JSON file does not exist.
        KeyError
            If the resource is not found in the provider's resources.
        """
        path = cls.path_for(provider_label, datasets_dir)
        return cls.from_file(path, resource=resource)

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        resource: str | None = None,
    ) -> Dataset:
        """Load a dataset from an arbitrary JSON file path.

        Parameters
        ----------
        path : str | Path
            Absolute or relative path to the provider JSON file.
        resource : str | None
            Resource name to select.  When ``None`` and there is exactly
            one resource in the file, that resource is used automatically.

        Returns
        -------
        Dataset

        Raises
        ------
        FileNotFoundError
            If the JSON file does not exist.
        KeyError
            If the resource is not found.
        ValueError
            If *resource* is ``None`` and the file contains multiple
            resources.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}")

        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        provider_data = data["provider"]
        resources = data.get("resources", [])

        resource_data = cls._resolve_resource(resources, resource, path)
        dataset_id = f"{provider_data['label']}_{resource_data['label']}"
        log.info("Loaded dataset '%s' from %s", dataset_id, path)
        return cls(provider_data, resource_data, dataset_id=dataset_id)

    @staticmethod
    def _resolve_resource(
        resources: list[dict[str, Any]],
        resource: str | None,
        path: Path,
    ) -> dict[str, Any]:
        """Find and return the matching resource entry by label."""
        if resource is not None:
            for entry in resources:
                if entry["label"] == resource:
                    return entry
            available = [r["label"] for r in resources]
            raise KeyError(
                f"Resource '{resource}' not found in {path}. Available: {available}"
            )
        if len(resources) == 1:
            return resources[0]
        if not resources:
            raise ValueError(f"No resources defined in {path}")
        available = [r["label"] for r in resources]
        raise ValueError(f"Multiple resources in {path}; specify one of: {available}")

    @classmethod
    def path_for(cls, provider_label: str, datasets_dir: str | Path) -> Path:
        """Return the filesystem path to the JSON config file for *provider_label*.

        Parameters
        ----------
        provider_label : str
            Provider file stem, e.g. ``"open_addresses"`` or ``"nad"``.
        datasets_dir : str | Path
            Directory containing the provider JSON files.

        Returns
        -------
        Path
        """
        return Path(datasets_dir) / f"{provider_label}.json"

    @classmethod
    def all_from_file(cls, path: str | Path) -> list[Dataset]:
        """Load *all* resource datasets from a provider JSON file.

        Parameters
        ----------
        path : str | Path
            Path to the provider JSON file.

        Returns
        -------
        list[Dataset]
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}")

        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        provider_data = data["provider"]
        return [
            cls(
                provider_data,
                res,
                dataset_id=f"{provider_data['label']}_{res['label']}",
            )
            for res in data.get("resources", [])
        ]

    # ------------------------------------------------------------------
    # Identity properties
    # ------------------------------------------------------------------

    @property
    def provider_data(self) -> dict[str, Any]:
        """Full provider data dict (label, name, url)."""
        return dict(self._provider)

    @property
    def resource_data(self) -> dict[str, Any]:
        """Full resource data dict (label, name, collection, etc.)."""
        return dict(self._resource_data)

    @property
    def dataset_id(self) -> str:
        """Unique identifier: ``<provider_label>_<resource>``."""
        return self._dataset_id

    @property
    def provider_label(self) -> str:
        """Short provider label used in file naming (e.g. ``"osm"``)."""
        return self._provider["label"]

    @property
    def provider_name(self) -> str:
        """Human-readable provider name (e.g. ``"OpenStreetMap"``)."""
        return self._provider["name"]

    @property
    def provider_url(self) -> dict[str, str]:
        """Provider URLs (primary and archive)."""
        return self._provider["url"]

    @property
    def resource_label(self) -> str:
        """Resource label (snake_case) within the provider (e.g. ``"planet"``)."""
        return self._resource_data["label"]

    @property
    def resource_name(self) -> str:
        """Human-readable resource name (e.g. ``"Microsoft ML Buildings"``)."""
        return self._resource_data["name"]

    # ------------------------------------------------------------------
    # Pipeline section accessors
    # ------------------------------------------------------------------

    @property
    def collection(self) -> dict[str, Any]:
        """Collection pipeline configuration.

        Contains data location, download info, license, coverage,
        refresh schedule, and any notes relevant to fetching data
        from the provider.
        """
        return self._resource_data.get("collection", {})

    @property
    def ingestion(self) -> dict[str, Any]:
        """Ingestion pipeline configuration.

        Contains transformation and normalisation rules applied after
        collection.  May be empty for datasets that have not yet
        defined ingestion steps.
        """
        return self._resource_data.get("ingestion", {})

    @property
    def matching(self) -> dict[str, Any]:
        """Matching pipeline configuration.

        Contains entity-matching rules and parameters used when
        reconciling this dataset against other datasets.  May be
        empty for datasets without special matching logic.
        """
        return self._resource_data.get("matching", {})

    # ------------------------------------------------------------------
    # Convenience accessors for common collection fields
    # ------------------------------------------------------------------

    @property
    def license(self) -> dict[str, Any]:
        """License information from the collection section."""
        return self.collection.get("license", {})

    @property
    def license_type(self) -> str:
        """SPDX license identifier (e.g. ``"ODbL-1.0"``)."""
        return self.license.get("type", "")

    @property
    def coverage(self) -> dict[str, Any]:
        """Geographic coverage from the collection section."""
        return self.collection.get("coverage", {})

    @property
    def data_location(self) -> dict[str, str]:
        """Data location URLs from the collection section."""
        return self.collection.get("data_location", {})

    @property
    def refresh_schedule(self) -> dict[str, str]:
        """Refresh schedule from the collection section."""
        return self.collection.get("refresh_schedule", {})

    @property
    def download_url(self) -> str:
        """Full download URL (``url`` + ``endpoint``) from ``collection.data_download``."""
        dd = self.collection.get("data_download")
        if not dd:
            raise ValueError(f"No data_download entry for dataset '{self._dataset_id}'")
        return dd["url"] + dd.get("endpoint", "")

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"Dataset(id={self._dataset_id!r}, provider={self.provider_name!r}, resource={self.resource_label!r})"

    def __str__(self) -> str:
        return self._dataset_id
