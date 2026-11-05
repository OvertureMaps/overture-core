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
3. **Finalize** -- stamp ``metadata.json`` with the run's status and write
   a new ``success`` file for ``"success"``, or delete it for any other.

`Bundle.status` reports where a stored bundle is in this lifecycle.

Each bundle type defines an ordered ``partitions`` dict whose keys
become Hive-style ``key=value`` path segments appended to the root
path.

Layout
------
Each bundle type declares ``BUNDLE_TYPE`` (the stage name in
``metadata.json``), ``ROOT_PATH`` (except `ModelBundle`, whose callers pass
the root), ``PARTITION_KEYS`` and ``COMPONENTS`` (the standard directories
and files under its base path; DAGs may add others, e.g. ``violations/``).
Each directory component has ``<name>_uri`` / ``<name>_prefix`` properties.
`overture_core.bundle.registry` maps an S3 path to its type.

Class hierarchy
---------------
- `Bundle` -- base class with ``data`` directory, metadata, and success file.
- `SourceRawBundle` -- raw collected resource from a provider (a.k.a dataset).
- `SourceIngestBundle` -- ingested/normalised dataset, with changelog and
  metrics.
- `ThemeAssembleBundle` -- assembled theme data from multiple datasets.
- `ThemePromoteBundle` -- promoted theme data with publishing artifacts.
- `ReleaseCandidateBundle` -- release candidate with publishing artifacts; a
  published one reports `BundleStatus.RELEASED`.
- `ScratchBundle` -- ephemeral debug/intermediate output, labeled by purpose.
- `ModelBundle` -- a trained model per training run, with a pointer naming
  the model's default run.

Deployment-specific inputs
--------------------------
These are passed in explicitly; orchestrators can subclass the bundle
classes to supply defaults.

- ``bucket`` and ``namespace`` are constructor arguments.  A non-empty
  ``namespace`` is prepended to the root path (``<namespace>/<root>``).
- `Bundle.initialize` / `Bundle.finalize` take a `BundleMetadata` writer
  (the batch variants take a ``metadata_factory``).
- `Bundle.delete` / `Bundle.cleanup_all` take an optional `TableDropper`
  to drop any catalogue tables registered for a bundle.
- `Bundle.initialize` takes the ``datasets_dir`` used to enrich input
  provenance from the ``overture_core`` Dataset catalogue, and an optional
  ``source_dataset`` recorded as the bundle's own provenance.
- ``store`` is the `BundleStore` used for S3 access; ``None`` opens a
  fresh `S3BundleStore` per operation.  Classmethods over many bundles use
  an explicit ``store`` for all of them; otherwise each bundle uses its own,
  and bundles without one share a single default store.

Modules
-------
This module re-exports the model from:

- `overture_core.bundle.base` -- `Bundle` and batch execution.
- `overture_core.bundle.bundle_types` -- the concrete bundle types.
- `overture_core.bundle.components` -- `BundleComponent` and `ComponentKind`.
- `overture_core.bundle.partitions` -- partition paths, resolution and walking.
- `overture_core.bundle.metadata` -- `BundleMetadata` and input provenance.
- `overture_core.bundle.cleanup` -- cleanup reports and `TableDropper`.
- `overture_core.bundle.status` -- `BundleStatus` and `BundleStatusReport`.
- `overture_core.bundle.store` -- `BundleStore` and its implementations.
"""

from overture_core.bundle.base import Bundle
from overture_core.bundle.bundle_types import (
    ModelBundle,
    ReleaseCandidateBundle,
    ScratchBundle,
    SourceIngestBundle,
    SourceRawBundle,
    ThemeAssembleBundle,
    ThemePromoteBundle,
)
from overture_core.bundle.cleanup import (
    BundleDecision,
    CleanupReport,
    DeleteResult,
    TableDropper,
)
from overture_core.bundle.components import BundleComponent, ComponentKind
from overture_core.bundle.metadata import (
    BundleMetadata,
    provenance_sections,
)
from overture_core.bundle.metadata import (
    _enrich_inputs_from_dataset as _enrich_inputs_from_dataset,
)
from overture_core.bundle.partitions import PartitionResolutionError
from overture_core.bundle.partitions import _natural_key as _natural_key
from overture_core.bundle.partitions import _resolve_partitions as _resolve_partitions
from overture_core.bundle.status import BundleStatus, BundleStatusReport
from overture_core.bundle.store import BundleStore, S3BundleStore

__all__ = [
    "Bundle",
    "BundleComponent",
    "BundleDecision",
    "BundleMetadata",
    "BundleStatus",
    "BundleStatusReport",
    "BundleStore",
    "CleanupReport",
    "ComponentKind",
    "DeleteResult",
    "ModelBundle",
    "PartitionResolutionError",
    "ReleaseCandidateBundle",
    "S3BundleStore",
    "ScratchBundle",
    "SourceIngestBundle",
    "SourceRawBundle",
    "TableDropper",
    "ThemeAssembleBundle",
    "ThemePromoteBundle",
    "provenance_sections",
]
