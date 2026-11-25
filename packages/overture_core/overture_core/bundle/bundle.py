"""Data bundle model for pipeline directories.

A pipeline-managed *bundle* is an S3 directory that contains:

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

For platform source data, `SourceRawBundle` uses the
``datasets/provider=.../resource=.../version=.../run=...`` layout. The guiding
platform flow places raw data in a shared source-data location, whether a
provider drops it there or a collection DAG fetches it. A provider drop may
have only raw data and a ``success`` marker, without ``metadata.json``.
Ingest writes its Parquet output as a `SourceIngestBundle` under ``feeds/``;
matching and loading into the corpus follow. When the ingest bundle is
initialized, a caller-supplied ``provenance_resolver`` can enrich its input
provenance; it does not add metadata to the raw input. The orchestrator supplies
the bucket, collection and ingest steps, and metadata writer.

Each bundle type defines an ordered ``partitions`` dict whose keys
become Hive-style ``key=value`` path segments appended to the root
path.

Layout
------
Each bundle type declares ``BUNDLE_TYPE`` (the stage name in
``metadata.json``), ``ROOT_PATH`` (except `ModelBundle`, whose callers pass
the root), ``PARTITION_KEYS`` and ``COMPONENTS`` (the standard directories
and files under its base path; DAGs may add others, e.g. ``violations/``).
`Bundle.component_uri` gives a component's path.
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
  published one has a ``released`` marker.
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
- `Bundle.initialize` takes an optional ``provenance_resolver`` that maps an
  input bundle's provider/resource labels to its `Provenance`, and an optional
  ``provenance`` recorded as the bundle's own.  The caller owns the catalogue
  (e.g. ``overture-dataset-configs``); this package never reads one.
- ``store`` is the `BundleStore` used for S3 access; ``None`` opens a
  fresh `S3BundleStore` per operation.  Classmethods over many bundles use
  an explicit ``store`` for all of them; otherwise each bundle uses its own,
  and bundles without one share a single default store.

Modules
-------
This module re-exports the model from:

- `overture_core.bundle.base` -- `Bundle` and batch execution.
- `overture_core.bundle.bundle_types` -- the concrete bundle types.
- `overture_core.bundle.components` -- `BundleComponent`, `ComponentKind` and
  the standard components.
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
    Provenance,
    ProvenanceResolver,
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
    "Provenance",
    "ProvenanceResolver",
    "ReleaseCandidateBundle",
    "S3BundleStore",
    "ScratchBundle",
    "SourceIngestBundle",
    "SourceRawBundle",
    "TableDropper",
    "ThemeAssembleBundle",
    "ThemePromoteBundle",
]
