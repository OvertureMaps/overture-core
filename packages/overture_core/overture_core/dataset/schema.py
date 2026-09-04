"""Pydantic schema for dataset config files.

Field-by-field documentation belongs alongside the actual config files a
caller validates. Unknown keys are rejected, so adding a new field to a
config file requires extending the models here in the same change.

This module makes no assumption about where dataset config files live --
callers pass explicit paths (or a directory to glob) rather than relying on
a baked-in default location. See ``overture_core.dataset.dataset`` for the
permissive runtime loader over that same JSON shape.

Run as a script to validate specific files (used by CI):

    python -m overture_core.dataset.schema path/to/provider.json ...
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

LABEL_PATTERN = r"^[a-z0-9_]+$"
ISO_3166_1_PATTERN = r"^([A-Z]{2}|GLOBAL)$"
ISO_3166_2_PATTERN = r"^[A-Z]{2}-[A-Z0-9]{1,3}$"

Label = Annotated[str, Field(pattern=LABEL_PATTERN)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
Month = Annotated[int, Field(ge=1, le=12)]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class UrlPair(_StrictModel):
    """A live URL plus its web.archive.org snapshot. Either may be empty."""

    primary: str
    archive: str


class Provider(_StrictModel):
    label: Label
    name: NonEmptyStr
    url: UrlPair


class License(_StrictModel):
    url: UrlPair
    type: str
    requires_attribution: bool
    text: str
    attribution: str

    @model_validator(mode="after")
    def _attribution_required(self) -> License:
        if self.requires_attribution and not self.attribution:
            raise ValueError(
                "attribution must be non-empty when requires_attribution is true"
            )
        return self


class CoverageArea(_StrictModel):
    iso_3166_1: Annotated[str, Field(pattern=ISO_3166_1_PATTERN)]
    iso_3166_2: Annotated[str, Field(pattern=ISO_3166_2_PATTERN)] | None = None


class Coverage(_StrictModel):
    areas: Annotated[list[CoverageArea], Field(min_length=1)]
    description: str


class RefreshSchedule(_StrictModel):
    frequency: Literal[
        "",
        "None",
        "Daily",
        "Weekly",
        "Monthly",
        "Semi-annually",
        "Annually",
        "Infrequent",
    ]
    source: str | None = None
    type: Literal["continuous", "manual"] | None = None
    month: list[Month] | None = None


class Api(_StrictModel):
    url: NonEmptyStr


class DataDownload(_StrictModel):
    """Where and how to fetch the raw resource.

    `type` names the transport, `url` is the base location, and `endpoint`
    is an optional sub-path appended to it (see `Dataset.download_url`).
    """

    type: Literal["http", "s3", "hf"]
    url: NonEmptyStr
    endpoint: str = ""

    @model_validator(mode="after")
    def _url_scheme_matches_type(self) -> DataDownload:
        prefixes = {
            "http": ("http://", "https://"),
            "s3": ("s3://",),
            "hf": ("hf://",),
        }[self.type]
        if not self.url.startswith(prefixes):
            raise ValueError(
                f"url scheme does not match type '{self.type}': {self.url}"
            )
        return self


class Collection(_StrictModel):
    data_location: UrlPair
    data_download: DataDownload | None = None
    license: License | None = None
    coverage: Coverage
    refresh_schedule: RefreshSchedule | None = None
    known_issues: str | None = None
    notes: str
    api: Api | None = None
    extras: dict[str, str] | None = None


class Resource(_StrictModel):
    label: Label
    name: NonEmptyStr
    collection: Collection
    # Free-form pipeline configs; intentionally unvalidated.
    ingestion: dict[str, Any]
    matching: dict[str, Any]


class DatasetFile(_StrictModel):
    provider: Provider
    resources: Annotated[list[Resource], Field(min_length=1)]

    @model_validator(mode="after")
    def _unique_resource_labels(self) -> DatasetFile:
        labels = [r.label for r in self.resources]
        dupes = sorted({label for label in labels if labels.count(label) > 1})
        if dupes:
            raise ValueError(f"duplicate resource labels: {dupes}")
        return self


def validate_file(path: str | Path) -> DatasetFile:
    """Validate one dataset config file; raises ValueError on any problem."""
    path = Path(path)
    if not re.fullmatch(LABEL_PATTERN, path.stem):
        raise ValueError(f"file name must be snake_case: {path.name}")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except OSError as e:
        raise ValueError(f"could not read {path}: {e}") from e
    parsed = DatasetFile.model_validate(data)
    if parsed.provider.label != path.stem:
        raise ValueError(
            f"provider label '{parsed.provider.label}' must match file stem '{path.stem}'"
        )
    return parsed


def validate_all(paths: list[Path]) -> list[tuple[Path, ValueError]]:
    """Validate the given files.

    Returns a list of (path, error) tuples; empty means everything passed.
    Also checks that provider labels are unique across the given files.
    """
    errors: list[tuple[Path, ValueError]] = []
    provider_labels: dict[str, Path] = {}
    for path in paths:
        try:
            parsed = validate_file(path)
        except ValueError as exc:  # includes json.JSONDecodeError, ValidationError
            errors.append((path, exc))
            continue
        label = parsed.provider.label
        if label in provider_labels:
            errors.append(
                (
                    path,
                    ValueError(
                        f"provider label '{label}' already used by {provider_labels[label].name}"
                    ),
                )
            )
        else:
            provider_labels[label] = path
    return errors


def main(argv: list[str] | None = None) -> int:
    """Validate the dataset config files passed as arguments.

    Unlike a repo-embedded validator, this module has no default directory to
    fall back to when no arguments are given — callers (e.g. a CI step) pass
    the files or a shell glob explicitly, e.g.:

        python -m overture_core.dataset.schema configs/datasets/*.json
    """
    args = sys.argv[1:] if argv is None else argv
    if not args:
        print(
            "usage: python -m overture_core.dataset.schema <path/to/provider.json> ...",
            file=sys.stderr,
        )
        return 2
    paths = [Path(a) for a in args]
    errors = validate_all(paths)
    for path, exc in errors:
        print(f"FAIL {path}:\n{exc}\n", file=sys.stderr)
    print(f"{len(paths) - len(errors)}/{len(paths)} dataset config files valid")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
