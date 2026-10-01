"""Pydantic schema for the per-theme license allowlist file.

The file maps each released theme to the license identifiers (SPDX or
``LicenseRef-*``) its inputs are allowed to carry::

    {"base": ["ODbL-1.0", "CC-BY-4.0"], "places": ["CDLA-Permissive-2.0"]}

As with ``overture_core.dataset.schema``, this module has no baked-in file
location; callers pass the path explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, Iterable, Mapping

from pydantic import Field, RootModel, model_validator

from overture_core.dataset.schema import LABEL_PATTERN

# SPDX short identifiers and LicenseRef-* strings: letters, digits, '.', '-', '+'.
LICENSE_ID_PATTERN = r"^[A-Za-z0-9.+-]+$"

LicenseId = Annotated[str, Field(pattern=LICENSE_ID_PATTERN)]
ThemeLabel = Annotated[str, Field(pattern=LABEL_PATTERN)]


class LicensePolicy(
    RootModel[dict[ThemeLabel, Annotated[list[LicenseId], Field(min_length=1)]]]
):
    """Theme -> allowed license identifiers."""

    @model_validator(mode="after")
    def _unique_per_theme(self) -> LicensePolicy:
        for theme, licenses in self.root.items():
            dupes = sorted({x for x in licenses if licenses.count(x) > 1})
            if dupes:
                raise ValueError(f"theme '{theme}' lists duplicate licenses: {dupes}")
        return self

    @property
    def themes(self) -> list[str]:
        return sorted(self.root)

    @property
    def all_licenses(self) -> set[str]:
        return {lic for licenses in self.root.values() for lic in licenses}

    def allowed(self, theme: str) -> set[str]:
        """Licenses allowed for *theme*; raises ``KeyError`` for an unknown theme."""
        if theme not in self.root:
            raise KeyError(f"unknown theme '{theme}'; known themes: {self.themes}")
        return set(self.root[theme])

    def is_allowed(self, theme: str, license_type: str | None) -> bool:
        return bool(license_type) and license_type in self.allowed(theme)

    def violations(
        self, theme: str, entries: Iterable[Mapping[str, Any]]
    ) -> list[tuple[str, str]]:
        """``(dataset_id, license_type)`` for every license entry not allowed under *theme*.

        *entries* are license-entry dicts (see ``overture_core.dataset.attribution``).
        """
        allowed = self.allowed(theme)
        return [
            (_dataset_id(e), e.get("license_type") or "")
            for e in entries
            if (e.get("license_type") or "") not in allowed
        ]

    def unlisted_licenses(
        self, entries: Iterable[Mapping[str, Any]]
    ) -> dict[str, list[str]]:
        """License ids used by *entries* that appear under no theme, with the dataset_ids using them."""
        known = self.all_licenses
        out: dict[str, list[str]] = {}
        for e in entries:
            lic = e.get("license_type") or ""
            if lic and lic not in known:
                out.setdefault(lic, []).append(_dataset_id(e))
        return out


def _dataset_id(entry: Mapping[str, Any]) -> str:
    return f"{entry.get('provider_label', '')}_{entry.get('resource_label', '')}"


def load_policy(path: str | Path) -> LicensePolicy:
    """Load and validate a license policy file; raises ``ValueError`` on any problem."""
    path = Path(path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data: Any = json.load(fh)
    except OSError as e:
        raise ValueError(f"could not read {path}: {e}") from e
    if not isinstance(data, dict):
        raise ValueError(
            f"{path.name}: top level must be a JSON object of theme -> licenses"
        )
    return LicensePolicy.model_validate(data)
