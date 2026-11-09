"""Run the packaged ``overture_dataset_configs`` data through the
``overture-datasets`` CLI, as release consumers do. License texts are skipped
to stay offline."""

from pathlib import Path

import overture_dataset_configs
import pytest
from overture_dataset_configs.attribution import entries_from_file
from overture_dataset_configs.schema import validate_file

from overture_core.dataset.cli import main

PACKAGE = Path(overture_dataset_configs.__file__).parent
DATASETS = PACKAGE / "datasets"
POLICY = PACKAGE / "license_policy.json"


def test_validate_with_policy():
    argv = ["--color", "never", "validate", str(DATASETS / "*.json")]
    assert main([*argv, "--policy", str(POLICY)]) == 0


@pytest.mark.parametrize("fmt", ["md", "txt"])
def test_license_renders_required_attributions(tmp_path, fmt):
    output = tmp_path / f"ATTRIBUTION.{fmt}"
    argv = ["--color", "never", "license", "-d", str(DATASETS), "--no-license-texts"]
    assert main([*argv, "-o", str(output)]) == 0

    rendered = output.read_text(encoding="utf-8")
    required = {
        e["attribution"]
        for path in DATASETS.glob("*.json")
        for e in entries_from_file(validate_file(path))
        if e["requires_attribution"]
    }
    assert required
    assert not [a for a in required if a not in rendered]
