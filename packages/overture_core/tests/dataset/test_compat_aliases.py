import importlib
import subprocess
import sys

import pytest


def test_schema_module_entry_point_propagates_exit_status(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "overture_core.dataset.schema", "missing.json"],
        cwd=tmp_path,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0


@pytest.mark.parametrize(
    ("module", "name"),
    [
        ("schema", "DatasetFile"),
        ("dataset", "Dataset"),
        ("license_policy", "LicensePolicy"),
        ("attribution", "license_entry"),
        ("spdx", "license_texts"),
    ],
)
def test_legacy_import_path_aliases_dataset_configs(module, name):
    legacy = importlib.import_module(f"overture_core.dataset.{module}")
    current = importlib.import_module(f"overture_dataset_configs.{module}")
    assert getattr(legacy, name) is getattr(current, name)
