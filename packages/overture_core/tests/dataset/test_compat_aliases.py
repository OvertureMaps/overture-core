import importlib

import pytest


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
