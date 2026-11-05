"""Compatibility alias for ``overture_dataset_configs.schema``."""

import sys

from overture_dataset_configs.schema import *  # noqa: F401,F403
from overture_dataset_configs.schema import main

if __name__ == "__main__":
    sys.exit(main())
