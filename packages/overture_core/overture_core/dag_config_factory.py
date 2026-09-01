"""Abstract base for theme-specific DAG configuration factories.

Consumers implement build_config() to translate a raw DAG run configuration
into whatever theme-specific config object their pipeline needs; this module
only declares the interface.
"""

from abc import ABC, abstractmethod


class DagConfigFactory(ABC):
    @abstractmethod
    def build_config(self, dag_conf):
        pass
