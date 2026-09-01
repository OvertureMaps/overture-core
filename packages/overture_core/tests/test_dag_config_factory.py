"""Tests for the DagConfigFactory abstract base class."""

import pytest

from overture_core.dag_config_factory import DagConfigFactory


class TestDagConfigFactory:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            DagConfigFactory()

    def test_concrete_subclass_must_implement_build_config(self):
        class Incomplete(DagConfigFactory):
            pass

        with pytest.raises(TypeError):
            Incomplete()

    def test_concrete_subclass_can_be_instantiated_and_used(self):
        class Concrete(DagConfigFactory):
            def build_config(self, dag_conf):
                return {"conf": dag_conf}

        factory = Concrete()
        assert isinstance(factory, DagConfigFactory)
        assert factory.build_config({"a": 1}) == {"conf": {"a": 1}}
