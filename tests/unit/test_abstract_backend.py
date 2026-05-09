import pytest

from rapyd_db.backends import AbstractBackend


def test_cannot_instantiate_abstract_backend_directly():
    with pytest.raises(TypeError):
        AbstractBackend()


def test_subclass_must_implement_connect():
    class Half(AbstractBackend):
        pass

    with pytest.raises(TypeError):
        Half()


def test_concrete_subclass_instantiates():
    class Concrete(AbstractBackend):
        def _connect(self):
            return object()

    assert Concrete()._connect() is not None


def test_default_execute_returns_none():
    class Concrete(AbstractBackend):
        def _connect(self):
            return None

    # current AbstractBackend.execute is a placeholder that returns None
    assert Concrete().execute() is None
