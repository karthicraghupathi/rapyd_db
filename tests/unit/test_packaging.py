import importlib

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "rapyd_db",
        "rapyd_db.backends",
        "rapyd_db.loggingadapter",
        "rapyd_db.utils",
    ],
)
def test_module_imports(module):
    importlib.import_module(module)


@pytest.mark.parametrize(
    ("module", "extra"),
    [
        ("rapyd_db.backends.mysql", "mysql"),
        ("rapyd_db.backends.mssql", "mssql"),
        ("rapyd_db.backends.mongo", "mongo"),
    ],
)
def test_optional_backend_modules_import_when_extras_installed(module, extra):
    try:
        importlib.import_module(module)
    except ImportError:
        pytest.skip(f"optional extra '{extra}' not installed")
