import os

import pytest


def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_INTEGRATION_TESTS") == "1":
        return
    skip_integration = pytest.mark.skip(
        reason="integration tests off; set RUN_INTEGRATION_TESTS=1"
    )
    for item in items:
        item.add_marker(skip_integration)


# Integration test files import optional backend drivers (mysqlclient, pymssql,
# pymongo) at module load time. Pytest imports test modules during collection,
# which happens BEFORE pytest_collection_modifyitems runs — so missing drivers
# would crash collection regardless of the gate above. To make the gate
# actually effective when drivers aren't installed, ignore the test files
# entirely at collection time when integration is disabled.
if os.environ.get("RUN_INTEGRATION_TESTS") != "1":
    collect_ignore_glob = ["test_*.py"]
