import logging
import pytest


@pytest.fixture(autouse=True)
def _silence_rapyd_db_logger(caplog):
    caplog.set_level(logging.DEBUG, logger="rapyd_db")
    yield
