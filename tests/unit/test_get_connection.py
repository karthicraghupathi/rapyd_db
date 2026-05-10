from unittest.mock import MagicMock

import pytest

from rapyd_db.backends import AbstractBackend, get_connection


class _Backend(AbstractBackend):
    def __init__(self, conn):
        self._conn = conn

    def _connect(self):
        return self._conn


def test_yields_connection_and_closes():
    conn = MagicMock()
    with get_connection(_Backend(conn)) as c:
        assert c is conn
    conn.close.assert_called_once()


def test_log_id_is_logged(caplog):
    conn = MagicMock()
    with get_connection(_Backend(conn), log_id="trace-1"):
        pass
    assert any("trace-1" in r.message for r in caplog.records)


def test_connect_failure_is_logged_and_raised(caplog):
    class Boom(AbstractBackend):
        def _connect(self):
            raise RuntimeError("nope")

    with pytest.raises(RuntimeError, match="nope"), get_connection(Boom()):
        pass
    assert any("Cannot connect to DB" in r.message for r in caplog.records)


def test_close_swallows_exception():
    conn = MagicMock()
    conn.close.side_effect = RuntimeError("close-failed")
    # the bare except in the finally must not propagate
    with get_connection(_Backend(conn)):
        pass
    conn.close.assert_called_once()
