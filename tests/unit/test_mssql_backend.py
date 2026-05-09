import sys
import types
from unittest.mock import MagicMock, patch

# Stub the pymssql module before rapyd_db.backends.mssql imports it.
# The dev venv does not have pymssql installed; this lets the source
# module import cleanly without the real C extension.
_pymssql_stub = types.ModuleType("pymssql")
_pymssql_stub.connect = MagicMock()
_pymssql_stub.set_max_connections = MagicMock()


class _OpError(Exception):
    def __init__(self, message):
        super().__init__(message)


_pymssql_stub.OperationalError = _OpError
sys.modules.setdefault("pymssql", _pymssql_stub)

import pytest  # noqa: E402

from rapyd_db.backends.mssql import MSSQL  # noqa: E402


class FakeOpError(Exception):
    def __init__(self, message):
        super().__init__(message)


@pytest.fixture
def mock_pymssql():
    with patch("rapyd_db.backends.mssql.pymssql") as m:
        m.OperationalError = FakeOpError
        yield m


def _set_cursor(
    mock_pymssql,
    *,
    rowcount=1,
    lastrowid=42,
    rows=None,
    fetch_exc=None,
    iter_rows=None,
):
    cursor = MagicMock()
    cursor.rowcount = rowcount
    cursor.lastrowid = lastrowid
    if fetch_exc is not None:
        cursor.fetchall.side_effect = fetch_exc
    else:
        cursor.fetchall.return_value = rows if rows is not None else [{"x": 1}]
    iter_data = iter_rows if iter_rows is not None else (rows or [{"x": 1}])
    cursor.__iter__ = lambda self: iter(iter_data)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_pymssql.connect.return_value = conn
    return conn, cursor


class TestMSSQLInit:
    def test_forces_as_dict_true_when_passed_false(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p", as_dict=False)
        assert db._connection_params["as_dict"] is True

    def test_forces_as_dict_true_when_not_passed(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p")
        assert db._connection_params["as_dict"] is True

    def test_assigns_connection_params(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p", database="db")
        assert db._connection_params["host"] == "h"
        assert db._connection_params["user"] == "u"
        assert db._connection_params["password"] == "p"
        assert db._connection_params["database"] == "db"

    def test_omits_none_values(self, mock_pymssql):
        db = MSSQL(host=None, user="u", password=None)
        assert "host" not in db._connection_params
        assert "password" not in db._connection_params
        assert db._connection_params["user"] == "u"

    def test_extra_kwargs_propagated(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p", port=1433, charset="utf8")
        assert db._connection_params["port"] == 1433
        assert db._connection_params["charset"] == "utf8"


class TestMSSQLConnect:
    def test_does_not_set_max_connections(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p")
        db._connect()
        mock_pymssql.set_max_connections.assert_not_called()

    def test_connect_uses_connection_params(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p", port=1433)
        result = db._connect()
        mock_pymssql.connect.assert_called_once_with(
            host="h", user="u", password="p", port=1433, as_dict=True
        )
        assert result is mock_pymssql.connect.return_value


class TestMSSQLNoStream:
    def test_returns_triple_with_results(self, mock_pymssql):
        _, cursor = _set_cursor(mock_pymssql, rowcount=2, lastrowid=99, rows=[{"a": 1}, {"a": 2}])
        db = MSSQL(host="h", user="u", password="p")
        affected, last_id, rows = db.execute("SELECT * FROM t")
        assert affected == 2
        assert last_id == 99
        assert rows == [{"a": 1}, {"a": 2}]
        cursor.execute.assert_called_once_with("SELECT * FROM t")

    def test_passes_params_when_provided(self, mock_pymssql):
        _, cursor = _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        db.execute("INSERT INTO t VALUES (%s)", ("a",))
        cursor.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", ("a",))

    def test_swallows_no_resultset_operationalerror(self, mock_pymssql):
        msg = "Statement not executed or executed statement has no resultset"
        _set_cursor(mock_pymssql, fetch_exc=mock_pymssql.OperationalError(msg))
        db = MSSQL(host="h", user="u", password="p")
        affected, _, rows = db.execute("CREATE TABLE t (a int)")
        assert rows == []

    def test_reraises_other_operationalerror(self, mock_pymssql):
        _set_cursor(
            mock_pymssql,
            fetch_exc=mock_pymssql.OperationalError("something else"),
        )
        db = MSSQL(host="h", user="u", password="p")
        with pytest.raises(mock_pymssql.OperationalError):
            db.execute("SELECT * FROM t")

    def test_autocommit_true_called(self, mock_pymssql):
        conn, _ = _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        db.execute("SELECT 1")
        conn.autocommit.assert_called_once_with(True)

    def test_logs_query(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("SELECT now()")
        assert any("SELECT now()" in r.message for r in caplog.records)

    def test_logs_params(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("SELECT %s", ("a",))
        assert any("Params:" in r.message for r in caplog.records)

    def test_logs_not_streaming_message(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("SELECT 1")
        assert any("Not streaming results from DB." in r.message for r in caplog.records)

    def test_logs_rows_affected_message(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql, rowcount=7)
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("UPDATE t SET x=1")
        assert any("7 row(s) affected" in r.message for r in caplog.records)

    def test_logs_execution_end(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql)
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("SELECT 1")
        assert any("Ended query execution at" in r.message for r in caplog.records)
        assert any("Starting executing query at" in r.message for r in caplog.records)


class TestMSSQLStream:
    def test_yields_rows(self, mock_pymssql):
        rows = [{"i": 0}, {"i": 1}, {"i": 2}]
        _, cursor = _set_cursor(mock_pymssql, iter_rows=rows)
        db = MSSQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t", stream=True))
        assert out == rows
        cursor.execute.assert_called_once_with("SELECT * FROM t")

    def test_passes_params(self, mock_pymssql):
        rows = [{"i": 1}]
        _, cursor = _set_cursor(mock_pymssql, iter_rows=rows)
        db = MSSQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t WHERE x=%s", ("a",), stream=True))
        assert out == rows
        cursor.execute.assert_called_once_with("SELECT * FROM t WHERE x=%s", ("a",))

    def test_autocommit_true_called(self, mock_pymssql):
        conn, _ = _set_cursor(mock_pymssql, iter_rows=[])
        db = MSSQL(host="h", user="u", password="p")
        list(db.execute("SELECT 1", stream=True))
        conn.autocommit.assert_called_once_with(True)

    def test_logs_query(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql, iter_rows=[])
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("SELECT now()", stream=True))
        assert any("SELECT now()" in r.message for r in caplog.records)

    def test_logs_streaming_message(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql, iter_rows=[])
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("SELECT 1", stream=True))
        assert any("Streaming results from DB." in r.message for r in caplog.records)

    def test_logs_execution_end(self, mock_pymssql, caplog):
        _set_cursor(mock_pymssql, iter_rows=[{"i": 1}])
        db = MSSQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("SELECT 1", stream=True))
        assert any("Ended query execution at" in r.message for r in caplog.records)
        assert any("Executed in" in r.message for r in caplog.records)
