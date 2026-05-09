import sys
import types
from unittest.mock import MagicMock, patch

# Stub the MySQLdb module before rapyd_db.backends.mysql imports it.
# The dev venv does not have mysqlclient installed; this lets the source
# module import cleanly without the real C extension.
_mysqldb_stub = types.ModuleType("MySQLdb")
_mysqldb_stub.connect = MagicMock()
_mysqldb_cursors = types.ModuleType("MySQLdb.cursors")
_mysqldb_cursors.DictCursor = MagicMock()
_mysqldb_cursors.SSDictCursor = MagicMock()
_mysqldb_stub.cursors = _mysqldb_cursors
sys.modules.setdefault("MySQLdb", _mysqldb_stub)
sys.modules.setdefault("MySQLdb.cursors", _mysqldb_cursors)

import pytest  # noqa: E402

from rapyd_db.backends.mysql import MySQL  # noqa: E402


@pytest.fixture
def mock_mysqldb():
    with patch("rapyd_db.backends.mysql.MySQLdb") as m:
        yield m


def _set_cursor(
    mock_mysqldb,
    *,
    rowcount=1,
    lastrowid=42,
    rows=None,
    stream_rows=None,
):
    cursor = MagicMock()
    cursor.execute.return_value = rowcount
    cursor.lastrowid = lastrowid
    cursor.fetchall.return_value = rows if rows is not None else [{"x": 1}]
    iter_rows = stream_rows if stream_rows is not None else (rows or [{"x": 1}])
    cursor.__iter__ = lambda self: iter(iter_rows)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_mysqldb.connect.return_value = conn
    return conn, cursor


class TestMySQLInit:
    def test_strips_cursorclass(self, mock_mysqldb):
        db = MySQL(host="h", user="u", password="p", cursorclass="ignored", port=3306)
        assert "cursorclass" not in db._connection_params
        assert db._connection_params["host"] == "h"
        assert db._connection_params["port"] == 3306
        assert db._connection_params["user"] == "u"
        assert db._connection_params["password"] == "p"

    def test_omits_none_values(self, mock_mysqldb):
        db = MySQL(host=None, user="u", password=None)
        assert "host" not in db._connection_params
        assert "password" not in db._connection_params
        assert db._connection_params["user"] == "u"

    def test_no_cursorclass_kwarg_pop_is_safe(self, mock_mysqldb):
        # default kwargs include no cursorclass; pop with default None must not raise
        db = MySQL(host="h", user="u", password="p")
        assert "cursorclass" not in db._connection_params

    def test_extra_kwargs_propagated(self, mock_mysqldb):
        db = MySQL(host="h", user="u", password="p", db="mydb", charset="utf8")
        assert db._connection_params["db"] == "mydb"
        assert db._connection_params["charset"] == "utf8"


class TestMySQLConnect:
    def test_connect_uses_connection_params(self, mock_mysqldb):
        db = MySQL(host="h", user="u", password="p", port=3306)
        result = db._connect()
        mock_mysqldb.connect.assert_called_once_with(host="h", user="u", password="p", port=3306)
        assert result is mock_mysqldb.connect.return_value


class TestMySQLNoStream:
    def test_returns_triple(self, mock_mysqldb):
        _, cursor = _set_cursor(mock_mysqldb, rowcount=3, lastrowid=99, rows=[{"a": 1}])
        db = MySQL(host="h", user="u", password="p")
        rows_affected, last_id, results = db.execute("SELECT * FROM t")
        assert rows_affected == 3
        assert last_id == 99
        assert results == [{"a": 1}]
        cursor.execute.assert_called_once_with("SELECT * FROM t")

    def test_passes_params(self, mock_mysqldb):
        _, cursor = _set_cursor(mock_mysqldb)
        db = MySQL(host="h", user="u", password="p")
        db.execute("INSERT INTO t VALUES (%s)", ("a",))
        cursor.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", ("a",))

    def test_logs_rendered_query(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb)
        db = MySQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("SELECT now()")
        assert any("Query: SELECT now()" in r.getMessage() for r in caplog.records)

    def test_logs_params_when_provided(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb)
        db = MySQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("INSERT INTO t VALUES (%s)", ("a",))
        assert any("Params:" in r.getMessage() for r in caplog.records)

    def test_sets_dictcursor_class(self, mock_mysqldb):
        _set_cursor(mock_mysqldb)
        sentinel = object()
        with patch("rapyd_db.backends.mysql.DictCursor", sentinel):
            db = MySQL(host="h", user="u", password="p")
            db.execute("SELECT 1")
            assert db._connection_params["cursorclass"] is sentinel

    def test_autocommit_true_called(self, mock_mysqldb):
        conn, _ = _set_cursor(mock_mysqldb)
        db = MySQL(host="h", user="u", password="p")
        db.execute("SELECT 1")
        conn.autocommit.assert_called_once_with(True)

    def test_logs_rows_affected_message(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, rowcount=7)
        db = MySQL(host="h", user="u", password="p")
        db.execute("UPDATE t SET x=1")
        assert any("7 row(s) affected" in r.message for r in caplog.records)


class TestMySQLStream:
    def test_yields_rows(self, mock_mysqldb):
        rows = [{"i": 1}, {"i": 2}, {"i": 3}]
        _, cursor = _set_cursor(mock_mysqldb, stream_rows=rows)
        db = MySQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t", stream=True))
        assert out == rows
        cursor.execute.assert_called_once_with("SELECT * FROM t")

    def test_passes_params(self, mock_mysqldb):
        rows = [{"i": 1}]
        _, cursor = _set_cursor(mock_mysqldb, stream_rows=rows)
        db = MySQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t WHERE x=%s", ("a",), stream=True))
        assert out == rows
        cursor.execute.assert_called_once_with("SELECT * FROM t WHERE x=%s", ("a",))

    def test_sets_ssdictcursor_class(self, mock_mysqldb):
        _set_cursor(mock_mysqldb, stream_rows=[])
        sentinel = object()
        with patch("rapyd_db.backends.mysql.SSDictCursor", sentinel):
            db = MySQL(host="h", user="u", password="p")
            list(db.execute("SELECT 1", stream=True))
            assert db._connection_params["cursorclass"] is sentinel

    def test_autocommit_true_called(self, mock_mysqldb):
        conn, _ = _set_cursor(mock_mysqldb, stream_rows=[])
        db = MySQL(host="h", user="u", password="p")
        list(db.execute("SELECT 1", stream=True))
        conn.autocommit.assert_called_once_with(True)

    def test_logs_rendered_query(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, stream_rows=[])
        db = MySQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("SELECT now()", stream=True))
        assert any("Query: SELECT now()" in r.getMessage() for r in caplog.records)

    def test_logs_params_when_provided(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, stream_rows=[])
        db = MySQL(host="h", user="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("SELECT * FROM t WHERE x=%s", ("a",), stream=True))
        assert any("Params:" in r.getMessage() for r in caplog.records)

    def test_logs_streaming_message(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, stream_rows=[])
        db = MySQL(host="h", user="u", password="p")
        list(db.execute("SELECT 1", stream=True))
        assert any("Streaming results from DB." in r.message for r in caplog.records)

    def test_logs_execution_end(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, stream_rows=[{"i": 1}])
        db = MySQL(host="h", user="u", password="p")
        list(db.execute("SELECT 1", stream=True))
        assert any("Ended query execution at" in r.message for r in caplog.records)
        assert any("Executed in" in r.message for r in caplog.records)
