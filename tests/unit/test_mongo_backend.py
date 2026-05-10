import sys
import types
from unittest.mock import MagicMock, patch

# Stub the pymongo module before rapyd_db.backends.mongo imports it.
# The dev venv may not have pymongo installed; this lets the source
# module import cleanly without the real driver.
_pymongo_stub = types.ModuleType("pymongo")
_pymongo_stub.MongoClient = MagicMock()


class _StubCursor:
    """Stand-in for pymongo.cursor.Cursor when pymongo is not installed."""


class _StubCommandCursor:
    """Stand-in for pymongo.command_cursor.CommandCursor when pymongo is not installed."""


_pymongo_cursor_stub = types.ModuleType("pymongo.cursor")
_pymongo_cursor_stub.Cursor = _StubCursor
_pymongo_command_cursor_stub = types.ModuleType("pymongo.command_cursor")
_pymongo_command_cursor_stub.CommandCursor = _StubCommandCursor

sys.modules.setdefault("pymongo", _pymongo_stub)
sys.modules.setdefault("pymongo.cursor", _pymongo_cursor_stub)
sys.modules.setdefault("pymongo.command_cursor", _pymongo_command_cursor_stub)

import pytest  # noqa: E402
from pymongo.cursor import Cursor  # noqa: E402

from rapyd_db.backends.mongo import Mongo  # noqa: E402


class _CursorLike(Cursor):
    """Cursor subclass that bypasses Cursor.__init__ for testing."""

    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)


def _cursor_mock(items):
    """Return an object that passes isinstance(_, Cursor) and iterates as items."""
    return _CursorLike(items)


@pytest.fixture
def mock_client():
    with patch("rapyd_db.backends.mongo.MongoClient") as m:
        yield m


def _wire_client(mock_client):
    """Builds a chain client[database] -> db, db[collection] -> coll."""
    coll = MagicMock()
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db
    mock_client.return_value = client
    return client, db, coll


class TestMongoInit:
    def test_default_params(self, mock_client):
        db = Mongo(host="h", username="u", password="p")
        assert db._connection_params["host"] == "h"
        assert db._connection_params["username"] == "u"
        assert db._connection_params["password"] == "p"
        assert db._connection_params["authSource"] == "admin"
        assert db._connection_params["connectTimeoutMS"] == 2000
        assert db._connection_params["connect"] is False
        assert db._connection_params["maxPoolSize"] == 1

    def test_overrides_auth_source_and_timeout(self, mock_client):
        db = Mongo(
            host="h",
            username="u",
            password="p",
            auth_source="other",
            connect_timeout_ms=5000,
        )
        assert db._connection_params["authSource"] == "other"
        assert db._connection_params["connectTimeoutMS"] == 5000

    def test_omits_none_host(self, mock_client):
        db = Mongo(host=None, username="u", password="p")
        assert "host" not in db._connection_params

    def test_omits_none_username(self, mock_client):
        db = Mongo(host="h", username=None, password="p")
        assert "username" not in db._connection_params

    def test_omits_none_password(self, mock_client):
        db = Mongo(host="h", username="u", password=None)
        assert "password" not in db._connection_params

    def test_omits_none_auth_source(self, mock_client):
        db = Mongo(host="h", username="u", password="p", auth_source=None)
        assert "authSource" not in db._connection_params

    def test_omits_none_connect_timeout(self, mock_client):
        db = Mongo(host="h", username="u", password="p", connect_timeout_ms=None)
        assert "connectTimeoutMS" not in db._connection_params

    def test_extra_kwargs_propagated(self, mock_client):
        db = Mongo(host="h", username="u", password="p", replicaSet="rs0")
        assert db._connection_params["replicaSet"] == "rs0"

    def test_forces_pool_and_connect_flags_even_if_kwargs_set_them(self, mock_client):
        # The source applies maxPoolSize=1 and connect=False AFTER kwargs.update;
        # any caller-provided values must be overridden.
        db = Mongo(
            host="h",
            username="u",
            password="p",
            maxPoolSize=50,
            connect=True,
        )
        assert db._connection_params["maxPoolSize"] == 1
        assert db._connection_params["connect"] is False


class TestMongoConnect:
    def test_connect_uses_connection_params(self, mock_client):
        db = Mongo(host="h", username="u", password="p")
        result = db._connect()
        mock_client.assert_called_once_with(**db._connection_params)
        assert result is mock_client.return_value


class TestMongoNoStream:
    def test_dispatches_to_collection_method(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        sentinel = MagicMock(inserted_ids=[1, 2])
        coll.insert_many.return_value = sentinel
        db = Mongo(host="h", username="u", password="p")
        result = db.execute(
            "insert_many",
            [{"a": 1}, {"a": 2}],
            database="d",
            collection="c",
        )
        coll.insert_many.assert_called_once_with([{"a": 1}, {"a": 2}])
        # non-cursor results (InsertManyResult etc.) pass through unchanged
        assert result is sentinel

    def test_dispatches_to_database_method_when_only_collection_missing(self, mock_client):
        _client, db, _ = _wire_client(mock_client)
        db.command.return_value = {"ok": 1}
        backend = Mongo(host="h", username="u", password="p")
        result = backend.execute("command", "ping", database="admin")
        db.command.assert_called_once_with("ping")
        assert result == {"ok": 1}

    def test_server_info_bypasses_db_requirement(self, mock_client):
        client, _, _ = _wire_client(mock_client)
        client.server_info.return_value = {"version": "7.0"}
        db = Mongo(host="h", username="u", password="p")
        result = db.execute("server_info")
        client.server_info.assert_called_once_with()
        assert result == {"version": "7.0"}

    def test_kwargs_passed_through(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([{"x": 1}])
        db = Mongo(host="h", username="u", password="p")
        db.execute("find", {"a": 1}, database="d", collection="c", limit=10)
        coll.find.assert_called_once_with({"a": 1}, limit=10)

    def test_returns_list_when_result_is_cursor(self, mock_client):
        # pymongo 4 fix: cursor results are materialized via list(result) so we
        # don't try to iterate after the connection context exits.
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = _cursor_mock([{"i": 0}, {"i": 1}])
        db = Mongo(host="h", username="u", password="p")
        out = db.execute("find", {}, database="d", collection="c")
        assert isinstance(out, list)
        assert out == [{"i": 0}, {"i": 1}]

    def test_logs_database_and_collection(self, mock_client, caplog):
        _, _, coll = _wire_client(mock_client)
        coll.insert_many.return_value = iter([1])
        db = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            db.execute(
                "insert_many",
                [{"a": 1}],
                database="mydb",
                collection="mycol",
            )
        assert any("Using database mydb" in r.message for r in caplog.records)
        assert any("Using collection mycol" in r.message for r in caplog.records)

    def test_no_db_log_when_database_missing(self, mock_client, caplog):
        client, _, _ = _wire_client(mock_client)
        client.server_info.return_value = iter([{"v": "7.0"}])
        db = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("server_info")
        assert not any("Using database" in r.message for r in caplog.records)
        assert not any("Using collection" in r.message for r in caplog.records)

    def test_no_collection_log_when_only_database_provided(self, mock_client, caplog):
        _client, db, _ = _wire_client(mock_client)
        db.command.return_value = iter([{"ok": 1}])
        backend = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            backend.execute("command", "ping", database="admin")
        assert any("Using database admin" in r.message for r in caplog.records)
        assert not any("Using collection" in r.message for r in caplog.records)

    def test_logs_args_kwargs_operation(self, mock_client, caplog):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([])
        db = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            db.execute("find", {"a": 1}, database="d", collection="c", limit=5)
        assert any("args:" in r.message for r in caplog.records)
        assert any("kwargs:" in r.message for r in caplog.records)
        assert any("Started executing find" in r.message for r in caplog.records)
        assert any("Not streaming results from DB." in r.message for r in caplog.records)
        assert any("Executed in" in r.message for r in caplog.records)
        assert any("Ended find execution at" in r.message for r in caplog.records)


class TestMongoStream:
    def test_yields_rows(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([{"i": 0}, {"i": 1}])
        db = Mongo(host="h", username="u", password="p")
        gen = db.execute("find", {}, database="d", collection="c", stream=True)
        assert list(gen) == [{"i": 0}, {"i": 1}]
        coll.find.assert_called_once_with({})

    def test_passes_kwargs(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([])
        db = Mongo(host="h", username="u", password="p")
        list(
            db.execute(
                "find",
                {"a": 1},
                database="d",
                collection="c",
                limit=10,
                stream=True,
            )
        )
        coll.find.assert_called_once_with({"a": 1}, limit=10)

    def test_missing_database_raises(self, mock_client):
        _wire_client(mock_client)
        db = Mongo(host="h", username="u", password="p")
        gen = db.execute("find", {}, collection="c", stream=True)
        with pytest.raises(KeyError):
            list(gen)

    def test_missing_collection_raises(self, mock_client):
        _wire_client(mock_client)
        db = Mongo(host="h", username="u", password="p")
        gen = db.execute("find", {}, database="d", stream=True)
        with pytest.raises(KeyError):
            list(gen)

    def test_missing_both_raises(self, mock_client):
        _wire_client(mock_client)
        db = Mongo(host="h", username="u", password="p")
        gen = db.execute("find", {}, stream=True)
        with pytest.raises(KeyError):
            list(gen)

    def test_logs_streaming_message(self, mock_client, caplog):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([])
        db = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("find", database="d", collection="c", stream=True))
        assert any("Streaming results from DB." in r.message for r in caplog.records)

    def test_logs_execution_lifecycle(self, mock_client, caplog):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([{"i": 1}])
        db = Mongo(host="h", username="u", password="p")
        with caplog.at_level("INFO"):
            list(db.execute("find", database="d", collection="c", stream=True))
        assert any("Using database d" in r.message for r in caplog.records)
        assert any("Using collection c" in r.message for r in caplog.records)
        assert any("Started executing find" in r.message for r in caplog.records)
        assert any("Executed in" in r.message for r in caplog.records)
        assert any("Ended find execution at" in r.message for r in caplog.records)
