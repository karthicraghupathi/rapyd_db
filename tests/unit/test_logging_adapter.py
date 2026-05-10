import logging

from rapyd_db.loggingadapter import LogIdAdapter


def _adapter(extra):
    base = logging.getLogger("rapyd_db.test_adapter")
    return LogIdAdapter(base, extra)


def test_prepends_log_id_when_present():
    adapter = _adapter({"log_id": "abc123"})
    msg, kwargs = adapter.process("hello", {})
    assert msg == "abc123 - hello"
    assert kwargs == {}


def test_returns_msg_unchanged_when_log_id_missing():
    adapter = _adapter({})
    msg, kwargs = adapter.process("hello", {"foo": 1})
    assert msg == "hello"
    assert kwargs == {"foo": 1}


def test_returns_msg_unchanged_when_log_id_falsy():
    adapter = _adapter({"log_id": None})
    msg, _kwargs = adapter.process("hello", {})
    assert msg == "hello"
