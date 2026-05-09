import re
import pytest

from rapyd_db.utils import _assign_if_not_none, _get_uuid


class _Bag:
    pass


class TestAssignIfNotNone:
    def test_assigns_to_dict_when_value_truthy(self):
        d = {}
        assert _assign_if_not_none(d, "k", "v") is True
        assert d == {"k": "v"}

    def test_assigns_to_object_when_value_truthy(self):
        b = _Bag()
        assert _assign_if_not_none(b, "k", 42) is True
        assert b.k == 42

    def test_does_nothing_when_value_is_none(self):
        d = {}
        assert _assign_if_not_none(d, "k", None) is False
        assert d == {}

    def test_does_nothing_when_value_is_falsy(self):
        # current implementation treats empty string and 0 as "skip"
        d = {}
        assert _assign_if_not_none(d, "k", "") is False
        assert _assign_if_not_none(d, "k", 0) is False
        assert d == {}


class TestGetUuid:
    def test_returns_32_char_hex(self):
        u = _get_uuid()
        assert isinstance(u, str)
        assert re.fullmatch(r"[0-9a-f]{32}", u)

    def test_uniqueness(self):
        assert _get_uuid() != _get_uuid()
