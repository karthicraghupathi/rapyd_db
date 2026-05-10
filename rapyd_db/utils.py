from __future__ import annotations

import uuid
from typing import Any


def _assign_if_not_none(obj: Any, param: str, value: Any) -> bool:
    """A method to quickly assign a value if it is not none to either a dictionary or an object."""
    if value:
        if isinstance(obj, dict):
            obj[param] = value
        else:
            setattr(obj, param, value)
        return True
    return False


def _get_uuid() -> str:
    """Returns a unique ID which can be used to track log messages by query."""
    return uuid.uuid4().hex
