from __future__ import annotations

import abc
import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from ..loggingadapter import LogIdAdapter

_logger = logging.getLogger(__name__)


class AbstractBackend(metaclass=abc.ABCMeta):
    _connection_params: dict[str, Any]

    @abc.abstractmethod
    def _connect(self) -> Any:
        """Connect to the backend and return a driver connection."""

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the query and return the result."""


@contextmanager
def get_connection(
    backend: AbstractBackend,
    log_id: Optional[str] = None,
) -> Iterator[Any]:
    """Returns a DB connection."""
    adapter = LogIdAdapter(_logger, {"log_id": log_id})
    try:
        adapter.info("Connecting to DB")
        connection = backend._connect()
    except Exception:
        adapter.exception("Cannot connect to DB")
        raise
    try:
        yield connection
    finally:
        try:
            adapter.info("Closed connection to DB")
            connection.close()
        except Exception:
            pass
