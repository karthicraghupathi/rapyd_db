import abc
import logging

import six

from contextlib import contextmanager

from ..loggingadapter import LogIdAdapter

_logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class AbstractBackend:
    _connection_params = None

    @abc.abstractmethod
    def _connect(self):
        """Connects to the backend and returns a connection."""

    def execute(self, stream=False, *args, **kwargs):
        """Executes the query and returns the result."""


@contextmanager
def get_connection(backend, log_id=None):
    """Returns a DB connection."""
    adapter = LogIdAdapter(_logger, dict(log_id=log_id))

    try:
        adapter.info("Connecting to DB")
        connection = backend._connect()
    except:
        adapter.exception("Cannot connect to DB")
        raise

    try:
        yield connection
    finally:
        try:
            adapter.info("Closed connection to DB")
            connection.close()
        except:
            pass
