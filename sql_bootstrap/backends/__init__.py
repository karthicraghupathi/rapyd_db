import logging

from abc import ABC, abstractmethod
from contextlib import contextmanager


logger = logging.getLogger(__name__)


class AbstractBackend(ABC):
    _connection_params = None

    @abstractmethod
    def _connect(self):
        """Connects to the backend and returns a connection."""

    def execute(self, stream=True, *args, **kwargs):
        """Executes the query and returns the result."""


@contextmanager
def get_connection(backend, uuid):
    """Returns a DB connection."""
    try:
        logger.info('{} - Connecting to DB'.format(uuid))
        connection = backend._connect()
    except:
        logger.exception('{} - Cannot connect to DB'.format(uuid))
        raise

    try:
        yield connection
    finally:
        try:
            logger.info('{} - Closed connection to DB'.format(uuid))
            connection.close()
        except:
            pass
