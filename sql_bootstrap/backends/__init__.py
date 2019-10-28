import logging

from abc import ABC, abstractmethod
from contextlib import contextmanager


logger = logging.getLogger(__name__)


class AbstractBackend(ABC):
    _connection_params = None

    @abstractmethod
    def _connect(self):
        """Connects to the backend and returns a connection."""


@contextmanager
def _get_db_cursor(backend, uuid):
    """Return a database connection and cursor."""
    try:
        logger.info('{} - Connecting to DB'.format(uuid))
        connection = backend._connect()
        connection.autocommit(True)
    except:
        logger.exception('{} - Cannot connect to DB'.format(uuid))
        raise

    cursor = connection.cursor()

    try:
        yield connection, cursor
    finally:
        try:
            logger.info('{} - Closed connection to DB'.format(uuid))
            connection.close()
        except:
            pass
