import logging

from abc import ABC, abstractmethod
from contextlib import contextmanager


logger = logging.getLogger(__name__)


class AbstractBackend(ABC):
    _connection_params = None

    @abstractmethod
    def _connect(self):
        """Connects to the backend and returns a connection."""

    def read(self, *args, **kwargs):
        """Reads data from the DB. Uses `yield` / `generator` to optimize fetching large volumes of data."""

    def write(self, *args, **kwargs):
        """Writes data from the DB. Does NOT use `yield` / `generator`."""

# @contextmanager
# def _get_db_cursor(backend, uuid):
#     """Return a database connection and cursor."""
#     try:
#         logger.info('{} - Connecting to DB'.format(uuid))
#         connection = backend._connect()
#         connection.autocommit(True)
#     except:
#         logger.exception('{} - Cannot connect to DB'.format(uuid))
#         raise
#
#     cursor = connection.cursor()
#
#     try:
#         yield connection, cursor
#     finally:
#         try:
#             logger.info('{} - Closed connection to DB'.format(uuid))
#             connection.close()
#         except:
#             pass


@contextmanager
def _get_connection(backend, uuid):
    """Return a DB connection."""
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
