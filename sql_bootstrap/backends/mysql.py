import logging
import MySQLdb

from datetime import datetime
from MySQLdb.cursors import DictCursor, SSDictCursor

from . import AbstractBackend, _get_connection
from ..utils import _assign_if_not_none, _get_uuid


logger = logging.getLogger(__name__)


class MySQL(AbstractBackend):
    def __init__(
        self,
        host=None,
        user=None,
        password=None,
        database=None,
        **kwargs
    ):
        """
        Initializes an instance of the MySQL backend with the connection parameters.

        :param str host: Name of the host to connect to.
        :param str user: User to authenticate as.
        :param str password: Password to authenticate with.
        :param str database: Database to use.
        :param kwargs:
            All other parameters supported by the MySQLdb `connect()` method.
            Refer https://mysqlclient.readthedocs.io/user_guide.html#functions-and-attributes for additional examples.
            Note: `cursorclass` is limited to return dictionaries only and cannot be changed.
        """
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, 'host', host)
        _assign_if_not_none(self._connection_params, 'user', user)
        _assign_if_not_none(self._connection_params, 'password', password)
        _assign_if_not_none(self._connection_params, 'database', database)
        self._connection_params.update(kwargs)
        # we will remove cursor class from as this will be set in the underlying methods
        self._connection_params.pop('cursorclass', None)

    def _connect(self):
        return MySQLdb.connect(**self._connection_params)

    def execute(self, query, params=None, stream=True):
        """
        Executes the query and returns the result.

        :param str query: The query to execute.
        :param tuple params: A tuple of parameters for substitution prior to executing the query.
        :param bool stream:
            When `True`, a generator is returned which will fetch data from the
            DB in a lazy fashion. Typically used when you want to
            return large volumes of data from the DB while while avoiding `MemoryError`.
        :return:
            Returns a generator when `stream` is `True`. Otherwise returns a
            tuple of the rows affected and a list of all rows returned after
            query execution.
        """
        uuid = _get_uuid()
        # the return has to be done this way to accommodate having
        # `yield` and `return` in the same method
        # https://stackoverflow.com/a/43459115/399435
        # unfortunately there is a lot of code duplication here
        if stream:
            # when streaming, we want to keep results on the server side to reduce client side memory footprint
            self._connection_params['cursorclass'] = SSDictCursor
            return self._stream(uuid, query, params)
        else:
            self._connection_params['cursorclass'] = DictCursor
            return self._no_stream(uuid, query, params)

    def _stream(self, uuid, query, params):
        with _get_connection(self, uuid) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )
            logger.info('{} - Streaming results from DB.'.format(uuid))

            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            logger.info('{} - {}'.format(uuid, cursor._last_executed.decode('utf8')))

            # returns the generator object
            for row in cursor:
                yield row

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info('{} - Ended query execution at {}'.format(uuid, execution_end))

    def _no_stream(self, uuid, query, params):
        with _get_connection(self, uuid) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )
            logger.info('{} - Not streaming results from DB.'.format(uuid))

            if params is not None:
                rows_affected = cursor.execute(query, params)
            else:
                rows_affected = cursor.execute(query)

            execution_end = datetime.now()
            logger.info('{} - {}'.format(uuid, cursor._last_executed.decode('utf8')))
            logger.info(
                '{} - {} row(s) affected in {} second(s)'.format(
                    uuid, rows_affected, (execution_end - execution_start).seconds
                )
            )
            logger.info('{} - Ended query execution at {}'.format(uuid, execution_end))

            # returns rows affected and all results
            return rows_affected, cursor.fetchall()
