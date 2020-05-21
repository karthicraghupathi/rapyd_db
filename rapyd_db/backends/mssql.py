import logging
import pymssql

from datetime import datetime

from . import AbstractBackend, get_connection
from ..loggingadapter import LogIdAdapter
from ..utils import _assign_if_not_none, _get_uuid


_logger = logging.getLogger(__name__)


class MSSQL(AbstractBackend):
    def __init__(self, host=None, user=None, password=None, **kwargs):
        """
        Initializes an instance of the MSSQL backend with the connection parameters.

        :param str host: Name of the host and instance to connect to.
        :param str user: User to authenticate as.
        :param str password: Password to authenticate with.
        :param str database:
            Database to use.
            By default SQL Server selects the database which is set as default for specific user.
        :param kwargs:
            All other parameters supported by the MySQLdb `connect()` method.
            Refer http://www.pymssql.org/en/stable/ref/pymssql.html#pymssql.connect for additional examples.
            Note: `as_dict` is limited to return dictionaries only and cannot be changed.
        """
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, "host", host)
        _assign_if_not_none(self._connection_params, "user", user)
        _assign_if_not_none(self._connection_params, "password", password)
        self._connection_params.update(kwargs)
        # we will force as_dict to True
        self._connection_params["as_dict"] = True

    def _connect(self):
        pymssql.set_max_connections(1)
        return pymssql.connect(**self._connection_params)

    def execute(self, query, params=None, stream=False):
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
        # the return has to be done this way to accommodate having
        # `yield` and `return` in the same method
        # https://stackoverflow.com/a/43459115/399435
        # unfortunately there is a lot of code duplication here
        if stream:
            # when streaming, we want to keep results on the server side to reduce client side memory footprint
            return self._stream(query, params)
        else:
            return self._no_stream(query, params)

    def _stream(self, query, params):
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, dict(log_id=log_id))

        with get_connection(self, log_id) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            adapter.info("Starting executing query at {}".format(execution_start))
            adapter.info("Streaming results from DB.")

            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            adapter.info("Query: {}".format(query))
            adapter.info("Params: {}".format(params))

            # returns the generator object
            for row in cursor:
                yield row

            execution_end = datetime.now()
            adapter.info(
                "Executed in {} second(s)".format(
                    (execution_end - execution_start).seconds
                )
            )
            adapter.info("Ended query execution at {}".format(execution_end))

    def _no_stream(self, query, params):
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, dict(log_id=log_id))

        with get_connection(self, log_id) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            adapter.info("Starting executing query at {}".format(execution_start))
            adapter.info("Not streaming results from DB.")
            adapter.info("Query: {}".format(query))
            adapter.info("Params: {}".format(params))

            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # This driver seems to be having issues fetching results from MS SQL Server
            # Not sure where the issue lies but for now I'm going to handle this
            # I'll need to see if this can be done in a better fashion
            try:
                result = cursor.fetchall()
            except pymssql.OperationalError as e:
                expected_msg = (
                    "Statement not executed or executed statement has no resultset"
                )
                if expected_msg == e.message:
                    result = []
                else:
                    raise e

            execution_end = datetime.now()
            adapter.info(
                "{} row(s) affected in {} second(s)".format(
                    cursor.rowcount, (execution_end - execution_start).seconds
                )
            )
            adapter.info("Ended query execution at {}".format(execution_end))

            # returns rows affected and all results
            return cursor.rowcount, cursor.lastrowid, result
