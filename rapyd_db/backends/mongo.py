import logging

from datetime import datetime
from pymongo import MongoClient

from . import AbstractBackend, get_connection
from ..loggingadapter import LogIdAdapter
from ..utils import _assign_if_not_none, _get_uuid


_logger = logging.getLogger(__name__)


class Mongo(AbstractBackend):
    def __init__(
        self,
        host=None,
        username=None,
        password=None,
        auth_source="admin",
        connect_timeout_ms=2000,
        **kwargs
    ):
        """
        Initializes an instance of the Mongo backend with the connection parameters.

        :param str host: Can be a full mongoDB URI or a simple hostname.
        :param str username: User to authenticate as.
        :param str password: Password to authenticate with.
        :param str auth_source: Database to authenticate against. Defaults to admin.
        :param int connect_timeout_ms:
            How long to wait when connecting to server before concluding server is unavailable.
            Defaults to 2000 (2 seconds).
        :param kwargs:
            All other parameters supported by the MongoClient `__init__()` method.
            Refer https://api.mongodb.com/python/current/api/pymongo/mongo_client.html for additional examples.
            Note: `maxPoolSize` is set to 1 only and cannot be changed.
            Note: `connect` is also set to False because a connection should only occur while querying
        """
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, "host", host)
        _assign_if_not_none(self._connection_params, "username", username)
        _assign_if_not_none(self._connection_params, "password", password)
        _assign_if_not_none(self._connection_params, "authSource", auth_source)
        _assign_if_not_none(
            self._connection_params, "connectTimeoutMS", connect_timeout_ms
        )
        self._connection_params.update(kwargs)
        self._connection_params["connect"] = False
        self._connection_params["maxPoolSize"] = 1

    def _connect(self):
        return MongoClient(**self._connection_params)

    def execute(self, operation, *args, **kwargs):
        """
        Executes the query and returns the result.

        :param str operation: Operation to run.
        :param str database: Database to use.
        :param str collection: Collection to use.
        :param bool stream:
            When `True`, a generator is returned which will fetch data from the
            DB in a lazy fashion. Typically used when you want to
            return large volumes of data from the DB while while avoiding `MemoryError`.
            Parameters `database` and `collection` are required when `stream=True`.
        :param args:
            All other positional arguments supported by the method you are calling via operation.
        :param kwargs:
            All other keyword arguments supported by the method you are calling via operation.
        :return:
            Returns a generator when `stream` is `True`. Otherwise returns a
            the result of the method you are calling via operation.
        """
        # in python 2 default arguments cannot be used with args and kwargs
        # https://stackoverflow.com/a/15302038/399435
        # so doing it this way
        stream = kwargs.pop("stream", False)

        # the return has to be done this way to accommodate having
        # `yield` and `return` in the same method
        # https://stackoverflow.com/a/43459115/399435
        # unfortunately there is a lot of code duplication here
        if stream:
            # when streaming, we want to keep results on the server side to reduce client side memory footprint
            return self._stream(operation, *args, **kwargs)
        else:
            return self._no_stream(operation, *args, **kwargs)

    def _stream(self, operation, *args, **kwargs):
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, dict(log_id=log_id))

        # get some optional parms if present
        database = kwargs.pop("database", None)
        collection = kwargs.pop("collection", None)
        if database is None or collection is None:
            raise KeyError(
                "Parameters 'database' and 'collection' are required when stream=True"
            )

        with get_connection(self, log_id) as connection:
            execution_start = datetime.now()
            adapter.info("Using database {}".format(database))
            adapter.info("Using collection {}".format(collection))
            adapter.info("args: {}".format(args))
            adapter.info("kwargs: {}".format(kwargs))
            adapter.info(
                "Started executing {} at {}".format(operation, execution_start)
            )
            adapter.info("Streaming results from DB.")
            operation_callable = getattr(connection[database][collection], operation)
            result = operation_callable(*args, **kwargs)

            # returns the generator object
            for row in result:
                yield row

            execution_end = datetime.now()
            adapter.info(
                "Executed in {} second(s)".format(
                    (execution_end - execution_start).seconds
                )
            )
            adapter.info("Ended {} execution at {}".format(operation, execution_end))

    def _no_stream(self, operation, *args, **kwargs):
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, dict(log_id=log_id))

        # get some optional parms if present
        database = kwargs.pop("database", None)
        collection = kwargs.pop("collection", None)

        with get_connection(self, log_id) as connection:
            execution_start = datetime.now()
            if database is not None:
                adapter.info("Using database {}".format(database))
            if collection is not None:
                adapter.info("Using collection {}".format(collection))
            adapter.info("args: {}".format(args))
            adapter.info("kwargs: {}".format(kwargs))
            adapter.info(
                "Started executing {} at {}".format(operation, execution_start)
            )
            adapter.info("Not streaming results from DB.")

            if database is not None and collection is not None:
                operation_callable = getattr(
                    connection[database][collection], operation
                )
            elif database is not None and collection is None:
                operation_callable = getattr(connection[database], operation)
            else:
                operation_callable = getattr(connection, operation)
            result = operation_callable(*args, **kwargs)

            execution_end = datetime.now()
            adapter.info(
                "Executed in {} second(s)".format(
                    (execution_end - execution_start).seconds
                )
            )
            adapter.info("Ended {} execution at {}".format(operation, execution_end))
            return result
