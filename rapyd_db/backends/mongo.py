from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from pymongo import MongoClient

from rapyd_db.loggingadapter import LogIdAdapter
from rapyd_db.utils import _assign_if_not_none, _get_uuid

from . import AbstractBackend, get_connection

_logger = logging.getLogger(__name__)


class Mongo(AbstractBackend):
    def __init__(
        self,
        host: str | None = None,
        username: str | None = None,
        password: str | None = None,
        auth_source: str = "admin",
        connect_timeout_ms: int = 2000,
        **kwargs: Any,
    ) -> None:
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
        self._connection_params = {}
        _assign_if_not_none(self._connection_params, "host", host)
        _assign_if_not_none(self._connection_params, "username", username)
        _assign_if_not_none(self._connection_params, "password", password)
        _assign_if_not_none(self._connection_params, "authSource", auth_source)
        _assign_if_not_none(self._connection_params, "connectTimeoutMS", connect_timeout_ms)
        self._connection_params.update(kwargs)
        self._connection_params["connect"] = False
        self._connection_params["maxPoolSize"] = 1

    def _connect(self) -> Any:
        return MongoClient(**self._connection_params)

    def execute(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        """
        Dispatches a named pymongo operation and returns the result.

        The first positional argument is the operation name (a string method
        name on the resolved pymongo target). The dispatch target is chosen
        from the `database` / `collection` keyword arguments:

        - `database` and `collection` both provided -> calls
          `client[database][collection].operation(*args, **kwargs)`
          (collection-level methods such as `find`, `insert_one`, etc.).
        - `database` only -> calls `client[database].operation(*args, **kwargs)`
          (database-level commands).
        - neither -> calls `client.operation(*args, **kwargs)`
          (client-level methods such as `server_info`).

        :param str operation: Name of the pymongo method to invoke on the
            resolved dispatch target.
        :param str database: (keyword) Database to use; consumed before dispatch.
        :param str collection: (keyword) Collection to use; consumed before dispatch.
        :param bool stream: (keyword)
            When `True`, a generator is returned which iterates lazily over
            the operation's result (e.g. a cursor from `find`). Typically
            used when returning large result sets while avoiding `MemoryError`.
            Both `database` and `collection` are required when `stream=True`.
        :param args:
            Remaining positional arguments forwarded to the pymongo method.
        :param kwargs:
            Remaining keyword arguments forwarded to the pymongo method
            (after `database`, `collection`, and `stream` are consumed).
        :return:
            When `stream=True`, returns a generator yielding items from the
            underlying pymongo cursor.
            When `stream=False`, returns the materialized result of the
            dispatched call (lists are returned as `list(result)`).
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

    def _stream(self, operation: str, *args: Any, **kwargs: Any) -> Iterator[Any]:
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, {"log_id": log_id})

        # get some optional parms if present
        database = kwargs.pop("database", None)
        collection = kwargs.pop("collection", None)
        if database is None or collection is None:
            raise KeyError("Parameters 'database' and 'collection' are required when stream=True")

        with get_connection(self, log_id) as connection:
            execution_start = datetime.now()
            adapter.info(f"Using database {database}")
            adapter.info(f"Using collection {collection}")
            adapter.info(f"args: {args}")
            adapter.info(f"kwargs: {kwargs}")
            adapter.info(f"Started executing {operation} at {execution_start}")
            adapter.info("Streaming results from DB.")
            operation_callable = getattr(connection[database][collection], operation)
            result = operation_callable(*args, **kwargs)

            # returns the generator object
            yield from result

            execution_end = datetime.now()
            adapter.info(f"Executed in {(execution_end - execution_start).seconds} second(s)")
            adapter.info(f"Ended {operation} execution at {execution_end}")

    def _no_stream(self, operation: str, *args: Any, **kwargs: Any) -> list[Any]:
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, {"log_id": log_id})

        # get some optional parms if present
        database = kwargs.pop("database", None)
        collection = kwargs.pop("collection", None)

        with get_connection(self, log_id) as connection:
            execution_start = datetime.now()
            if database is not None:
                adapter.info(f"Using database {database}")
            if collection is not None:
                adapter.info(f"Using collection {collection}")
            adapter.info(f"args: {args}")
            adapter.info(f"kwargs: {kwargs}")
            adapter.info(f"Started executing {operation} at {execution_start}")
            adapter.info("Not streaming results from DB.")

            if database is not None and collection is not None:
                operation_callable = getattr(connection[database][collection], operation)
            elif database is not None and collection is None:
                operation_callable = getattr(connection[database], operation)
            else:
                operation_callable = getattr(connection, operation)
            result = operation_callable(*args, **kwargs)

            execution_end = datetime.now()
            adapter.info(f"Executed in {(execution_end - execution_start).seconds} second(s)")
            adapter.info(f"Ended {operation} execution at {execution_end}")
            return list(result)
