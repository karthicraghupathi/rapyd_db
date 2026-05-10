from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pymssql

from rapyd_db.loggingadapter import LogIdAdapter
from rapyd_db.utils import _assign_if_not_none, _get_uuid

from . import AbstractBackend, get_connection

_logger = logging.getLogger(__name__)


class MSSQL(AbstractBackend):
    def __init__(
        self,
        host: str | None = None,
        user: str | None = None,
        password: str | None = None,
        **kwargs: Any,
    ) -> None:
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
        self._connection_params = {}
        _assign_if_not_none(self._connection_params, "host", host)
        _assign_if_not_none(self._connection_params, "user", user)
        _assign_if_not_none(self._connection_params, "password", password)
        self._connection_params.update(kwargs)
        # we will force as_dict to True
        self._connection_params["as_dict"] = True

    def _connect(self) -> Any:
        return pymssql.connect(**self._connection_params)

    def execute(
        self,
        query: str,
        params: tuple[Any, ...] | None = None,
        stream: bool = False,
    ) -> tuple[int, int | None, list[dict[str, Any]]] | Iterator[dict[str, Any]]:
        """
        Executes the query and returns the result.

        :param str query: The query to execute.
        :param tuple params: A tuple of parameters for substitution prior to executing the query.
        :param bool stream:
            When `True`, a generator is returned which fetches rows from the
            DB lazily. Typically used when returning large result sets while
            avoiding `MemoryError`.
        :return:
            When `stream=False` (default), returns a tuple of
            `(rows_affected, lastrowid, results_list)` where `results_list` is
            a list of dict rows.
            When `stream=True`, returns a generator yielding dict rows one at
            a time.
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

    def _stream(
        self,
        query: str,
        params: tuple[Any, ...] | None,
    ) -> Iterator[dict[str, Any]]:
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, {"log_id": log_id})

        with get_connection(self, log_id) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            adapter.info(f"Starting executing query at {execution_start}")
            adapter.info("Streaming results from DB.")

            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            adapter.info(f"Query: {query}")
            adapter.info(f"Params: {params}")

            # returns the generator object
            yield from cursor

            execution_end = datetime.now()
            adapter.info(f"Executed in {(execution_end - execution_start).seconds} second(s)")
            adapter.info(f"Ended query execution at {execution_end}")

    def _no_stream(
        self,
        query: str,
        params: tuple[Any, ...] | None,
    ) -> tuple[int, int | None, list[dict[str, Any]]]:
        # setup logging
        log_id = _get_uuid()
        adapter = LogIdAdapter(_logger, {"log_id": log_id})

        with get_connection(self, log_id) as connection:
            connection.autocommit(True)
            cursor = connection.cursor()
            execution_start = datetime.now()
            adapter.info(f"Starting executing query at {execution_start}")
            adapter.info("Not streaming results from DB.")
            adapter.info(f"Query: {query}")
            adapter.info(f"Params: {params}")

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
                expected_msg = "Statement not executed or executed statement has no resultset"
                if e.args and e.args[0] == expected_msg:
                    result = []
                else:
                    raise

            execution_end = datetime.now()
            adapter.info(
                f"{cursor.rowcount} row(s) affected in {(execution_end - execution_start).seconds} second(s)"
            )
            adapter.info(f"Ended query execution at {execution_end}")

            # returns rows affected and all results
            return cursor.rowcount, cursor.lastrowid, result
