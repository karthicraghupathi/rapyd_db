import logging
import MySQLdb

from datetime import datetime
from MySQLdb.cursors import DictCursor

from . import AbstractBackend, _get_db_cursor
from ..utils import _assign_if_not_none, _get_uuid


logger = logging.getLogger(__name__)


class MySQL(AbstractBackend):
    def __init__(
        self,
        host=None,
        user=None,
        passwd=None,
        db=None,
        cursorclass=DictCursor,
        **kwargs
    ):
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, 'host', host)
        _assign_if_not_none(self._connection_params, 'user', user)
        _assign_if_not_none(self._connection_params, 'passwd', passwd)
        _assign_if_not_none(self._connection_params, 'db', db)
        _assign_if_not_none(self._connection_params, 'cursorclass', cursorclass)
        self._connection_params.update(kwargs)

    def _connect(self):
        return MySQLdb.connect(**self._connection_params)

    def execute(self, query, params=None):
        uuid = _get_uuid()
        with _get_db_cursor(self, uuid) as (connection, cursor):
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )

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

            for row in cursor:
                yield row
