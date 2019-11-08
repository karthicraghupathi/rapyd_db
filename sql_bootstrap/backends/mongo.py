import logging

from datetime import datetime
from pymongo import MongoClient

from . import AbstractBackend, _get_db_cursor, _get_mongo_connection
from ..utils import _assign_if_not_none, _get_uuid


logger = logging.getLogger(__name__)


class Mongo(AbstractBackend):
    def __init__(
        self,
        host=None,
        username=None,
        password=None,
        db=None,
        authSource='admin',
        maxPoolSize=1,
        **kwargs
    ):
        self._db = db
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, 'host', host)
        _assign_if_not_none(self._connection_params, 'user', user)
        _assign_if_not_none(self._connection_params, 'passwd', passwd)
        _assign_if_not_none(self._connection_params, 'db', db)
        _assign_if_not_none(self._connection_params, 'cursorclass', cursorclass)
        self._connection_params.update(kwargs)

    def _connect(self):
        if self._db is not None:
            return MongoClient(**self._connection_params)[self._db]
        else:
            return MongoClient(**self._connection_params)

    def find(self, *args, **kwargs):
        uuid = _get_uuid()
        with _get_mongo_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )

            rows = connection.find(*args, **kwargs)

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info('{} - Ended query execution at {}'.format(uuid, execution_end))

            for row in rows:
                yield row

    def aggregate(self):
        uuid = _get_uuid()
        with _get_mongo_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )

            rows = connection.aggregate(*args, **kwargs)

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info('{} - Ended query execution at {}'.format(uuid, execution_end))

            for row in rows:
                yield row

    def execute(self, operation, *args, **kwargs):
        uuid = _get_uuid()
        with _get_mongo_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info(
                '{} - Starting executing query at {}'.format(uuid, execution_start)
            )

            yield getattr(connection, operation)(*args, **kwargs)

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info('{} - Ended query execution at {}'.format(uuid, execution_end))
