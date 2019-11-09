import logging

from datetime import datetime
from pymongo import MongoClient

from . import AbstractBackend, _get_connection
from ..utils import _assign_if_not_none, _get_uuid


logger = logging.getLogger(__name__)


class Mongo(AbstractBackend):
    def __init__(
        self,
        db,
        host=None,
        username=None,
        password=None,
        auth_source='admin',
        max_pool_size=1,
        **kwargs
    ):
        self._db = db
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, 'host', host)
        _assign_if_not_none(self._connection_params, 'username', username)
        _assign_if_not_none(self._connection_params, 'password', password)
        _assign_if_not_none(self._connection_params, 'authSource', auth_source)
        _assign_if_not_none(self._connection_params, 'maxPoolSize', max_pool_size)
        self._connection_params.update(kwargs)

    def _connect(self):
        return MongoClient(**self._connection_params)[self._db]

    # def find(self, collection, *args, **kwargs):
    #     uuid = _get_uuid()
    #     with _get_connection(self, uuid) as connection:
    #         execution_start = datetime.now()
    #         logger.info('{} - Using collection {}'.format(uuid, collection))
    #         logger.info('{} - args: {}'.format(uuid, args))
    #         logger.info('{} - kwargs: {}'.format(uuid, kwargs))
    #         logger.info(
    #             '{} - Started executing find at {}'.format(uuid, execution_start)
    #         )
    #
    #         cursor = connection[collection].find(*args, **kwargs)
    #
    #         execution_end = datetime.now()
    #         logger.info(
    #             '{} - Executed in {} second(s)'.format(
    #                 uuid, (execution_end - execution_start).seconds
    #             )
    #         )
    #         logger.info('{} - Ended find execution at {}'.format(uuid, execution_end))
    #
    #         for row in cursor:
    #             yield row

    def read(self, collection, *args, **kwargs):
        uuid = _get_uuid()
        with _get_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info('{} - Using collection {}'.format(uuid, collection))
            logger.info('{} - args: {}'.format(uuid, args))
            logger.info('{} - kwargs: {}'.format(uuid, kwargs))
            logger.info(
                '{} - Started executing aggregation at {}'.format(
                    uuid, execution_start
                )
            )

            cursor = connection[collection].aggregate(*args, **kwargs)

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info(
                '{} - Ended aggregation execution at {}'.format(uuid, execution_end)
            )

            for row in cursor:
                yield row

    def write(self, collection, operation, *args, **kwargs):
        uuid = _get_uuid()
        with _get_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info('{} - Using collection {}'.format(uuid, collection))
            logger.info('{} - args: {}'.format(uuid, args))
            logger.info('{} - kwargs: {}'.format(uuid, kwargs))
            logger.info(
                '{} - Started executing {} at {}'.format(
                    uuid, operation, execution_start
                )
            )

            result = getattr(connection[collection], operation)(*args, **kwargs)

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info(
                '{} - Ended {} execution at {}'.format(uuid, operation, execution_end)
            )

            return result
