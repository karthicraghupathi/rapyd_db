import logging

from datetime import datetime
from pymongo import MongoClient

from . import AbstractBackend, get_connection
from ..utils import _assign_if_not_none, _get_uuid


logger = logging.getLogger(__name__)


class Mongo(AbstractBackend):
    def __init__(
        self,
        host=None,
        username=None,
        password=None,
        database=None,
        auth_source='admin',
        connect_timeout_ms=2000,
        **kwargs
    ):
        """
        Initializes an instance of the Mongo backend with the connection parameters.

        :param str host: Can be a full mongoDB URI or a simple hostname.
        :param str username: User to authenticate as.
        :param str password: Password to authenticate with.
        :param str database: Database to use.
        :param str auth_source: Database to authenticate against. Defaults to admin.
        :param int connect_timeout_ms:
            How long to wait when connecting to server before concluding server is unavailable.
            Defaults to 2000 (2 seconds).
        :param kwargs:
            All other parameters supported by the MongoClient `__init__()` method.
            Refer https://api.mongodb.com/python/current/api/pymongo/mongo_client.html for additional examples.
            Note: `maxPoolSize` is set to 1 only and cannot be changed.
        """
        self._database = database
        self._connection_params = dict()
        _assign_if_not_none(self._connection_params, 'host', host)
        _assign_if_not_none(self._connection_params, 'username', username)
        _assign_if_not_none(self._connection_params, 'password', password)
        _assign_if_not_none(self._connection_params, 'authSource', auth_source)
        _assign_if_not_none(self._connection_params, 'connectTimeoutMS', connect_timeout_ms)
        self._connection_params.update(kwargs)
        self._connection_params['maxPoolSize'] = 1

    def _connect(self):
        if self._database is not None:
            return MongoClient(**self._connection_params)[self._database]
        else:
            return MongoClient(**self._connection_params)

    def execute(self, collection, operation, *args, stream=True, **kwargs):
        """
        Executes the query and returns the result.

        :param str collection: Collection to use.
        :param str operation: Operation to run.
        :param bool stream:
            When `True`, a generator is returned which will fetch data from the
            DB in a lazy fashion. Typically used when you want to
            return large volumes of data from the DB while while avoiding `MemoryError`.
        :param args:
            All other positional arguments supported by the method you are calling via operation.
        :param kwargs:
            All other keyword arguments supported by the method you are calling via operation.
        :return:
            Returns a generator when `stream` is `True`. Otherwise returns a
            the result of the method you are calling via operation.
        """
        uuid = _get_uuid()
        # the return has to be done this way to accommodate having
        # `yield` and `return` in the same method
        # https://stackoverflow.com/a/43459115/399435
        # unfortunately there is a lot of code duplication here
        if stream:
            # when streaming, we want to keep results on the server side to reduce client side memory footprint
            return self._stream(uuid, collection, operation, *args, **kwargs)
        else:
            return self._no_stream(uuid, collection, operation, *args, **kwargs)

    def _stream(self, uuid, collection, operation, *args, **kwargs):
        with get_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info('{} - Using collection {}'.format(uuid, collection))
            logger.info('{} - args: {}'.format(uuid, args))
            logger.info('{} - kwargs: {}'.format(uuid, kwargs))
            logger.info(
                '{} - Started executing {} at {}'.format(
                    uuid, operation, execution_start
                )
            )
            logger.info('{} - Streaming results from DB.'.format(uuid))

            operation_callable = getattr(connection[collection], operation)
            result = operation_callable(*args, **kwargs)

            # returns the generator object
            for row in result:
                yield row

            execution_end = datetime.now()
            logger.info(
                '{} - Executed in {} second(s)'.format(
                    uuid, (execution_end - execution_start).seconds
                )
            )
            logger.info(
                '{} - Ended {} execution at {}'.format(uuid, operation, execution_end)
            )

    def _no_stream(self, uuid, collection, operation, *args, **kwargs):
        with get_connection(self, uuid) as connection:
            execution_start = datetime.now()
            logger.info('{} - Using collection {}'.format(uuid, collection))
            logger.info('{} - args: {}'.format(uuid, args))
            logger.info('{} - kwargs: {}'.format(uuid, kwargs))
            logger.info(
                '{} - Started executing {} at {}'.format(
                    uuid, operation, execution_start
                )
            )
            logger.info('{} - Not streaming results from DB.'.format(uuid))

            operation_callable = getattr(connection[collection], operation)
            result = operation_callable(*args, **kwargs)

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
