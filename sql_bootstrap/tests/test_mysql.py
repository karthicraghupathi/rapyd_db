import getpass
import logging
import os
import unittest

from sql_bootstrap.backends.mysql import MySQL


logging.basicConfig(level='WARNING')
logger = logging.getLogger(__name__)


class TestMySQLBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get('MYSQL_HOST') or 'localhost'
        self._port = os.environ.get('MYSQL_PORT') or 3306
        self._user = os.environ.get('MYSQL_USER') or getpass.getuser()
        self._password = os.environ.get('MYSQL_PASSWORD')
        self._test_db = os.environ.get('MYSQL_TEST_DB') or 'test_db'

        logger.info(
            'Using the following connection params for MySQL backend testing...'
        )
        logger.info('Host: {}'.format(self._host))

        self._db = MySQL(
            host=self._host,
            user=self._user,
            password=self._password,
            # database=self._test_db,
            port=self._port,
        )

    def tearDown(self):
        pass

    def test_mysql_db_connection(self):
        rows_affected, last_row_id, rows = self._db.execute(
            'SELECT version()', stream=False
        )
        self.assertEqual(rows_affected, 1)

    def test_mysql_create_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            'CREATE DATABASE %s', params=(self._test_db,), stream=False
        )
        print(rows_affected, last_row_id, rows)
        self.assertEqual(True)


if __name__ == '__main__':
    unittest.main()
