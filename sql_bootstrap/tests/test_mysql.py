import csv
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
            host=self._host, user=self._user, password=self._password, port=self._port,
        )

    def tearDown(self):
        # self._db.execute(
        #     'DROP DATABASE IF EXISTS `{}`'.format(self._test_db), stream=False
        # )
        pass

    def test_01_mysql_db_connection(self):
        rows_affected, last_row_id, rows = self._db.execute(
            'SELECT version()', stream=False
        )
        self.assertIn('version()', rows[0])

    def test_02_mysql_create_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            'CREATE DATABASE IF NOT EXISTS `{}`'.format(self._test_db), stream=False
        )
        self.assertEqual(rows_affected, 1)

    def test_03_mysql_create_salaries_table(self):
        query = (
            'CREATE TABLE IF NOT EXISTS `{}`.`salaries` ('
            ' `emp_no` int(11) NOT NULL,'
            ' `salary` int(11) NOT NULL,'
            ' `from_date` date NOT NULL,'
            ' `to_date` date NOT NULL,'
            ' PRIMARY KEY (`emp_no`,`from_date`))'.format(self._test_db)
        )
        rows_affected, last_row_id, rows = self._db.execute(query, stream=False)

    def test_04_mysql_insert_salaries(self):
        data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_dir, 'test_data_salaries.csv')) as data_file:
            reader = csv.reader(data_file)
            for row in reader:
                data.append(row)
        self.assertEqual(1000, len(data))
        # TODO insert into DB; explore using executemany


if __name__ == '__main__':
    unittest.main()
