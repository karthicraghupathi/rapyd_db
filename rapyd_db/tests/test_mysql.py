import csv
import getpass
import logging
import os
import unittest

from rapyd_db.utils import _get_uuid
from rapyd_db.backends import get_connection
from rapyd_db.backends.mysql import MySQL


logging.basicConfig(level=os.environ.get("RAPYD_DB_LOGLEVEL") or "WARNING")


class TestMySQLBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get("MYSQL_HOST") or "localhost"
        self._port = os.environ.get("MYSQL_PORT") or 3306
        self._user = os.environ.get("MYSQL_USER") or getpass.getuser()
        self._password = os.environ.get("MYSQL_PASSWORD")
        self._test_db = os.environ.get("MYSQL_TEST_DB") or "test_db"

        self._db = MySQL(
            host=self._host, user=self._user, password=self._password, port=self._port
        )

    def test_00_mysql_db_connection(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "SELECT version()", stream=False
        )
        self.assertIn("version()", rows[0])

    def test_01_mysql_create_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "CREATE DATABASE IF NOT EXISTS `{}`".format(self._test_db), stream=False
        )
        self.assertEqual(rows_affected, 1)

    def test_02_mysql_create_salaries_table(self):
        query = (
            "CREATE TABLE IF NOT EXISTS `{}`.`salaries` ("
            " `emp_no` int(11) NOT NULL,"
            " `salary` int(11) NOT NULL,"
            " `from_date` date NOT NULL,"
            " `to_date` date NOT NULL,"
            " PRIMARY KEY (`emp_no`,`from_date`))".format(self._test_db)
        )
        self._db.execute(query, stream=False)

    def test_03_mysql_insert_salaries(self):
        data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_dir, "test_data_salaries.csv")) as data_file:
            reader = csv.DictReader(data_file)
            for row in reader:
                data.append(tuple(row.values()))
        self.assertEqual(1000, len(data))
        query = (
            "INSERT INTO `{}`.`salaries` (`emp_no`, `salary`, `from_date`, `to_date`)"
            " VALUES (%s, %s, %s, %s)".format(self._test_db)
        )
        with get_connection(self._db, _get_uuid()) as connection:
            connection.autocommit(False)
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()

    def test_04_mysql_stream_salaries(self):
        query = "SELECT * FROM {}.`salaries`".format(self._test_db)
        rows = self._db.execute(query, stream=True)
        count = 0
        for row in rows:
            count += 1
        self.assertEqual(1000, count)

    def test_99_mysql_delete_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "DROP DATABASE IF EXISTS `{}`".format(self._test_db)
        )
        self.assertEqual(rows_affected, 1)


if __name__ == "__main__":
    unittest.main()
