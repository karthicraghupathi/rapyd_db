import csv
import getpass
import logging
import os
import unittest

from rapyd_db.backends import get_connection
from rapyd_db.backends.mysql import MySQL
from rapyd_db.utils import _get_uuid

logging.basicConfig(level=os.environ.get("RAPYD_DB_LOGLEVEL") or "WARNING")


class TestMySQLBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get("MYSQL_HOST") or "localhost"
        self._port = int(os.environ.get("MYSQL_PORT") or 3306)
        self._user = os.environ.get("MYSQL_USER") or getpass.getuser()
        self._password = os.environ.get("MYSQL_PASSWORD")
        self._test_db = os.environ.get("MYSQL_TEST_DB") or "test_db"

        self._db = MySQL(host=self._host, user=self._user, password=self._password, port=self._port)

    def test_00_mysql_db_connection(self):
        _rows_affected, _last_row_id, rows = self._db.execute("SELECT version()", stream=False)
        assert "version()" in rows[0]

    def test_01_mysql_create_test_db(self):
        rows_affected, _last_row_id, _rows = self._db.execute(
            f"CREATE DATABASE IF NOT EXISTS `{self._test_db}`", stream=False
        )
        assert rows_affected == 1

    def test_02_mysql_create_salaries_table(self):
        query = (
            f"CREATE TABLE IF NOT EXISTS `{self._test_db}`.`salaries` ("
            " `emp_no` int(11) NOT NULL,"
            " `salary` int(11) NOT NULL,"
            " `from_date` date NOT NULL,"
            " `to_date` date NOT NULL,"
            " PRIMARY KEY (`emp_no`,`from_date`))"
        )
        self._db.execute(query, stream=False)

    def test_03_mysql_insert_salaries(self):
        data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_dir, "test_data_salaries.csv")) as data_file:
            reader = csv.DictReader(data_file)
            for row in reader:
                data.append(tuple(row.values()))
        assert len(data) == 1000
        query = (
            f"INSERT INTO `{self._test_db}`.`salaries` (`emp_no`, `salary`, `from_date`, `to_date`)"
            " VALUES (%s, %s, %s, %s)"
        )
        with get_connection(self._db, _get_uuid()) as connection:
            connection.autocommit(False)
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()

    def test_04_mysql_stream_salaries(self):
        query = f"SELECT * FROM {self._test_db}.`salaries`"
        rows = self._db.execute(query, stream=True)
        count = 0
        for _row in rows:
            count += 1
        assert count == 1000

    def test_99_mysql_delete_test_db(self):
        rows_affected, _last_row_id, _rows = self._db.execute(
            f"DROP DATABASE IF EXISTS `{self._test_db}`"
        )
        assert rows_affected == 1


if __name__ == "__main__":
    unittest.main()
