import csv
import getpass
import logging
import os
import unittest

from rapyd_db.backends import get_connection
from rapyd_db.backends.mssql import MSSQL
from rapyd_db.utils import _get_uuid

logging.basicConfig(level=os.environ.get("RAPYD_DB_LOGLEVEL") or "WARNING")


class TestMSSQLBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get("MSSQL_HOST") or "localhost"
        self._port = int(os.environ.get("MSSQL_PORT") or 1433)
        self._user = os.environ.get("MSSQL_USER") or getpass.getuser()
        self._password = os.environ.get("MSSQL_PASSWORD")
        self._test_db = os.environ.get("MSSQL_TEST_DB") or "test_db"

        self._db = MSSQL(host=self._host, user=self._user, password=self._password, port=self._port)

    def test_00_mssql_db_connection(self):
        _rows_affected, _last_row_id, rows = self._db.execute(
            "SELECT @@VERSION AS 'version'", stream=False
        )
        assert "version" in rows[0]

    def test_01_mssql_create_test_db(self):
        _rows_affected, _last_row_id, _rows = self._db.execute(
            f"CREATE DATABASE [{self._test_db}]", stream=False
        )

    def test_02_mssql_create_salaries_table(self):
        query = (
            f"CREATE TABLE [{self._test_db}].[dbo].[salaries] ("
            " [emp_no] int NOT NULL,"
            " [salary] int NOT NULL,"
            " [from_date] date NOT NULL,"
            " [to_date] date NOT NULL,"
            " PRIMARY KEY ([emp_no],[from_date]))"
        )
        self._db.execute(query, stream=False)

    def test_03_mssql_insert_salaries(self):
        data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_dir, "test_data_salaries.csv")) as data_file:
            reader = csv.DictReader(data_file)
            for row in reader:
                data.append(tuple(row.values()))
        assert len(data) == 1000
        query = (
            f"INSERT INTO [{self._test_db}].[dbo].[salaries] ([emp_no], [salary], [from_date], [to_date])"
            " VALUES (%s, %s, %s, %s)"
        )
        with get_connection(self._db, _get_uuid()) as connection:
            connection.autocommit(False)
            cursor = connection.cursor()
            cursor.executemany(query, data)
            connection.commit()

    def test_04_mssql_stream_salaries(self):
        query = f"SELECT * FROM [{self._test_db}].[dbo].[salaries]"
        rows = self._db.execute(query, stream=True)
        count = 0
        for _row in rows:
            count += 1
        assert count == 1000

    def test_99_mssql_delete_test_db(self):
        _rows_affected, _last_row_id, _rows = self._db.execute(f"DROP DATABASE [{self._test_db}]")


if __name__ == "__main__":
    unittest.main()
