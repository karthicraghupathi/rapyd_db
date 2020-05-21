import csv
import getpass
import itertools
import logging
import os
import unittest

from rapyd_db.backends.mssql import MSSQL

logging.basicConfig(level="WARNING")


class TestMSSQLBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get("MSSQL_HOST") or "localhost"
        self._port = os.environ.get("MSSQL_PORT") or 1433
        self._user = os.environ.get("MSSQL_USER") or getpass.getuser()
        self._password = os.environ.get("MSSQL_PASSWORD")
        self._test_db = os.environ.get("MSSQL_TEST_DB") or "test_db"

        # define some csv file reading limits
        self._csv_start = 0
        self._csv_stop = 1
        self._csv_step = 1

        self._db = MSSQL(
            host=self._host, user=self._user, password=self._password, port=self._port,
        )

    def test_00_mssql_db_connection(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "SELECT @@VERSION AS 'version'", stream=False
        )
        self.assertIn("version", rows[0])

    def test_01_mssql_create_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "CREATE DATABASE [{}]".format(self._test_db), stream=False
        )

    def test_02_mssql_create_salaries_table(self):
        query = (
            "CREATE TABLE [{}].[dbo].[salaries] ("
            " [emp_no] int NOT NULL,"
            " [salary] int NOT NULL,"
            " [from_date] date NOT NULL,"
            " [to_date] date NOT NULL,"
            " PRIMARY KEY ([emp_no],[from_date]))".format(self._test_db)
        )
        self._db.execute(query, stream=False)

    def test_03_mssql_insert_salaries(self):
        query = (
            "INSERT INTO [{}].[dbo].[salaries] ([emp_no], [salary], [from_date], [to_date])"
            " VALUES (%s, %s, %s, %s)".format(self._test_db)
        )
        base_dir = os.path.dirname(os.path.abspath(__file__))
        count = 0
        with open(os.path.join(base_dir, "test_data_salaries.csv")) as data_file:
            reader = csv.DictReader(
                itertools.islice(
                    data_file, self._csv_start, self._csv_stop + 1, self._csv_step
                )
            )
            for row in reader:
                self._db.execute(query, tuple(row.values()))
                count += 1
        self.assertEqual(self._csv_stop, count)

    def test_04_mssql_stream_salaries(self):
        query = "SELECT * FROM [{}].[dbo].[salaries]".format(self._test_db)
        rows = self._db.execute(query, stream=True)
        count = 0
        for row in rows:
            count += 1
        self.assertEqual(self._csv_stop, count)

    def test_99_mssql_delete_test_db(self):
        rows_affected, last_row_id, rows = self._db.execute(
            "DROP DATABASE [{}]".format(self._test_db)
        )


if __name__ == "__main__":
    unittest.main()
