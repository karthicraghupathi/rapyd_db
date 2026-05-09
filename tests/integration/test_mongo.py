import csv
import getpass
import logging
import os
import unittest

from rapyd_db.backends.mongo import Mongo

logging.basicConfig(level=os.environ.get("RAPYD_DB_LOGLEVEL") or "WARNING")


class TestMongoBackend(unittest.TestCase):
    def setUp(self):
        # read connection params from environment variables
        self._host = os.environ.get("MONGO_HOST") or "localhost"
        self._port = os.environ.get("MONGO_PORT") or 27017
        self._username = os.environ.get("MONGO_USERNAME") or getpass.getuser()
        self._password = os.environ.get("MONGO_PASSWORD")
        self._test_db = os.environ.get("MONGO_TEST_DB") or "test_db"
        self._test_collection = os.environ.get("MONGO_TEST_COLLECTION") or "test_collection"
        self._db = Mongo(
            host=self._host,
            username=self._username,
            password=self._password,
            port=self._port,
        )

    def test_00_mongo_db_connection(self):
        result = self._db.execute("server_info")
        assert "version" in result

    def test_03_mongo_insert_salaries(self):
        data = []
        base_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(base_dir, "test_data_salaries.csv")) as data_file:
            reader = csv.DictReader(data_file)
            for row in reader:
                data.append(row)
        assert len(data) == 1000
        self._db.execute(
            "insert_many",
            data,
            database=self._test_db,
            collection=self._test_collection,
        )

    def test_04_mongo_stream_salaries(self):
        rows = self._db.execute(
            "find",
            {},
            database=self._test_db,
            collection=self._test_collection,
            stream=True,
        )
        count = 0
        for _row in rows:
            count += 1
        assert count == 1000

    def test_99_mongo_delete_test_db(self):
        self._db.execute("drop_database", self._test_db)


if __name__ == "__main__":
    unittest.main()
