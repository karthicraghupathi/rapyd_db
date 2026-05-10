# rapyd_db

[![test](https://github.com/karthicraghupathi/rapyd_db/actions/workflows/test.yml/badge.svg)](https://github.com/karthicraghupathi/rapyd_db/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/rapyd_db.svg)](https://pypi.org/project/rapyd_db/)
[![Python versions](https://img.shields.io/pypi/pyversions/rapyd_db.svg)](https://pypi.org/project/rapyd_db/)
[![License](https://img.shields.io/pypi/l/rapyd_db.svg)](LICENSE)

An opinionated lightweight wrapper around various database backend drivers.
The wrapper unifies **connection lifecycle, audit-trail logging, and streaming**
across SQL and document stores while staying out of the way of the underlying
driver's query semantics.

Supported backends:

| Backend | Driver | Extra |
|---|---|---|
| MySQL / MariaDB | [`mysqlclient`](https://pypi.org/project/mysqlclient/) | `pip install "rapyd_db[mysql]"` |
| Microsoft SQL Server | [`pymssql`](https://pypi.org/project/pymssql/) | `pip install "rapyd_db[mssql]"` |
| MongoDB | [`pymongo`](https://pypi.org/project/pymongo/) | `pip install "rapyd_db[mongo]"` |

## Install

```bash
pip install "rapyd_db[mysql,mssql,mongo]"
```

## Quickstart

```python
from rapyd_db.backends.mysql import MySQL

db = MySQL(host="localhost", user="root", password="...")

# fire-and-fetch (default): returns (rows_affected, lastrowid, results)
affected, last_id, rows = db.execute(
    "INSERT INTO users(email) VALUES (%s)",
    ("alice@example.com",),
)

# streaming for large SELECTs (server-side cursor → no MemoryError)
for row in db.execute("SELECT * FROM events", stream=True):
    process(row)
```

## Logging

Every `execute()` mints a UUID and prepends it to log lines so you can
correlate all log records belonging to a single query:

```
INFO:rapyd_db.backends:f2e47d8... - Connecting to DB
INFO:rapyd_db.backends.mysql:f2e47d8... - Starting executing query at 2026-05-09 09:00:00
INFO:rapyd_db.backends.mysql:f2e47d8... - Query: SELECT * FROM events
INFO:rapyd_db.backends.mysql:f2e47d8... - 2,844,047 row(s) affected in 10 second(s)
INFO:rapyd_db.backends:f2e47d8... - Closed connection to DB
```

The package logger ships a `NullHandler`. Configure logging in your application:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Mongo example

```python
from rapyd_db.backends.mongo import Mongo

db = Mongo(host="localhost", username="me", password="...")
db.execute(
    "insert_many",
    [{"emp_no": 1, "salary": 60_000}],
    database="hr",
    collection="salaries",
)
```

The first positional after the operation name is forwarded to the underlying
PyMongo collection method. Pass `database=` and `collection=` as keyword
arguments. Operations like `server_info` operate on the client itself and do
not require those.

## MSSQL example

```python
from rapyd_db.backends.mssql import MSSQL

db = MSSQL(host="localhost", user="sa", password="...")
affected, _, rows = db.execute("SELECT @@VERSION AS version")
print(rows[0]["version"])
```

## Supported Python versions

Python 3.9, 3.10, 3.11, 3.12, 3.13.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md) for vulnerability disclosure.

## License

Apache 2.0 — see [LICENSE](LICENSE).
