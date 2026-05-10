# rapyd_db Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modernize `rapyd_db`'s tooling, packaging, CI/CD, code quality, and documentation to current (2026) Python conventions while preserving the public API and reaching 100% line coverage.

**Architecture:** Tests-first: build a mocked unit-test layer against the *current* code as a safety net (Stage 0), then perform packaging, tooling, test-gap-fill, documentation, and CI/CD modernization in independent commits. Each stage is reversible and produces a working repo. CI/CD is the last technical stage before the version bump so the released artifact is what CI exercised.

**Tech Stack:** Python 3.9+, `uv` (deps + build), `ruff` (lint+format), `mypy` (types), `pytest` + `pytest-cov` + `pytest-mock` (test), `pre-commit`, GitHub Actions (CI/CD), `docker compose` (integration DB fixtures), Keep a Changelog (CHANGELOG).

---

## Project context (from audit, 2026-05-09)

| Area | State |
|---|---|
| Source | `rapyd_db/` (5 modules + `backends/` subpkg, ~440 LOC) |
| Tests | `rapyd_db/tests/test_{mysql,mssql,mongo}.py`, unittest, **require live DBs** via env vars |
| Build | `setup.py` (legacy) + `MANIFEST.in` |
| Deps | `requirements.txt` (`Cython`, `six`); `setup.py install_requires=["Cython", "six"]`; `extras_require` for mysql/mongo/mssql |
| Lint/Format | none |
| Types | none, no `py.typed` |
| CI | none |
| Docs | `README.rst`, `HISTORY.rst`, no CONTRIBUTING/SECURITY/CHANGELOG |
| Version | 0.0.9 (2023-08-01) |
| Python support claimed | 2 + 3 (classifiers); `six` is the only real Py2 artifact |

### Smells to fix as part of this sweep
- bare `except:` in `backends/__init__.py` (lines 31, 39)
- `cursor._executed` private API (mysqlclient) — already broke once
- `pymssql.set_max_connections(1)` global side effect on every connect
- `pymssql.OperationalError.message` string-match (Py2-era; modern pymssql has no `.message`)
- `Cython` in `install_requires` — only a build-time dep of `mysqlclient`
- `six` import for Py2/3 metaclass compat (Py2 EOL was 2020)
- README references `passwd=` kwarg but code uses `password=`

### Flag-not-fix decisions called out at the right stage
- Drop Python 2 classifiers and `six` (Stage 2, Task 2.2). Recommend min `python_requires = ">=3.9"`.
- Replace `cursor._executed` logging with original-query logging (Stage 2, Task 2.2).
- Remove `pymssql.set_max_connections(1)` — needs MSSQL integration test confirmation (Stage 2, Task 2.2).
- Move `rapyd_db/tests/` → `tests/` (Stage 0, Task 0.3). Tests are not public API; this is purely repo layout.

---

## Execution order

1. **Stage 0** — Safety net (tests against current code)
2. **Stage 1** — Packaging & dependency management
3. **Stage 2** — Code quality tooling
4. **Stage 3** — Test gap fill
5. **Stage 4** — Documentation
6. **Stage 5** — CI/CD
7. **Stage 6** — Final verification + version bump

---

## Final repo layout (target)

```
rapyd_db/
  __init__.py
  py.typed                  (NEW, Stage 2)
  backends/
    __init__.py
    mongo.py
    mssql.py
    mysql.py
  loggingadapter.py
  utils.py
tests/                       (MOVED + restructured, Stage 0)
  __init__.py
  conftest.py
  unit/
    __init__.py
    test_abstract_backend.py
    test_get_connection.py
    test_logging_adapter.py
    test_mongo_backend.py
    test_mssql_backend.py
    test_mysql_backend.py
    test_utils.py
  integration/
    __init__.py
    conftest.py
    test_data_salaries.csv
    test_mongo.py
    test_mssql.py
    test_mysql.py
docs/
  superpowers/plans/2026-05-09-modernization.md   (this file)
.github/
  dependabot.yml             (NEW, Stage 5)
  workflows/
    publish.yml              (NEW, Stage 5)
    release.yml              (NEW, Stage 5)
    test.yml                 (NEW, Stage 5)
.editorconfig                (NEW, Stage 2)
.gitignore                   (UPDATE, Stage 2)
.pre-commit-config.yaml      (NEW, Stage 2)
docker-compose.yml           (NEW, Stage 0)
CHANGELOG.md                 (CONVERT from HISTORY.rst, Stage 4)
CONTRIBUTING.md              (NEW, Stage 4)
LICENSE
Makefile                     (REWRITE for uv, Stage 1)
README.md                    (CONVERT from README.rst + expand, Stage 4)
SECURITY.md                  (NEW, Stage 4)
pyproject.toml               (NEW, Stage 1 — replaces setup.py + MANIFEST.in + requirements.txt)
test_environment.sh          (KEEP as documented example for ad-hoc runs)
tox.ini                      (NEW, Stage 5)
uv.lock                      (NEW, Stage 1)
```

Removed: `setup.py`, `MANIFEST.in`, `requirements.txt`, `HISTORY.rst`, `rapyd_db/tests/` (moved).

---

# Stage 0 — Safety net (tests against current code)

**Why first:** Every later stage modifies code or layout. Without coverage on today's behavior, we can't claim "no regressions". Stage 0 freezes current behavior under test before any change.

**Outcome:** mocked unit-test suite at 100% line coverage of `rapyd_db/` and a docker-compose-driven integration test runner. Source code is unchanged.

### Task 0.1 — Add dev tooling minimal venv via uv

**Files:**
- Create: `pyproject.toml` (minimal — full version comes in Stage 1)
- Create: `.python-version`

- [ ] **Step 1:** Install `uv` if not present.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv --version
```

- [ ] **Step 2:** Create a stub `pyproject.toml` so `uv sync` has something to read. The full project metadata lands in Stage 1; for now we just want a sandbox to install dev deps.

```toml
[project]
name = "rapyd_db"
version = "0.0.9"
description = "An opinionated lightweight wrapper around various DB backend drivers."
requires-python = ">=3.9"
dependencies = []

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "pytest-cov>=5",
    "pytest-mock>=3.14",
    "coverage[toml]>=7.6",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["rapyd_db"]
```

- [ ] **Step 3:** Pin Python.

```bash
echo "3.12" > .python-version
```

- [ ] **Step 4:** Bootstrap.

```bash
uv sync --extra dev
```

Expected: creates `.venv/`, installs pytest + cov + mock.

- [ ] **Step 5:** Commit.

```bash
git add pyproject.toml .python-version
git commit -m "chore: add minimal pyproject.toml + uv bootstrap for stage 0"
```

---

### Task 0.2 — Configure pytest + coverage (no tests yet)

**Files:**
- Modify: `pyproject.toml` (append `[tool.pytest.ini_options]` + `[tool.coverage.*]`)
- Create: `.gitignore` entries for cache dirs

- [ ] **Step 1:** Append to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers --strict-config"
markers = [
    "integration: requires a running DB (skipped unless RUN_INTEGRATION_TESTS=1)",
]

[tool.coverage.run]
branch = true
source = ["rapyd_db"]
omit = ["rapyd_db/tests/*", "tests/*"]

[tool.coverage.report]
exclude_also = [
    "if TYPE_CHECKING:",
    "raise NotImplementedError",
    "@(abc\\.)?abstractmethod",
]
fail_under = 100
show_missing = true
skip_covered = false
```

- [ ] **Step 2:** Append to `.gitignore`:

```
.venv/
.pytest_cache/
.coverage
htmlcov/
.ruff_cache/
.mypy_cache/
```

- [ ] **Step 3:** Verify pytest collects nothing (no tests yet).

```bash
uv run pytest --collect-only
```

Expected: "no tests ran".

- [ ] **Step 4:** Commit.

```bash
git add pyproject.toml .gitignore
git commit -m "chore: configure pytest + coverage thresholds"
```

---

### Task 0.3 — Move existing real-DB tests to `tests/integration/` and gate them

**Files:**
- Move: `rapyd_db/tests/test_mysql.py` → `tests/integration/test_mysql.py`
- Move: `rapyd_db/tests/test_mssql.py` → `tests/integration/test_mssql.py`
- Move: `rapyd_db/tests/test_mongo.py` → `tests/integration/test_mongo.py`
- Move: `rapyd_db/tests/test_data_salaries.csv` → `tests/integration/test_data_salaries.csv`
- Delete: `rapyd_db/tests/__init__.py`, `rapyd_db/tests/`
- Create: `tests/__init__.py`, `tests/integration/__init__.py`, `tests/integration/conftest.py`

- [ ] **Step 1:** Move files (preserve git history).

```bash
git mv rapyd_db/tests/test_mysql.py tests/integration/test_mysql.py
git mv rapyd_db/tests/test_mssql.py tests/integration/test_mssql.py
git mv rapyd_db/tests/test_mongo.py tests/integration/test_mongo.py
git mv rapyd_db/tests/test_data_salaries.csv tests/integration/test_data_salaries.csv
git rm rapyd_db/tests/__init__.py
rmdir rapyd_db/tests
touch tests/__init__.py tests/integration/__init__.py
```

- [ ] **Step 2:** Create `tests/integration/conftest.py` to gate all integration tests behind `RUN_INTEGRATION_TESTS=1`:

```python
import os
import pytest

def pytest_collection_modifyitems(config, items):
    if os.environ.get("RUN_INTEGRATION_TESTS") == "1":
        return
    skip_integration = pytest.mark.skip(reason="integration tests off; set RUN_INTEGRATION_TESTS=1")
    for item in items:
        item.add_marker(skip_integration)
```

- [ ] **Step 3:** Update each moved test file to fix the CSV path (file lives in same dir now — current code already uses `os.path.dirname(os.path.abspath(__file__))`, so no change needed). Confirm by reading lines that reference `test_data_salaries.csv`.

- [ ] **Step 4:** Verify collection still works and integration tests are skipped.

```bash
uv run pytest -v
```

Expected: 3 tests collected per backend file, all SKIPPED.

- [ ] **Step 5:** Commit.

```bash
git add -A
git commit -m "test: move real-DB tests to tests/integration and gate behind RUN_INTEGRATION_TESTS"
```

---

### Task 0.4 — Provide `docker-compose.yml` for local integration tests

**Files:**
- Create: `docker-compose.yml`
- Create: `tests/integration/.env.example`

- [ ] **Step 1:** Write `docker-compose.yml`:

```yaml
services:
  mysql:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: rapyd
    ports: ["3306:3306"]
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-prapyd"]
      interval: 5s
      retries: 20

  mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      ACCEPT_EULA: "Y"
      MSSQL_SA_PASSWORD: "Rapyd_Pass1!"
    ports: ["1433:1433"]

  mongo:
    image: mongo:7
    environment:
      MONGO_INITDB_ROOT_USERNAME: rapyd
      MONGO_INITDB_ROOT_PASSWORD: rapyd
    ports: ["27017:27017"]
```

- [ ] **Step 2:** Write `tests/integration/.env.example`:

```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=rapyd
export MYSQL_TEST_DB=rapyd_test

export MSSQL_HOST=127.0.0.1
export MSSQL_PORT=1433
export MSSQL_USER=sa
export MSSQL_PASSWORD=Rapyd_Pass1!
export MSSQL_TEST_DB=rapyd_test

export MONGO_HOST=127.0.0.1
export MONGO_PORT=27017
export MONGO_USERNAME=rapyd
export MONGO_PASSWORD=rapyd
export MONGO_TEST_DB=rapyd_test
export MONGO_TEST_COLLECTION=salaries

export RUN_INTEGRATION_TESTS=1
```

- [ ] **Step 3:** Smoke-run integration tests against docker once. (Manual confirmation; do not block automation on it.)

```bash
docker compose up -d
source tests/integration/.env.example
uv sync --extra dev --extra mysql --extra mongo --extra mssql
uv run pytest tests/integration -v
docker compose down
```

Expected: all currently-passing integration tests still pass.

- [ ] **Step 4:** Commit.

```bash
git add docker-compose.yml tests/integration/.env.example
git commit -m "test: add docker-compose for local integration tests"
```

---

### Task 0.5 — Add `tests/conftest.py` shared fixtures

**Files:**
- Create: `tests/conftest.py`

- [ ] **Step 1:** Write `tests/conftest.py`:

```python
import logging
import pytest


@pytest.fixture(autouse=True)
def _silence_rapyd_db_logger(caplog):
    caplog.set_level(logging.DEBUG, logger="rapyd_db")
    yield
```

- [ ] **Step 2:** Verify no collection errors.

```bash
uv run pytest --collect-only -q
```

- [ ] **Step 3:** Commit.

```bash
git add tests/conftest.py
git commit -m "test: add shared conftest with rapyd_db log capture"
```

---

### Task 0.6 — Unit test `utils.py` (100%)

**Files:**
- Create: `tests/unit/__init__.py`
- Create: `tests/unit/test_utils.py`

- [ ] **Step 1:** Write the failing tests:

```python
import re
import pytest

from rapyd_db.utils import _assign_if_not_none, _get_uuid


class _Bag:
    pass


class TestAssignIfNotNone:
    def test_assigns_to_dict_when_value_truthy(self):
        d = {}
        assert _assign_if_not_none(d, "k", "v") is True
        assert d == {"k": "v"}

    def test_assigns_to_object_when_value_truthy(self):
        b = _Bag()
        assert _assign_if_not_none(b, "k", 42) is True
        assert b.k == 42

    def test_does_nothing_when_value_is_none(self):
        d = {}
        assert _assign_if_not_none(d, "k", None) is False
        assert d == {}

    def test_does_nothing_when_value_is_falsy(self):
        # current implementation treats empty string and 0 as "skip"
        d = {}
        assert _assign_if_not_none(d, "k", "") is False
        assert _assign_if_not_none(d, "k", 0) is False
        assert d == {}


class TestGetUuid:
    def test_returns_32_char_hex(self):
        u = _get_uuid()
        assert isinstance(u, str)
        assert re.fullmatch(r"[0-9a-f]{32}", u)

    def test_uniqueness(self):
        assert _get_uuid() != _get_uuid()
```

- [ ] **Step 2:** Run.

```bash
uv run pytest tests/unit/test_utils.py -v --no-cov
```

Expected: all PASS (this just locks in current behavior including the falsy-skip quirk).

- [ ] **Step 3:** Confirm coverage.

```bash
uv run pytest tests/unit/test_utils.py --cov=rapyd_db.utils --cov-report=term-missing --no-cov-on-fail
```

Expected: `rapyd_db/utils.py 100%`.

- [ ] **Step 4:** Commit.

```bash
git add tests/unit/__init__.py tests/unit/test_utils.py
git commit -m "test: add unit tests for utils (100% coverage)"
```

---

### Task 0.7 — Unit test `loggingadapter.py` (100%)

**Files:**
- Create: `tests/unit/test_logging_adapter.py`

- [ ] **Step 1:** Write tests:

```python
import logging

from rapyd_db.loggingadapter import LogIdAdapter


def _adapter(extra):
    base = logging.getLogger("rapyd_db.test_adapter")
    return LogIdAdapter(base, extra)


def test_prepends_log_id_when_present():
    adapter = _adapter({"log_id": "abc123"})
    msg, kwargs = adapter.process("hello", {})
    assert msg == "abc123 - hello"
    assert kwargs == {}


def test_returns_msg_unchanged_when_log_id_missing():
    adapter = _adapter({})
    msg, kwargs = adapter.process("hello", {"foo": 1})
    assert msg == "hello"
    assert kwargs == {"foo": 1}


def test_returns_msg_unchanged_when_log_id_falsy():
    adapter = _adapter({"log_id": None})
    msg, kwargs = adapter.process("hello", {})
    assert msg == "hello"
```

- [ ] **Step 2:** Run + confirm 100%.

```bash
uv run pytest tests/unit/test_logging_adapter.py --cov=rapyd_db.loggingadapter --cov-report=term-missing --no-cov-on-fail
```

- [ ] **Step 3:** Commit.

```bash
git add tests/unit/test_logging_adapter.py
git commit -m "test: add unit tests for LogIdAdapter (100% coverage)"
```

---

### Task 0.8 — Unit test `backends.AbstractBackend` + `get_connection` (100%)

**Files:**
- Create: `tests/unit/test_abstract_backend.py`
- Create: `tests/unit/test_get_connection.py`

- [ ] **Step 1:** `tests/unit/test_abstract_backend.py`:

```python
import pytest

from rapyd_db.backends import AbstractBackend


def test_cannot_instantiate_abstract_backend_directly():
    with pytest.raises(TypeError):
        AbstractBackend()


def test_subclass_must_implement_connect():
    class Half(AbstractBackend):
        pass
    with pytest.raises(TypeError):
        Half()


def test_concrete_subclass_instantiates():
    class Concrete(AbstractBackend):
        def _connect(self):
            return object()
    assert Concrete()._connect() is not None


def test_default_execute_returns_none():
    class Concrete(AbstractBackend):
        def _connect(self):
            return None
    # current AbstractBackend.execute is a placeholder that returns None
    assert Concrete().execute() is None
```

- [ ] **Step 2:** `tests/unit/test_get_connection.py`:

```python
from unittest.mock import MagicMock

import pytest

from rapyd_db.backends import AbstractBackend, get_connection


class _Backend(AbstractBackend):
    def __init__(self, conn):
        self._conn = conn

    def _connect(self):
        return self._conn


def test_yields_connection_and_closes():
    conn = MagicMock()
    with get_connection(_Backend(conn)) as c:
        assert c is conn
    conn.close.assert_called_once()


def test_log_id_is_logged(caplog):
    conn = MagicMock()
    with get_connection(_Backend(conn), log_id="trace-1"):
        pass
    assert any("trace-1" in r.message for r in caplog.records)


def test_connect_failure_is_logged_and_raised(caplog):
    class Boom(AbstractBackend):
        def _connect(self):
            raise RuntimeError("nope")
    with pytest.raises(RuntimeError, match="nope"):
        with get_connection(Boom()):
            pass
    assert any("Cannot connect to DB" in r.message for r in caplog.records)


def test_close_swallows_exception():
    conn = MagicMock()
    conn.close.side_effect = RuntimeError("close-failed")
    # the bare except in the finally must not propagate
    with get_connection(_Backend(conn)):
        pass
    conn.close.assert_called_once()
```

- [ ] **Step 3:** Run + verify 100%.

```bash
uv run pytest tests/unit/test_abstract_backend.py tests/unit/test_get_connection.py \
    --cov=rapyd_db.backends --cov=rapyd_db.backends.__init__ \
    --cov-report=term-missing --no-cov-on-fail
```

Expected: `rapyd_db/backends/__init__.py 100%`.

- [ ] **Step 4:** Commit.

```bash
git add tests/unit/test_abstract_backend.py tests/unit/test_get_connection.py
git commit -m "test: add unit tests for AbstractBackend + get_connection (100% coverage)"
```

---

### Task 0.9 — Unit test `backends/mysql.py` (100%)

**Files:**
- Create: `tests/unit/test_mysql_backend.py`

- [ ] **Step 1:** Strategy. We patch `MySQLdb.connect` to return a `MagicMock` connection whose `.cursor()` returns a configurable cursor. We assert: `_connection_params` constructed correctly, `cursorclass` stripped, `_no_stream` returns the `(rows_affected, lastrowid, fetchall())` triple, `_stream` yields rows from `SSDictCursor`.

```python
from unittest.mock import MagicMock, patch

import pytest

from rapyd_db.backends.mysql import MySQL


@pytest.fixture
def mock_mysqldb():
    with patch("rapyd_db.backends.mysql.MySQLdb") as m:
        yield m


def _set_cursor(mock_mysqldb, *, executed=b"SELECT 1", rowcount=1, lastrowid=42, rows=None, stream_rows=None):
    cursor = MagicMock()
    cursor._executed = executed
    cursor.execute.return_value = rowcount
    cursor.lastrowid = lastrowid
    cursor.fetchall.return_value = rows or [{"x": 1}]
    cursor.__iter__ = lambda self: iter(stream_rows or rows or [{"x": 1}])
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_mysqldb.connect.return_value = conn
    return conn, cursor


class TestMySQLInit:
    def test_strips_cursorclass(self, mock_mysqldb):
        db = MySQL(host="h", user="u", password="p", cursorclass="ignored", port=3306)
        assert "cursorclass" not in db._connection_params
        assert db._connection_params["host"] == "h"
        assert db._connection_params["port"] == 3306

    def test_omits_none_values(self, mock_mysqldb):
        db = MySQL(host=None, user="u", password=None)
        assert "host" not in db._connection_params
        assert "password" not in db._connection_params
        assert db._connection_params["user"] == "u"


class TestMySQLNoStream:
    def test_returns_triple(self, mock_mysqldb):
        _, cursor = _set_cursor(mock_mysqldb, rowcount=3, lastrowid=99, rows=[{"a": 1}])
        db = MySQL(host="h", user="u", password="p")
        rows_affected, last_id, results = db.execute("SELECT * FROM t")
        assert rows_affected == 3
        assert last_id == 99
        assert results == [{"a": 1}]
        cursor.execute.assert_called_once_with("SELECT * FROM t")

    def test_passes_params(self, mock_mysqldb):
        _, cursor = _set_cursor(mock_mysqldb)
        db = MySQL(host="h", user="u", password="p")
        db.execute("INSERT INTO t VALUES (%s)", ("a",))
        cursor.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", ("a",))

    def test_logs_rendered_query(self, mock_mysqldb, caplog):
        _set_cursor(mock_mysqldb, executed=b"SELECT now()")
        db = MySQL(host="h", user="u", password="p")
        db.execute("SELECT now()")
        assert any("SELECT now()" in r.message for r in caplog.records)


class TestMySQLStream:
    def test_yields_rows(self, mock_mysqldb):
        rows = [{"i": 1}, {"i": 2}, {"i": 3}]
        _, cursor = _set_cursor(mock_mysqldb, stream_rows=rows)
        db = MySQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t", stream=True))
        assert out == rows
```

- [ ] **Step 2:** Run.

```bash
uv run pytest tests/unit/test_mysql_backend.py --cov=rapyd_db.backends.mysql --cov-report=term-missing --no-cov-on-fail
```

If any line is uncovered (e.g., a branch in `_stream` for `params is None` vs given), add a test for it. Iterate until 100%.

- [ ] **Step 3:** Commit.

```bash
git add tests/unit/test_mysql_backend.py
git commit -m "test: add unit tests for MySQL backend (100% coverage)"
```

---

### Task 0.10 — Unit test `backends/mssql.py` (100%)

**Files:**
- Create: `tests/unit/test_mssql_backend.py`

- [ ] **Step 1:** Tests must cover: `_connection_params` build, `as_dict=True` forced, `set_max_connections(1)` call, both `_no_stream` and `_stream` paths, the `OperationalError` swallow path with the *exact* matched message, and the re-raise path with a different message.

```python
from unittest.mock import MagicMock, patch

import pytest

from rapyd_db.backends.mssql import MSSQL


@pytest.fixture
def mock_pymssql():
    with patch("rapyd_db.backends.mssql.pymssql") as m:
        # construct an OperationalError class on the mock that behaves like the real one
        class FakeOpError(Exception):
            def __init__(self, message):
                super().__init__(message)
                self.message = message
        m.OperationalError = FakeOpError
        yield m


def _set_cursor(mock_pymssql, *, rowcount=1, rows=None, fetch_exc=None):
    cursor = MagicMock()
    cursor.rowcount = rowcount
    if fetch_exc is not None:
        cursor.fetchall.side_effect = fetch_exc
    else:
        cursor.fetchall.return_value = rows or [{"x": 1}]
    cursor.__iter__ = lambda self: iter(rows or [{"x": 1}])
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_pymssql.connect.return_value = conn
    return conn, cursor


class TestMSSQLInit:
    def test_forces_as_dict(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p", as_dict=False)
        assert db._connection_params["as_dict"] is True


class TestMSSQLConnect:
    def test_sets_max_connections(self, mock_pymssql):
        db = MSSQL(host="h", user="u", password="p")
        db._connect()
        mock_pymssql.set_max_connections.assert_called_once_with(1)


class TestMSSQLNoStream:
    def test_returns_triple_with_results(self, mock_pymssql):
        _, cursor = _set_cursor(mock_pymssql, rowcount=2, rows=[{"a": 1}, {"a": 2}])
        db = MSSQL(host="h", user="u", password="p")
        affected, last_id, rows = db.execute("SELECT * FROM t")
        assert affected == 2
        assert rows == [{"a": 1}, {"a": 2}]

    def test_swallows_no_resultset_operationalerror(self, mock_pymssql):
        msg = "Statement not executed or executed statement has no resultset"
        _set_cursor(mock_pymssql, fetch_exc=mock_pymssql.OperationalError(msg))
        db = MSSQL(host="h", user="u", password="p")
        affected, _, rows = db.execute("CREATE TABLE t (a int)")
        assert rows == []

    def test_reraises_other_operationalerror(self, mock_pymssql):
        _set_cursor(mock_pymssql, fetch_exc=mock_pymssql.OperationalError("something else"))
        db = MSSQL(host="h", user="u", password="p")
        with pytest.raises(mock_pymssql.OperationalError):
            db.execute("SELECT * FROM t")


class TestMSSQLStream:
    def test_yields_rows(self, mock_pymssql):
        _, _cursor = _set_cursor(mock_pymssql, rows=[{"i": i} for i in range(3)])
        db = MSSQL(host="h", user="u", password="p")
        out = list(db.execute("SELECT * FROM t", stream=True))
        assert out == [{"i": 0}, {"i": 1}, {"i": 2}]
```

- [ ] **Step 2:** Run + iterate until 100%.

- [ ] **Step 3:** Commit.

```bash
git add tests/unit/test_mssql_backend.py
git commit -m "test: add unit tests for MSSQL backend (100% coverage)"
```

---

### Task 0.11 — Unit test `backends/mongo.py` (100%)

**Files:**
- Create: `tests/unit/test_mongo_backend.py`

- [ ] **Step 1:** Tests must cover: param defaults (`auth_source`, `connect_timeout_ms`, `maxPoolSize=1`, `connect=False`), special operations bypass database/collection requirement (`server_info`), missing `database`/`collection` raises `KeyError`, dispatch via `getattr(connection[db][col], operation)(*args, **kwargs)`, `_stream` materializes (per pymongo 4 fix).

```python
from unittest.mock import MagicMock, patch

import pytest

from rapyd_db.backends.mongo import Mongo


@pytest.fixture
def mock_client():
    with patch("rapyd_db.backends.mongo.MongoClient") as m:
        yield m


def _wire_client(mock_client):
    coll = MagicMock()
    db = MagicMock()
    db.__getitem__.return_value = coll
    client = MagicMock()
    client.__getitem__.return_value = db
    mock_client.return_value = client
    return client, db, coll


class TestMongoInit:
    def test_forces_pool_and_connect_flags(self, mock_client):
        db = Mongo(host="h", username="u", password="p")
        assert db._connection_params["maxPoolSize"] == 1
        assert db._connection_params["connect"] is False
        assert db._connection_params["authSource"] == "admin"
        assert db._connection_params["connectTimeoutMS"] == 2000


class TestMongoExecuteRequiresDbAndCollection:
    def test_missing_database_raises(self, mock_client):
        _wire_client(mock_client)
        db = Mongo(host="h", username="u", password="p")
        with pytest.raises(KeyError):
            db.execute("insert_many", documents=[{"a": 1}], collection="c")

    def test_missing_collection_raises(self, mock_client):
        _wire_client(mock_client)
        db = Mongo(host="h", username="u", password="p")
        with pytest.raises(KeyError):
            db.execute("insert_many", documents=[{"a": 1}], database="d")


class TestMongoExecuteOperationDispatch:
    def test_dispatches_to_collection_method(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        coll.insert_many.return_value = MagicMock(inserted_ids=[1, 2])
        db = Mongo(host="h", username="u", password="p")
        result = db.execute(
            "insert_many",
            [{"a": 1}, {"a": 2}],
            database="d",
            collection="c",
        )
        coll.insert_many.assert_called_once_with([{"a": 1}, {"a": 2}])
        assert result.inserted_ids == [1, 2]

    def test_server_info_bypasses_db_requirement(self, mock_client):
        client, _, _ = _wire_client(mock_client)
        client.server_info.return_value = {"version": "7.0"}
        db = Mongo(host="h", username="u", password="p")
        result = db.execute("server_info")
        assert result == {"version": "7.0"}


class TestMongoStream:
    def test_returns_list_not_generator(self, mock_client):
        _, _, coll = _wire_client(mock_client)
        coll.find.return_value = iter([{"i": 0}, {"i": 1}])
        db = Mongo(host="h", username="u", password="p")
        out = db.execute("find", {}, database="d", collection="c", stream=True)
        # pymongo 4 fix: _stream materializes
        assert list(out) == [{"i": 0}, {"i": 1}]
```

> Note: the *exact* set of operations that bypass the db/collection requirement (`server_info`, possibly others) must be cross-checked against current `mongo.py` source while writing the tests. If the source uses a hardcoded set, mirror it here. If it uses `getattr` on the `MongoClient` directly, the test stays as-is.

- [ ] **Step 2:** Run + iterate until 100%.

- [ ] **Step 3:** Commit.

```bash
git add tests/unit/test_mongo_backend.py
git commit -m "test: add unit tests for Mongo backend (100% coverage)"
```

---

### Task 0.12 — Verify 100% coverage gate

- [ ] **Step 1:** Run the full unit test suite with the coverage threshold enforced.

```bash
uv run pytest tests/unit -v
```

Expected: all tests PASS, exit code 0, coverage ≥ 100% (the `fail_under = 100` configured in 0.2 enforces this).

- [ ] **Step 2:** Run integration tests gated as before.

```bash
uv run pytest tests/integration -v
```

Expected: all SKIPPED (no `RUN_INTEGRATION_TESTS=1`).

- [ ] **Step 3:** Tag the safety net.

```bash
git tag stage-0-safety-net
```

---

# Stage 1 — Packaging & dependency management

**Outcome:** `pyproject.toml` is the single source of truth. `setup.py`, `MANIFEST.in`, `requirements.txt` are gone. `uv.lock` committed. Built wheel imports cleanly.

### Task 1.1 — Promote stub `pyproject.toml` to full project metadata

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1:** Replace contents with full metadata. **Do not** declare classifiers for Python 2.

```toml
[project]
name = "rapyd_db"
version = "0.0.9"
description = "An opinionated lightweight wrapper around various DB backend drivers."
readme = "README.md"
license = { file = "LICENSE" }
authors = [{ name = "Karthic Raghupathi", email = "karthicr@gmail.com" }]
requires-python = ">=3.9"
keywords = ["database", "mysql", "mariadb", "mssql", "mongodb", "wrapper"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Topic :: Database",
]
dependencies = []

[project.urls]
Homepage = "https://github.com/karthicraghupathi/rapyd_db"
Issues = "https://github.com/karthicraghupathi/rapyd_db/issues"
Changelog = "https://github.com/karthicraghupathi/rapyd_db/blob/master/CHANGELOG.md"

[project.optional-dependencies]
mysql = ["mysqlclient>=2.2"]
mssql = ["pymssql>=2.3"]
mongo = ["pymongo>=4.6"]
dev = [
    "pytest>=8",
    "pytest-cov>=5",
    "pytest-mock>=3.14",
    "coverage[toml]>=7.6",
    "ruff>=0.6",
    "mypy>=1.11",
    "pre-commit>=3.8",
    "tox>=4.18",
    "build>=1.2",
    "twine>=5.1",
]

[build-system]
requires = ["hatchling>=1.25"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["rapyd_db"]

[tool.hatch.build.targets.sdist]
include = ["rapyd_db", "README.md", "LICENSE", "CHANGELOG.md"]
```

> The pytest/coverage/ruff/mypy `[tool.*]` blocks are appended in their respective stages (already present from Stage 0 for pytest+coverage).

- [ ] **Step 2:** Refresh lock and venv.

```bash
uv lock
uv sync --extra dev
```

- [ ] **Step 3:** Verify full unit suite still green.

```bash
uv run pytest tests/unit
```

- [ ] **Step 4:** Commit.

```bash
git add pyproject.toml uv.lock
git commit -m "build: promote pyproject.toml to full project metadata; add uv.lock"
```

---

### Task 1.2 — Build wheel + import smoke test

**Files:**
- Create: `tests/unit/test_packaging.py`

- [ ] **Step 1:** Confirm wheel builds and contains the package.

```bash
uv run python -m build --wheel
ls dist/
uv run python -c "import zipfile; zipfile.ZipFile('dist/rapyd_db-0.0.9-py3-none-any.whl').printdir()"
```

Expected: wheel includes `rapyd_db/__init__.py`, `rapyd_db/backends/__init__.py`, all 3 backend modules, `rapyd_db/loggingadapter.py`, `rapyd_db/utils.py`. **Should not** include `tests/`.

- [ ] **Step 2:** Add a smoke test that imports every public module.

```python
import importlib

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "rapyd_db",
        "rapyd_db.backends",
        "rapyd_db.loggingadapter",
        "rapyd_db.utils",
    ],
)
def test_module_imports(module):
    importlib.import_module(module)


@pytest.mark.parametrize(
    ("module", "extra"),
    [
        ("rapyd_db.backends.mysql", "mysql"),
        ("rapyd_db.backends.mssql", "mssql"),
        ("rapyd_db.backends.mongo", "mongo"),
    ],
)
def test_optional_backend_modules_import_when_extras_installed(module, extra):
    try:
        importlib.import_module(module)
    except ImportError:
        pytest.skip(f"optional extra '{extra}' not installed")
```

- [ ] **Step 3:** Run with all extras installed.

```bash
uv sync --extra dev --extra mysql --extra mssql --extra mongo
uv run pytest tests/unit/test_packaging.py -v
```

- [ ] **Step 4:** Commit.

```bash
git add tests/unit/test_packaging.py
git commit -m "test: add wheel + import smoke tests"
```

---

### Task 1.3 — Remove legacy build files

**Files:**
- Delete: `setup.py`
- Delete: `MANIFEST.in`
- Delete: `requirements.txt`
- Modify: `Makefile`

- [ ] **Step 1:** Remove the legacy files.

```bash
git rm setup.py MANIFEST.in requirements.txt
```

- [ ] **Step 2:** Rewrite `Makefile` to use `uv` (kept thin — main interface is `uv` directly):

```makefile
.PHONY: install lint format type test test-unit test-integration cov build clean

install:
	uv sync --extra dev --extra mysql --extra mssql --extra mongo

lint:
	uv run ruff check .

format:
	uv run ruff format .

type:
	uv run mypy rapyd_db

test-unit:
	uv run pytest tests/unit

test-integration:
	RUN_INTEGRATION_TESTS=1 uv run pytest tests/integration

test: test-unit

cov:
	uv run pytest tests/unit --cov-report=term-missing --cov-report=html

build:
	uv run python -m build

clean:
	rm -rf build dist *.egg-info .coverage htmlcov .pytest_cache .mypy_cache .ruff_cache
```

- [ ] **Step 3:** Re-build to confirm nothing depended on the removed files.

```bash
rm -rf dist build
uv run python -m build
uv run pip install --force-reinstall dist/*.whl
uv run python -c "import rapyd_db; from rapyd_db.backends import AbstractBackend; print('ok')"
```

- [ ] **Step 4:** Commit.

```bash
git add -A
git commit -m "build: remove setup.py/MANIFEST.in/requirements.txt; rewrite Makefile for uv"
```

---

### Task 1.4 — Auto-generate `requirements-dev.txt` for non-uv consumers

**Files:**
- Create: `requirements-dev.txt`

- [ ] **Step 1:** Generate from the lockfile.

```bash
uv export --format requirements-txt --extra dev --no-hashes -o requirements-dev.txt
```

- [ ] **Step 2:** Commit.

```bash
git add requirements-dev.txt
git commit -m "build: regenerate requirements-dev.txt from uv lockfile"
```

> **Note for the executor:** keep this file regenerated whenever `uv.lock` changes. Consider a pre-commit hook (Stage 2.7) to enforce it.

---

# Stage 2 — Code quality tooling

### Task 2.1 — Ruff lint + format baseline

**Files:**
- Modify: `pyproject.toml` (append `[tool.ruff]` blocks)

- [ ] **Step 1:** Append:

```toml
[tool.ruff]
line-length = 100
target-version = "py39"
extend-exclude = ["build", "dist"]

[tool.ruff.lint]
select = [
    "E", "F", "W",      # pycodestyle/pyflakes
    "I",                # isort
    "B",                # bugbear
    "UP",               # pyupgrade
    "SIM",              # simplify
    "RUF",              # ruff-specific
    "PT",               # pytest-style
    "C4",               # comprehensions
    "TID",              # tidy-imports
]
ignore = [
    "E501",  # long lines — formatter handles
]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["B011"]   # allow assert False

[tool.ruff.format]
quote-style = "double"
```

- [ ] **Step 2:** Run lint to see baseline.

```bash
uv run ruff check . --statistics
```

- [ ] **Step 3:** Auto-fix safe issues.

```bash
uv run ruff check . --fix
uv run ruff format .
```

- [ ] **Step 4:** Run tests to confirm nothing broke.

```bash
uv run pytest tests/unit
```

- [ ] **Step 5:** Commit.

```bash
git add -A
git commit -m "style: introduce ruff (lint + format) baseline"
```

---

### Task 2.2 — Resolve Stage 0-flagged smells (drop six, fix bare except, replace `_executed`, kill global `set_max_connections`)

This is the most behavior-sensitive task. Each sub-step is its own commit so we can bisect.

**Files:** `rapyd_db/backends/__init__.py`, `rapyd_db/backends/mysql.py`, `rapyd_db/backends/mssql.py`

- [ ] **Step 1: Drop `six`.** In `backends/__init__.py`, replace:

```python
import six
...
@six.add_metaclass(abc.ABCMeta)
class AbstractBackend:
```

with:

```python
class AbstractBackend(metaclass=abc.ABCMeta):
```

Run `uv run pytest tests/unit`. Expected: green.

```bash
git add rapyd_db/backends/__init__.py
git commit -m "refactor: drop six dependency, use native metaclass syntax"
```

- [ ] **Step 2: Replace bare `except:` with `except Exception:`.** Same file. Open `rapyd_db/backends/__init__.py` and change both bare `except:` blocks (one around the connect, one around the close) to `except Exception:`. Run unit tests; the close-swallows-exception test (`tests/unit/test_get_connection.py`) keeps it honest.

```bash
git add rapyd_db/backends/__init__.py
git commit -m "refactor: replace bare except with except Exception in get_connection"
```

- [ ] **Step 3: Stop using `cursor._executed`.** In `rapyd_db/backends/mysql.py`, replace the line that logs `cursor._executed.decode("utf8")` with a log of the original `query` and `params`:

```python
adapter.info("Query: %s", query)
if params is not None:
    adapter.info("Params: %s", (params,))
```

Update `tests/unit/test_mysql_backend.py::TestMySQLNoStream::test_logs_rendered_query` accordingly: assert the original SQL appears in `caplog`, drop the rendered-string check.

```bash
git add rapyd_db/backends/mysql.py tests/unit/test_mysql_backend.py
git commit -m "refactor: log original query+params instead of mysqlclient private _executed"
```

- [ ] **Step 4: Remove `pymssql.set_max_connections(1)`.** In `rapyd_db/backends/mssql.py`, delete the line. Update `tests/unit/test_mssql_backend.py::TestMSSQLConnect::test_sets_max_connections` to `test_does_not_set_max_connections` and assert it is **not** called.

```bash
git add rapyd_db/backends/mssql.py tests/unit/test_mssql_backend.py
git commit -m "refactor: remove pymssql.set_max_connections global side effect"
```

- [ ] **Step 5: Rewrite the OperationalError swallow** to use `args[0]` instead of `e.message`:

```python
except pymssql.OperationalError as e:
    expected_msg = "Statement not executed or executed statement has no resultset"
    if e.args and e.args[0] == expected_msg:
        result = []
    else:
        raise
```

Update the corresponding unit tests' `FakeOpError` to populate `args` instead of `message`.

```bash
git add rapyd_db/backends/mssql.py tests/unit/test_mssql_backend.py
git commit -m "fix: read pymssql OperationalError via args[0] instead of removed .message"
```

- [ ] **Step 6: Confirm 100% coverage still holds.**

```bash
uv run pytest tests/unit
```

Expected: green, fail_under=100 satisfied.

---

### Task 2.3 — Type hints on small modules first (`utils`, `loggingadapter`)

**Files:** `rapyd_db/utils.py`, `rapyd_db/loggingadapter.py`

- [ ] **Step 1:** Annotate `rapyd_db/utils.py`:

```python
from __future__ import annotations

import uuid
from typing import Any


def _assign_if_not_none(obj: Any, param: str, value: Any) -> bool:
    if value:
        if isinstance(obj, dict):
            obj[param] = value
        else:
            setattr(obj, param, value)
        return True
    return False


def _get_uuid() -> str:
    return uuid.uuid4().hex
```

- [ ] **Step 2:** Annotate `rapyd_db/loggingadapter.py`:

```python
from __future__ import annotations

import logging
from typing import Any, MutableMapping


class LogIdAdapter(logging.LoggerAdapter):
    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        log_id = self.extra.get("log_id") if self.extra else None
        if log_id:
            return f"{log_id} - {msg}", kwargs
        return msg, kwargs
```

- [ ] **Step 3:** Tests + commit.

```bash
uv run pytest tests/unit
git add rapyd_db/utils.py rapyd_db/loggingadapter.py
git commit -m "refactor: add type hints to utils and loggingadapter"
```

---

### Task 2.4 — Type hints on `backends/__init__.py`

**Files:** `rapyd_db/backends/__init__.py`

- [ ] **Step 1:** Annotate:

```python
from __future__ import annotations

import abc
import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from ..loggingadapter import LogIdAdapter

_logger = logging.getLogger(__name__)


class AbstractBackend(metaclass=abc.ABCMeta):
    _connection_params: dict[str, Any] | None = None

    @abc.abstractmethod
    def _connect(self) -> Any:
        """Connect to the backend and return a driver connection."""

    def execute(self, stream: bool = False, *args: Any, **kwargs: Any) -> Any:
        """Execute the query and return the result."""


@contextmanager
def get_connection(
    backend: AbstractBackend,
    log_id: Optional[str] = None,
) -> Iterator[Any]:
    adapter = LogIdAdapter(_logger, {"log_id": log_id})
    try:
        adapter.info("Connecting to DB")
        connection = backend._connect()
    except Exception:
        adapter.exception("Cannot connect to DB")
        raise
    try:
        yield connection
    finally:
        try:
            adapter.info("Closed connection to DB")
            connection.close()
        except Exception:
            pass
```

- [ ] **Step 2:** Tests + commit.

```bash
uv run pytest tests/unit
git add rapyd_db/backends/__init__.py
git commit -m "refactor: add type hints to AbstractBackend and get_connection"
```

---

### Task 2.5 — Type hints on each backend

**Files:** `rapyd_db/backends/mysql.py`, `rapyd_db/backends/mssql.py`, `rapyd_db/backends/mongo.py`

For each backend:
- Annotate `__init__` parameters as `str | None = None`.
- Annotate `_connect` return type with the driver's connection type (use `Any` if the driver lacks stubs).
- Annotate `execute(...)` return type as `Iterator[dict[str, Any]] | tuple[int, int | None, list[dict[str, Any]]]` for SQL backends; for Mongo, `Any` is acceptable given the operation-dispatch shape.
- Use `from __future__ import annotations` so quoted forwards refs aren't required.

- [ ] **Step 1–3:** One commit per backend.

```bash
git add rapyd_db/backends/mysql.py
git commit -m "refactor: add type hints to MySQL backend"
git add rapyd_db/backends/mssql.py
git commit -m "refactor: add type hints to MSSQL backend"
git add rapyd_db/backends/mongo.py
git commit -m "refactor: add type hints to Mongo backend"
```

- [ ] **Step 4:** Run unit tests after each commit.

---

### Task 2.6 — Add mypy config + `py.typed`

**Files:**
- Create: `rapyd_db/py.typed` (empty file)
- Modify: `pyproject.toml`

- [ ] **Step 1:** Create marker.

```bash
touch rapyd_db/py.typed
```

- [ ] **Step 2:** Append to `pyproject.toml`:

```toml
[tool.mypy]
python_version = "3.9"
strict = false
warn_unused_configs = true
warn_redundant_casts = true
warn_unused_ignores = true
no_implicit_optional = true
check_untyped_defs = true
ignore_missing_imports = true   # MySQLdb / pymssql / pymongo lack stubs
files = ["rapyd_db"]
```

- [ ] **Step 3:** Run mypy.

```bash
uv run mypy rapyd_db
```

Expected: 0 errors. Fix any that appear.

- [ ] **Step 4:** Commit.

```bash
git add rapyd_db/py.typed pyproject.toml
git commit -m "chore: add mypy config and PEP 561 py.typed marker"
```

---

### Task 2.7 — pre-commit

**Files:**
- Create: `.pre-commit-config.yaml`

- [ ] **Step 1:** Write config:

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: check-merge-conflict
      - id: check-yaml
      - id: check-toml
      - id: check-added-large-files

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.6.9
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.11.2
    hooks:
      - id: mypy
        files: ^rapyd_db/
        additional_dependencies: []
```

- [ ] **Step 2:** Pin to current versions.

```bash
uv run pre-commit autoupdate
uv run pre-commit run --all-files
```

Expected: any changes are autoformat fixes; commit them with the config.

- [ ] **Step 3:** Commit.

```bash
git add .pre-commit-config.yaml
git commit -m "chore: add pre-commit with ruff + mypy hooks"
```

---

### Task 2.8 — `.editorconfig` and `.gitignore` cleanup

**Files:**
- Create: `.editorconfig`
- Modify: `.gitignore`

- [ ] **Step 1:** `.editorconfig`:

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true
indent_style = space
indent_size = 4

[*.{yml,yaml,toml,md,json}]
indent_size = 2

[Makefile]
indent_style = tab
```

- [ ] **Step 2:** Append to `.gitignore` (idempotent — items already present from 0.2 stay):

```
.idea/
.vscode/
*.egg-info/
build/
dist/
```

Untrack any IDE folders accidentally tracked.

```bash
git rm -r --cached .idea/ .vscode/ 2>/dev/null || true
```

- [ ] **Step 3:** Commit.

```bash
git add .editorconfig .gitignore
git commit -m "chore: add .editorconfig and broaden .gitignore"
```

---

# Stage 3 — Test gap fill

By design, Stage 0 already secured 100% line coverage on the unchanged surface. Stage 2 added new behavior (the `_executed` → `query+params` swap, the `args[0]` exception read) and each sub-step updated its test in lock-step. So Stage 3 is short and verifies the post-refactor state.

### Task 3.1 — Branch coverage gap audit

- [ ] **Step 1:** Run with branch coverage explicit.

```bash
uv run pytest tests/unit --cov=rapyd_db --cov-branch --cov-report=term-missing
```

- [ ] **Step 2:** For any uncovered branch reported, add a targeted test in the matching `tests/unit/test_*.py` file. Common candidates:
  - `mongo.py` operation-bypass-set fallthrough
  - `mssql.py` `params is None` vs given path in `_stream`
  - `__init__.py` `connection.close()` raising vs not raising

- [ ] **Step 3:** Commit.

```bash
git add tests/unit/
git commit -m "test: close branch-coverage gaps surfaced after refactor"
```

---

### Task 3.2 — Local integration smoke against docker

- [ ] **Step 1:** Run locally with docker compose; confirm passing. CI confirmation comes in Stage 5.

```bash
docker compose up -d
source tests/integration/.env.example
uv sync --extra dev --extra mysql --extra mssql --extra mongo
uv run pytest tests/integration -v
docker compose down
```

Expected: all integration tests pass against fresh containers. (No commit; this is a verification gate.)

---

# Stage 4 — Documentation

### Task 4.1 — Convert `HISTORY.rst` → `CHANGELOG.md` (Keep a Changelog)

**Files:**
- Delete: `HISTORY.rst`
- Create: `CHANGELOG.md`

- [ ] **Step 1:** Write `CHANGELOG.md`:

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `pyproject.toml` as the single source of truth for build, deps, and tool config.
- Mocked unit-test layer with 100% line + branch coverage of the package.
- `tests/integration/` runs against real DB instances (docker-compose fixtures, GitHub Actions service containers); gated behind `RUN_INTEGRATION_TESTS=1`.
- Type hints on the public API + PEP 561 `py.typed` marker.
- `ruff` (lint + format), `mypy`, `pre-commit`, `tox`, GitHub Actions CI/CD (test, lint, packaging, publish, release), Dependabot.
- `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`.

### Changed
- Replaced `cursor._executed` (private mysqlclient API) with logging of the original query and params.
- `MSSQL` backend reads `OperationalError.args[0]` instead of the removed `.message` attribute.
- README converted to Markdown and expanded with badges, install matrix, supported versions.

### Removed
- `setup.py`, `MANIFEST.in`, `requirements.txt` (superseded by `pyproject.toml`).
- `six` dependency.
- `pymssql.set_max_connections(1)` global side effect.
- Python 2 classifiers (Py2 was already not supported in practice).

## [0.0.9] - 2023-08-01

### Changed
- Updated for pymongo 4+ behavior (`Mongo` backend `_stream` returns a list to avoid `Cannot use MongoClient after close`).
- Switched from `cursor._last_executed` to `cursor._executed` after mysqlclient 1.3.14 dropped the former.
- MSSQL backend uses `executemany()` for bulk inserts to mirror MariaDB.

## [0.0.8] - 2021-01-15

### Added
- Mongo backend connects on first operation.
- LICENSE shipped in the package distribution.

### Fixed
- pip install failing due to missing LICENSE file.
```

- [ ] **Step 2:**

```bash
git rm HISTORY.rst
git add CHANGELOG.md
git commit -m "docs: convert HISTORY.rst to CHANGELOG.md (Keep a Changelog)"
```

---

### Task 4.2 — Convert `README.rst` → `README.md` and expand

**Files:**
- Delete: `README.rst`
- Create: `README.md`

- [ ] **Step 1:** Write:

````markdown
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
````

- [ ] **Step 2:**

```bash
git rm README.rst
git add README.md
git commit -m "docs: convert README to Markdown and expand with badges, install, quickstart"
```

---

### Task 4.3 — `CONTRIBUTING.md`

**Files:**
- Create: `CONTRIBUTING.md`

- [ ] **Step 1:**

````markdown
# Contributing

Thanks for considering a contribution.

## Dev setup

```bash
git clone https://github.com/karthicraghupathi/rapyd_db.git
cd rapyd_db
curl -LsSf https://astral.sh/uv/install.sh | sh   # if you don't have uv
make install                                        # uv sync --extra dev + all backend extras
pre-commit install
```

## Run unit tests (no DB required)

```bash
make test-unit
# or
uv run pytest tests/unit
```

Coverage threshold is enforced at 100% (line + branch). PRs that drop coverage
will fail CI.

## Run integration tests (real DBs)

Spin up the docker fixtures:

```bash
docker compose up -d
source tests/integration/.env.example
make test-integration
docker compose down
```

## Lint, format, type-check

```bash
make lint
make format
make type
```

`pre-commit` runs all of the above on every commit.

## Releasing

1. Bump version in `pyproject.toml`.
2. Move the `Unreleased` section in `CHANGELOG.md` under the new version.
3. Commit + tag: `git tag vX.Y.Z && git push --tags`.
4. The `release` workflow drafts a GitHub Release.
5. The `publish` workflow uploads to PyPI via OIDC trusted publishing.

## Coding conventions

- `ruff` for lint + format. Line length 100.
- Type hints on public APIs.
- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`, `test:`, `ci:`, `style:`, `perf:`, `build:`.
````

- [ ] **Step 2:** Commit.

```bash
git add CONTRIBUTING.md
git commit -m "docs: add CONTRIBUTING.md"
```

---

### Task 4.4 — `SECURITY.md`

**Files:**
- Create: `SECURITY.md`

- [ ] **Step 1:**

```markdown
# Security Policy

## Supported versions

Only the latest minor version receives security fixes.

| Version | Supported |
|---|---|
| 0.x latest | yes |
| older | no |

## Reporting a vulnerability

Please **do not** open a public issue. Email `karthicr@gmail.com` with:

- a description of the issue,
- steps to reproduce,
- the affected version(s),
- any suggested mitigation.

You will receive an acknowledgement within 7 days. Public disclosure will be
coordinated with you after a fix is available.
```

- [ ] **Step 2:** Commit.

```bash
git add SECURITY.md
git commit -m "docs: add SECURITY.md"
```

---

### Task 4.5 — Top-level docstring sweep

**Files:** `rapyd_db/backends/{mysql,mssql,mongo}.py`

- [ ] **Step 1:** Open each `__init__` and `execute` docstring. Fix:
  - README/code drift (`passwd=` → `password=` already correct in code; flag if a docstring still says otherwise).
  - Add `:rtype:` for return types now visible from type hints.
  - For Mongo, document the operation-dispatch shape and which operations bypass `database`/`collection`.

- [ ] **Step 2:** Commit.

```bash
git add rapyd_db/backends/
git commit -m "docs: tighten backend docstrings to match current API"
```

---

# Stage 5 — CI/CD

CI/CD is added last (before the version bump) so the workflows exercise the final, post-modernization, fully-documented codebase. The released artifact is exactly what CI ran on.

### Task 5.1 — `test.yml` workflow with service containers

**Files:**
- Create: `.github/workflows/test.yml`

- [ ] **Step 1:** Write workflow:

```yaml
name: test

on:
  push:
    branches: [master]
  pull_request:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  unit:
    name: unit (py${{ matrix.python }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python: ["3.9", "3.10", "3.11", "3.12", "3.13"]
    steps:
      - uses: actions/checkout@v6
      - uses: astral-sh/setup-uv@v8.1.0
      - uses: actions/setup-python@v6
        with:
          python-version: ${{ matrix.python }}
      - run: uv sync --extra dev
      - run: uv run pytest tests/unit -v

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: astral-sh/setup-uv@v8.1.0
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: uv sync --extra dev
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy rapyd_db

  integration:
    runs-on: ubuntu-latest
    services:
      mysql:
        image: mysql:8.4
        env:
          MYSQL_ROOT_PASSWORD: rapyd
        ports: ["3306:3306"]
        options: >-
          --health-cmd "mysqladmin ping -h localhost -prapyd"
          --health-interval 5s --health-timeout 5s --health-retries 20
      mongo:
        image: mongo:7
        env:
          MONGO_INITDB_ROOT_USERNAME: rapyd
          MONGO_INITDB_ROOT_PASSWORD: rapyd
        ports: ["27017:27017"]
      mssql:
        image: mcr.microsoft.com/mssql/server:2022-latest
        env:
          ACCEPT_EULA: "Y"
          MSSQL_SA_PASSWORD: "Rapyd_Pass1!"
        ports: ["1433:1433"]
    env:
      RUN_INTEGRATION_TESTS: "1"
      MYSQL_HOST: 127.0.0.1
      MYSQL_PORT: "3306"
      MYSQL_USER: root
      MYSQL_PASSWORD: rapyd
      MYSQL_TEST_DB: rapyd_test
      MSSQL_HOST: 127.0.0.1
      MSSQL_PORT: "1433"
      MSSQL_USER: sa
      MSSQL_PASSWORD: "Rapyd_Pass1!"
      MSSQL_TEST_DB: rapyd_test
      MONGO_HOST: 127.0.0.1
      MONGO_PORT: "27017"
      MONGO_USERNAME: rapyd
      MONGO_PASSWORD: rapyd
      MONGO_TEST_DB: rapyd_test
      MONGO_TEST_COLLECTION: salaries
    steps:
      - uses: actions/checkout@v6
      - uses: astral-sh/setup-uv@v8.1.0
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: |
          sudo apt-get update
          sudo apt-get install -y default-libmysqlclient-dev pkg-config
      - run: uv sync --extra dev --extra mysql --extra mssql --extra mongo
      - run: uv run pytest tests/integration -v

  packaging:
    runs-on: ubuntu-latest
    needs: [unit, lint]
    steps:
      - uses: actions/checkout@v6
      - uses: astral-sh/setup-uv@v8.1.0
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: uv sync --extra dev
      - run: uv run python -m build
      - run: |
          python -m venv /tmp/probe
          /tmp/probe/bin/pip install dist/*.whl
          /tmp/probe/bin/python -c "import rapyd_db; from rapyd_db.backends import AbstractBackend; print('ok')"
```

- [ ] **Step 2:** Commit.

```bash
git add .github/workflows/test.yml
git commit -m "ci: add test workflow (unit/lint/integration/packaging matrix)"
```

---

### Task 5.2 — `tox.ini` mirroring CI locally

**Files:**
- Create: `tox.ini`

- [ ] **Step 1:** Write:

```ini
[tox]
requires = tox>=4
env_list = py{39,310,311,312,313}, lint, type

[testenv]
deps =
    pytest>=8
    pytest-cov>=5
    pytest-mock>=3.14
commands = pytest tests/unit {posargs}

[testenv:lint]
skip_install = true
deps = ruff>=0.6
commands =
    ruff check .
    ruff format --check .

[testenv:type]
deps = mypy>=1.11
commands = mypy rapyd_db
```

- [ ] **Step 2:** Commit.

```bash
git add tox.ini
git commit -m "ci: add tox config mirroring GitHub Actions matrix"
```

---

### Task 5.3 — PyPI publish via OIDC trusted publishing

**Files:**
- Create: `.github/workflows/publish.yml`

> **Pre-req:** Configure trusted publisher on PyPI (https://pypi.org/manage/account/publishing/) pointing to repo `karthicraghupathi/rapyd_db`, workflow `publish.yml`, environment `pypi`. This is a manual one-time step done out of band.

- [ ] **Step 1:** Workflow:

```yaml
name: publish

on:
  push:
    tags: ["v*"]
  release:
    types: [published]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: astral-sh/setup-uv@v8.1.0
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: uv sync --extra dev
      - run: uv run python -m build
      - uses: actions/upload-artifact@v5
        with:
          name: dist
          path: dist/

  publish:
    needs: build
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v5
        with:
          name: dist
          path: dist/
      - uses: pypa/gh-action-pypi-publish@v1.10.3
```

- [ ] **Step 2:** Commit.

```bash
git add .github/workflows/publish.yml
git commit -m "ci: add PyPI publish workflow via OIDC trusted publishing"
```

---

### Task 5.4 — Release workflow

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1:**

```yaml
name: release

on:
  push:
    tags: ["v*"]

jobs:
  release:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v6
        with:
          fetch-depth: 0
      - uses: softprops/action-gh-release@v2.0.8
        with:
          generate_release_notes: true
```

- [ ] **Step 2:** Commit.

```bash
git add .github/workflows/release.yml
git commit -m "ci: auto-create GitHub release on tag push"
```

---

### Task 5.5 — Dependabot

**Files:**
- Create: `.github/dependabot.yml`

- [ ] **Step 1:**

```yaml
version: 2
updates:
  - package-ecosystem: pip
    directory: "/"
    schedule:
      interval: weekly
    groups:
      dev-deps:
        dependency-type: development

  - package-ecosystem: github-actions
    directory: "/"
    schedule:
      interval: weekly

  - package-ecosystem: pre-commit
    directory: "/"
    schedule:
      interval: weekly
```

- [ ] **Step 2:** Commit.

```bash
git add .github/dependabot.yml
git commit -m "ci: enable Dependabot for pip, github-actions, pre-commit"
```

---

# Stage 6 — Final verification + version bump

### Task 6.1 — End-to-end verification

- [ ] **Step 1:** Run all gates locally.

```bash
make lint type test-unit
docker compose up -d
source tests/integration/.env.example
make test-integration
docker compose down
make build
```

Expected: every step exits 0.

- [ ] **Step 2:** Push branch, verify CI green on PR (all jobs from Task 5.1: unit matrix, lint, integration, packaging).

---

### Task 6.2 — Version bump + tag

**Files:**
- Modify: `pyproject.toml` (`version = "0.1.0"`)
- Modify: `CHANGELOG.md` (move Unreleased → 0.1.0 with today's date)

- [ ] **Step 1:** Pick version. Recommendation: **0.1.0**. Public API is preserved (smoke test enforces import compatibility), but tooling/packaging changes are user-visible enough that a minor bump is honest.

- [ ] **Step 2:** Edit both files; commit.

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore: release 0.1.0"
git tag v0.1.0
```

- [ ] **Step 3:** Push tag (after merging the PR to `master`):

```bash
git push origin master --tags
```

The `release` and `publish` workflows take it from there.

---

# Things to flag, not fix (carried from the modernization spec)

The plan **does not** silently do any of these — each is called out at the right stage but requires explicit user approval to execute as-is, since they involve semantic decisions:

1. **Drop Python 2 classifiers** (Stage 1, Task 1.1). The current `setup.py` declares Py2 + Py3 classifiers; since `six` is the only Py2 artifact and Py2 has been EOL for 6 years, dropping the Py2 classifiers and `six` together is reasonable. Decision needed: minimum Python version (recommendation: 3.9).
2. **Drop `six`** (Stage 2, Task 2.2 Step 1). Couples to the Py2 decision.
3. **Replace `cursor._executed`** (Stage 2, Task 2.2 Step 3). User-visible log line changes from rendered SQL to original `query` + `params`. If you depend on log-grep-ing the rendered string with substituted parameters, this changes that.
4. **Remove `pymssql.set_max_connections(1)`** (Stage 2, Task 2.2 Step 4). Side-effect removed. Confirm via the integration test job that current MSSQL behavior still works under concurrency.
5. **Move `rapyd_db/tests/` → `tests/`** (Stage 0, Task 0.3). If anyone imports from `rapyd_db.tests`, this breaks them. Unlikely (no public API there) but worth noting.
6. **Dependency major bumps:** `mysqlclient>=2.2`, `pymssql>=2.3`, `pymongo>=4.6`. Each must be confirmed via the integration job. Roll back if regressions appear.

For each: write up the trade-off in the PR description and confirm before merging.

---

# Self-review

**Spec coverage check** (against `~/MODERNIZATION_PLAN.md`):

| Spec item | Plan task |
|---|---|
| Audit first | Stage 0 preamble + project-context table |
| One PR-sized commit per category | Each Task ends in `git commit`; sub-steps within Task 2.2 are independently committed for bisectability |
| Stage in order: Packaging → tooling → CI → tests → docs | Adjusted to **Tests-first** (Stage 0), then Packaging (1) → Tooling (2) → Test gap fill (3) → Docs (4) → CI/CD (5) → Version bump (6). CI/CD is intentionally last so it exercises the final artifact. Deviation from the spec's ordering is documented above. |
| Don't break public API | Stage 0 (mocked unit tests) + 1.2 (wheel import smoke test) + ongoing pytest run after every step |
| Preserve supported versions | "Things to flag, not fix" item 1 |
| No promo content | Commit messages use conventional format only |
| **Spec Stage 1** uv, pyproject.toml, [dev] extra, requirements-dev, drop setup.py, py.typed, wheel-import CI check | Plan Tasks 0.1, 1.1, 1.2, 1.3, 1.4, 2.6 (py.typed), 1.2 (wheel check) |
| **Spec Stage 1** bumpversion migration | **Not present** — original `setup.py` doesn't use bumpversion. Skipped. |
| **Spec Stage 2** ruff, types, mypy, .editorconfig, .gitignore, untrack IDE folders, pre-commit | Plan Tasks 2.1, 2.3–2.5, 2.6, 2.8, 2.7 |
| **Spec Stage 3** test workflow, lint job, tox, OIDC publish, release workflow, pinned actions, dependabot | Plan Tasks 5.1, 5.2, 5.3, 5.4, 5.5 |
| **Spec Stage 4** public API tests, edge cases, pytest, packaging smoke | Stage 0 (entire) + Plan Tasks 3.1, 3.2, 1.2 |
| **Spec Stage 5** README, CONTRIBUTING, SECURITY, CHANGELOG | Plan Tasks 4.1, 4.2, 4.3, 4.4, 4.5 |
| Version bump | Plan Task 6.2 |

**Placeholder scan:** searched for "TBD", "TODO" (in plan body, not target code), "implement later", "fill in details", "appropriate error handling", "handle edge cases", "Similar to Task". One acknowledged note in Task 0.11 Step 1 that the precise set of Mongo operations bypassing the db/collection requirement must be cross-checked against current source — that is a verification step, not a placeholder for unwritten work.

**Type consistency:** `_assign_if_not_none`, `_get_uuid`, `LogIdAdapter`, `AbstractBackend`, `get_connection`, `MySQL`, `MSSQL`, `Mongo` referenced consistently. Method names `_connect`, `execute`, `_no_stream`, `_stream` consistent across tasks. No drift.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-09-modernization.md`.**
