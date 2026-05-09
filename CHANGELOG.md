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
