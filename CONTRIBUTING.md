# Contributing

Thanks for considering a contribution.

## Dev setup

```bash
git clone https://github.com/karthicraghupathi/rapyd_db.git
cd rapyd_db
curl -LsSf https://astral.sh/uv/install.sh | sh   # if you don't have uv
make install                                        # uv sync --extra dev (no compiled drivers)
pre-commit install
```

If you need a specific backend driver for integration testing:

```bash
make install-mysql      # or install-mssql / install-mongo / install-all
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
