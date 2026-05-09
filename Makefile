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
