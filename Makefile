.PHONY: reset install-dev install-pre-commit-hook tests typecheck lint build

reset:
	rm -rf .venv

install-pre-commit-hook:
	.venv/bin/pre-commit install --install-hooks

install-dev:
	devenv sync
	uv sync --group dev
	$(MAKE) install-pre-commit-hook

install-ci:
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv sync --group dev

tests:
	pytest -vv tests

typecheck:
	mypy --strict sentry_kafka_management
	mypy --strict tests

lint:
	black --config=pyproject.toml sentry_kafka_management
	flake8 sentry_kafka_management
	isort sentry_kafka_management

build:
	uv build --wheel
