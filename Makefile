.PHONY: reset install-dev install-prek-hook tests typecheck lint build

reset:
	rm -rf .venv

install-prek-hook:
	.venv/bin/prek install --prepare-hooks -f

install-dev:
	devenv sync
	uv sync --group dev
	$(MAKE) install-prek-hook

install-ci:
	which uv || (curl -LsSf https://astral.sh/uv/0.13.7/install.sh | sh)
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
