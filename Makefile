.PHONY: reset install-dev install-pre-commit-hook tests typecheck lint build

reset:
	rm -rf .venv

install-pre-commit-hook:
	.venv/bin/pre-commit install --install-hooks

install-dev:
	devenv sync
	uv sync --group dev
	$(MAKE) install-pre-commit-hook

# Special casing Python 3.9 since a bunch of internal pypi packages
# don't support it
install-ci-3.9:
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv sync --only-group ci_3_9 --python-preference only-system --no-cache

install-ci:
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv sync --group dev --python-preference only-system --no-cache

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
