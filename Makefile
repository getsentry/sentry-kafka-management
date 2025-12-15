.PHONY: reset install-dev install-pre-commit-hook tests typecheck lint build

reset:
	rm -rf .venv

install-pre-commit-hook:
	.venv/bin/pre-commit install --install-hooks

install-dev:
	devenv sync
	uv sync --group dev
	$(MAKE) install-pre-commit-hook

# Internal pypy doesn't support versions lower than 3.11, so we install the minimum
# to make tests work from external pypi
install-ci-3.9:
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv export -o requirements.txt --group ci_3_9 --python-preference only-system --default-index 'https://pypi.org/simple' --no-cache
	uv venv --default-index 'https://pypi.org/simple'
	uv pip install -r requirements.txt --default-index 'https://pypi.org/simple' --no-cache

install-ci:
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv sync --only-group dev --python-preference only-system --no-cache

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
