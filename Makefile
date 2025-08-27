.PHONY: reset install-dev install-pre-commit-hook tests typecheck

reset:
	rm -rf .venv

install-pre-commit-hook:
	.venv/bin/pre-commit install --install-hooks

install-dev:
	devenv sync
	uv sync --group dev
	$(MAKE) install-pre-commit-hook

tests:
	pytest -vv tests

typecheck:
	mypy --strict sentry_kafka_management
	mypy --strict tests
