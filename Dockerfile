# syntax=docker/dockerfile:1

FROM python:3.12.6-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Create non-root user and group
RUN set -ex; \
    groupadd -r kafka_mgmt --gid 1000; \
    useradd -r -g kafka_mgmt --uid 1000 kafka_mgmt

# Install minimal OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

# Install Python dependencies declared in pyproject.toml
COPY pyproject.toml pyproject.toml
RUN uv pip install --system -r pyproject.toml

# Copy application source (owned by non-root user)
COPY --chown=kafka_mgmt:kafka_mgmt sentry_kafka_management/ sentry_kafka_management/

# Ensure workspace ownership
RUN chown -R kafka_mgmt:kafka_mgmt /app

# Switch to non-root user
USER kafka_mgmt

# Default entrypoint is the unified CLI router
ENTRYPOINT ["python", "-m", "sentry_kafka_management.cli"]
CMD ["--help"]
