# syntax=docker/dockerfile:1

FROM python:3.12.6-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal OS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python runtime dependencies directly
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir "confluent-kafka>=2.8.0" "PyYAML"

# Copy application source
COPY sentry_kafka_management/ sentry_kafka_management/

# Default entrypoint is the unified CLI router
ENTRYPOINT ["python", "-m", "sentry_kafka_management.cli"]
CMD ["--help"]
