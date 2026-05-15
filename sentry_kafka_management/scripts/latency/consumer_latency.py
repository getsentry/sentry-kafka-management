#!/usr/bin/env python3

import time
from pathlib import Path

import click

from sentry_kafka_management.actions.latency.consumer_latency import (
    run_latency_metrics as run_latency_metrics_action,
)
from sentry_kafka_management.actions.latency.metrics import DatadogMetricsBackend
from sentry_kafka_management.brokers import YamlKafkaConfig


@click.command(name="consumer-latency")
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the YAML configuration file",
)
@click.option(
    "--statsd-host",
    required=True,
    help="DogStatsD host to emit metrics to",
)
@click.option(
    "--statsd-port",
    required=True,
    type=int,
    help="DogStatsD port to emit metrics to",
)
@click.option(
    "--interval",
    required=False,
    default=1.0,
    type=click.FloatRange(min=0.0, min_open=True),
    help="How often in seconds to collect metrics. Defaults to 1s.",
)
def consumer_latency(
    config: Path,
    statsd_host: str,
    statsd_port: int,
    interval: float,
) -> None:
    """
    Emit Kafka consumer latency metrics for configured clusters.
    """
    kafka_config = YamlKafkaConfig(config)
    metrics_backend = DatadogMetricsBackend(statsd_host, statsd_port)

    while True:
        run_latency_metrics_action(kafka_config, metrics_backend)
        time.sleep(interval)
