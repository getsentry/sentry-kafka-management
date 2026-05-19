#!/usr/bin/env python3

import time
from pathlib import Path

import click

from sentry_kafka_management.actions.latency.consumer_latency import (
    record_consumer_group_latency as record_consumer_group_latency_action,
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
    "-i",
    "--interval",
    required=False,
    default=10.0,
    type=click.FloatRange(min=0.0, min_open=True),
    help="How often in seconds to collect metrics. Defaults to 10s.",
)
@click.option(
    "-t",
    "--timeout",
    required=False,
    default=10,
    type=int,
    help="How long in seconds to wait before Kafka requests time out. Defaults to 10s.",
)
def consumer_latency(
    config: Path,
    statsd_host: str,
    statsd_port: int,
    interval: float,
    timeout: int,
) -> None:
    """
    Emit Kafka consumer latency metrics for configured clusters.
    """
    kafka_config = YamlKafkaConfig(config)
    metrics_backend = DatadogMetricsBackend(statsd_host, statsd_port)

    click.echo(f"Starting consumer latency collection (interval={interval:.3f}s)")

    while True:
        scans = record_consumer_group_latency_action(kafka_config, metrics_backend, timeout)
        if scans:
            for scan in scans:
                click.echo(
                    f"Collected latency cluster={scan.cluster_name} "
                    f"group={scan.group_id} topic={scan.topic_name} "
                    f"latency_ms={scan.latency_ms:.1f}"
                )
        else:
            click.echo("No consumer latency collected this iteration")
        time.sleep(interval)
