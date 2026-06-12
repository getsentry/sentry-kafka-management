#!/usr/bin/env python3

import logging
import time
from pathlib import Path

import click
import sentry_sdk

from sentry_kafka_management.actions.latency.consumer_latency import (
    DEFAULT_MAX_WORKERS,
)
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
@click.option(
    "-w",
    "--max-workers",
    required=False,
    default=DEFAULT_MAX_WORKERS,
    type=click.IntRange(min=1),
    help="Maximum concurrent partition scans per cluster. Defaults to 128.",
)
@click.option(
    "-l",
    "--log-level",
    required=False,
    default="DEBUG",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Logging verbosity. DEBUG (default) shows per-partition latency details.",
)
def consumer_latency(
    config: Path,
    statsd_host: str,
    statsd_port: int,
    interval: float,
    timeout: int,
    max_workers: int,
    log_level: str,
) -> None:
    """
    Emit Kafka consumer latency metrics for configured clusters.
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    kafka_config = YamlKafkaConfig(config)
    metrics_backend = DatadogMetricsBackend(statsd_host, statsd_port)

    click.echo(
        f"Starting consumer latency collection "
        f"(interval={interval:.3f}s, max_workers={max_workers})"
    )

    while True:
        try:
            result = record_consumer_group_latency_action(
                kafka_config, metrics_backend, timeout, max_workers
            )
        except Exception:
            sentry_sdk.capture_exception()
            raise

        for scan in result.scans:
            click.echo(
                f"Collected latency cluster={scan.cluster_name} "
                f"group={scan.group_id} topic={scan.topic_name} "
                f"partition={scan.partition} latency_ms={scan.latency_ms:.1f}"
            )

        if not result.scans and not result.errors:
            click.echo("No consumer latency collected this iteration")

        if result.errors:
            for error in result.errors:
                click.echo(f"Error: {error}", err=True)
                sentry_sdk.capture_exception(error)
            raise click.ClickException(
                f"Consumer latency collection failed ({len(result.errors)} error(s))"
            )

        time.sleep(interval)
