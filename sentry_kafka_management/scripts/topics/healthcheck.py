#!/usr/bin/env python3

import json
import time
from pathlib import Path

import click

from sentry_kafka_management.actions.topics.healthcheck import (
    HealthResponse,
)
from sentry_kafka_management.actions.topics.healthcheck import (
    healthcheck_cluster_topics as healthcheck_cluster_topics_actions,
)
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.scripts.config_helpers import get_cluster_config


class HealthcheckTimeoutError(TimeoutError):
    def __init__(self, health_response: HealthResponse):
        super().__init__(
            f"Cluster did not become healthy in time. Reason(s): {health_response.reason}."
        )


def _maybe_log_result(
    health_resonse: HealthResponse, timeout_occurred: bool, log_each_iteration: bool
) -> None:
    """
    Logs the given HealthResponse if one of the following is True:
    * The cluster is healthy
    * The cluster did not become healthy within the timeout period
    * The user set the script to log every healthcheck iteration
    """
    if health_resonse.healthy or timeout_occurred or log_each_iteration:
        click.echo(json.dumps(health_resonse.to_json(), indent=2))


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the YAML configuration file",
)
@click.option(
    "-n",
    "--cluster",
    required=True,
    help="Name of the cluster to query",
)
@click.option(
    "-t",
    "--timeout",
    required=False,
    default=60,
    type=int,
    help="How long in seconds to wait before healthcheck times out. Defaults to 60s.",
)
@click.option(
    "-i",
    "--check-interval",
    required=False,
    default=2,
    type=int,
    help="How often in seconds to run the healthcheck. Defaults to every 2s.",
)
@click.option(
    "-l",
    "--log-each-iteration",
    is_flag=True,
    help="Whether the healthcheck should log each response received from the cluster.",
)
def healthcheck_cluster_topics(
    config: Path,
    cluster: str,
    timeout: int,
    check_interval: int,
    log_each_iteration: bool,
) -> None:
    """
    Healthcheck the topics on a cluster.
    Blocks in a loop until either a healthy response is received, or the timeout is reached.
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)
    cluster_is_healthy = False
    timeout_occurred = False
    start_time = time.time()
    while not cluster_is_healthy and not timeout_occurred:
        result = healthcheck_cluster_topics_actions(client)
        timeout_occurred = (time.time() - start_time) >= timeout
        _maybe_log_result(result, timeout_occurred, log_each_iteration)
        cluster_is_healthy = result.healthy
        time.sleep(check_interval)
    if not cluster_is_healthy:
        raise HealthcheckTimeoutError(result)
