#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.clusters import (
    describe_cluster as describe_cluster_action,
)
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.scripts.config_helpers import get_cluster_config


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
def describe_cluster(config: Path, cluster: str) -> None:
    """
    Describe a Kafka cluster.
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)
    result = describe_cluster_action(client)
    click.echo(json.dumps(result, indent=2))
