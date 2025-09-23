#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.brokers import (
    describe_broker_configs as describe_broker_configs_action,
)
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client


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
def describe_broker_configs(config: Path, cluster: str) -> None:
    """
    List all broker configs on a cluster, including whether they were set dynamically or statically.
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)
    result = describe_broker_configs_action(client)
    click.echo(json.dumps(result, indent=2))
