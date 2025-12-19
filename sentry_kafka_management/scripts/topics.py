#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.topics import list_offsets as list_offsets_action
from sentry_kafka_management.actions.topics import list_topics as list_topics_action
from sentry_kafka_management.brokers import ClusterConfig, YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client


def get_cluster_config(config: Path, cluster: str) -> ClusterConfig:
    yaml_config = YamlKafkaConfig(config)
    return yaml_config.get_clusters()[cluster]


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
def list_topics(config: Path, cluster: str) -> None:
    """
    List Kafka topics for a given Kafka cluster.
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)
    result = list_topics_action(client)
    click.echo(json.dumps(result, indent=2))


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
@click.option("-t", "--topic", required=True, help="Name of the topic to query")
def list_offsets(config: Path, cluster: str, topic: str) -> None:
    """
    List Kafka topics for a given Kafka cluster.
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)
    result = list_offsets_action(client, topic)
    click.echo(json.dumps(result, indent=2))
