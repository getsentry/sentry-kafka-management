#!/usr/bin/env python3

from pathlib import Path

import click

from sentry_kafka_management.actions.clusters import describe_cluster
from sentry_kafka_management.actions.topics.describe import (
    describe_topic_partitions as describe_topic_partitions_action,
)
from sentry_kafka_management.actions.topics.describe import (
    list_topics as list_topics_action,
)
from sentry_kafka_management.actions.topics.placement import compute_cluster_placement
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client


@click.command()
@click.option("-c", "--config", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("-n", "--cluster", required=True)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(path_type=Path),
    required=True,
    help="Directory to write per-topic YAML files.",
)
def compute_topic_placement(config: Path, cluster: str, output_dir: Path) -> None:
    """Compute partition placement for all topics in a cluster."""
    kafka_config = YamlKafkaConfig(config)
    cluster_config = kafka_config.get_clusters()[cluster]
    brokers_config = list(cluster_config["brokers"])

    client = get_admin_client(cluster_config)
    cluster_brokers = describe_cluster(client)

    broker_id_mapping = {}
    for broker in brokers_config:
        for cluster_broker in cluster_brokers:
            if broker == f"{cluster_broker['host']}:{cluster_broker['port']}":
                broker_id_mapping[broker] = int(cluster_broker["id"])
                break
        else:
            raise click.ClickException(f"Broker {broker} not found in cluster")

    topic_partitions: dict[str, int] = {}
    for name in list_topics_action(client):
        partitions = describe_topic_partitions_action(client, name)
        topic_partitions[name] = len(partitions)

    output_dir.mkdir(parents=True, exist_ok=True)

    result = compute_cluster_placement(broker_id_mapping, topic_partitions)
    for topic_placement in result:
        lines = [
            f"cluster: {cluster}",
            f"partitions: {len(topic_placement.partitions)}",
            "placement:",
            "  staticAssignments:",
        ]
        for assignment in topic_placement.partitions:
            lines.append(f"  - [{', '.join(str(r) for r in assignment)}]")
        lines.append("  strategy: static")

        file_path = output_dir / f"{topic_placement.topic}.yaml"
        file_path.write_text("\n".join(lines) + "\n")
        click.echo(f"Wrote {file_path}")
