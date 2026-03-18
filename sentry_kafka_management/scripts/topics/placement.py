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
from sentry_kafka_management.actions.topics.placement import (
    build_slices,
    compute_cluster_placement,
)
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client


@click.command()
@click.option("-c", "--config", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("-n", "--cluster", required=True)
def compute_topic_placement(config: Path, cluster: str) -> None:
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

    slices = build_slices(broker_id_mapping)

    current: dict[str, dict[int, list[int]]] = {}
    for name in list_topics_action(client):
        partitions = describe_topic_partitions_action(client, name)
        current[name] = {p["id"]: p["replicas"] for p in partitions}

    result = compute_cluster_placement(slices, current)
    output_blocks: list[str] = []
    for topic_name in sorted(result.keys()):
        topic_partitions = result[topic_name]
        static_assignments = [replicas for _, replicas in sorted(topic_partitions.items())]
        lines = [
            f"topic: {topic_name}",
            "---",
            f"cluster: {cluster}",
            f"partitions: {len(static_assignments)}",
            "placement:",
            "  staticAssignments:",
        ]
        for replicas in static_assignments:
            lines.append(f"  - [{', '.join(str(r) for r in replicas)}]")
        lines.append("  strategy: static")
        output_blocks.append("\n".join(lines))

    click.echo("\n---\n".join(output_blocks))
