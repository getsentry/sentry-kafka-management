#!/usr/bin/env python3

from collections import defaultdict
from pathlib import Path

import click
import yaml

from sentry_kafka_management.actions.brokers.parser import get_broker_id
from sentry_kafka_management.actions.topics.placement import (
    TopicPlacement,
    compute_cluster_placement,
)


def get_default_partitions(shared_config_path: Path) -> int:
    """Get the default partition count from the defaults file."""
    defaults_path = shared_config_path / "topics" / "defaults" / "defaults.yaml"
    with open(defaults_path, "r") as f:
        config = yaml.safe_load(f)
    return int(config["partitions"])


def get_topic_partitions(shared_config_path: Path, topic_name: str) -> int | None:
    """Get the partition count from the top-level topic definition."""
    topic_path = shared_config_path / "topics" / f"{topic_name}.yaml"
    if not topic_path.exists():
        return None
    with open(topic_path, "r") as f:
        config = yaml.safe_load(f)
    partitions = config.get("partitions")
    return int(partitions) if partitions is not None else None


def parse_topic_partitions(
    shared_config_path: Path, region: str, cluster_name: str
) -> dict[str, int]:
    """
    Parse the topic partitions for a given cluster from regional override files.

    Looks up partition counts in this order:
    1. Regional override file (topics/regional_overrides/{region}/{topic}.yaml)
    2. Top-level topic file (topics/{topic}.yaml)
    3. Global defaults (topics/defaults/defaults.yaml)
    """
    default_partitions = get_default_partitions(shared_config_path)
    overrides_directory = shared_config_path / "topics" / "regional_overrides" / region

    topic_partitions: dict[str, int] = {}
    for topic_file in sorted(overrides_directory.glob("*.yaml")):
        topic_name = topic_file.stem
        with open(topic_file, "r") as f:
            config = yaml.safe_load(f) or {}

        if config.get("cluster") != cluster_name:
            continue

        partitions = config.get("partitions")
        if partitions is None:
            partitions = get_topic_partitions(shared_config_path, topic_name)
        if partitions is None:
            partitions = default_partitions

        topic_partitions[topic_name] = partitions

    return topic_partitions


def count_leader_distribution(result: list[TopicPlacement]) -> None:
    leader_counts: dict[int, int] = defaultdict(int)
    replica_counts: dict[int, int] = defaultdict(int)
    for topic in result:
        for assignment in topic.partitions:
            leader_counts[assignment[0]] += 1
            for broker in assignment:
                replica_counts[broker] += 1

    sorted_leaders = [str(leader_counts[b]) for b in sorted(leader_counts)]
    sorted_replicas = [str(replica_counts[b]) for b in sorted(replica_counts)]
    click.echo(f"Leader distribution: {'/'.join(sorted_leaders)}")
    click.echo(f"Replica distribution: {'/'.join(sorted_replicas)}")


@click.command()
@click.option("--shared-config-path", type=click.Path(exists=True, path_type=Path), required=True)
@click.option("--region", required=True)
@click.option("--cluster-name", required=True)
def compute_topic_placement(
    shared_config_path: Path,
    region: str,
    cluster_name: str,
) -> None:
    """Compute partition placement for all topics in a cluster."""
    with open(Path(shared_config_path, "clusters", f"{region}.yaml"), "r") as f:
        config = yaml.safe_load(f)
    brokers = config[cluster_name]["brokers"]

    broker_id_mapping = {}
    for broker in brokers:
        broker_id_mapping[broker] = get_broker_id(broker)

    topic_partitions = parse_topic_partitions(shared_config_path, region, cluster_name)

    result = compute_cluster_placement(broker_id_mapping, topic_partitions)
    overrides_directory = shared_config_path / "topics" / "regional_overrides" / region
    for topic_placement in result:
        file_path = overrides_directory / f"{topic_placement.topic}.yaml"
        with open(file_path, "r") as f:
            config = yaml.safe_load(f) or {}

        config["placement"] = {
            "staticAssignments": [list(assignment) for assignment in topic_placement.partitions],
            "strategy": "static",
        }

        with open(file_path, "w") as f:
            yaml.dump(config, f, default_flow_style=None)
        click.echo(f"Wrote {file_path}")

    count_leader_distribution(result)
