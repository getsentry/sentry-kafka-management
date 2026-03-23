import json
from pathlib import Path
from typing import Tuple

import click

from sentry_kafka_management.actions.topics.partitions import (
    elect_partition_leaders as elect_partition_leaders_action,
)
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.scripts.config_helpers import get_cluster_config


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the YAML configuration file.",
)
@click.option(
    "-n",
    "--cluster",
    required=True,
    help="Name of the cluster to perform leader election on.",
)
@click.option(
    "-t", "--topic", required=False, multiple=True, help="A topic to perform leader election on."
)
def elect_partition_leaders(config: Path, cluster: str, topic: Tuple[str, ...]) -> None:
    """
    Runs leader elections on a cluster. If `--topic` is provided will only run an
    election for those topics' partitions. Otherwise, will run an election for all
    partitions in the cluster.
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)

    election_results = elect_partition_leaders_action(
        admin_client=client,
        topics=topic,
    )
    click.echo(json.dumps(election_results, indent=2))
