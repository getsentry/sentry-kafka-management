#!/usr/bin/env python3

from __future__ import annotations

import click

from sentry_kafka_management import __version__
from sentry_kafka_management.scripts.brokers.configs import (
    apply_configs,
    describe_broker_configs,
    remove_dynamic_configs,
)
from sentry_kafka_management.scripts.clusters import describe_cluster
from sentry_kafka_management.scripts.local.filesystem import (
    remove_recorded_dynamic_configs,
)
from sentry_kafka_management.scripts.local.manage_configs import (
    update_config_state,
)
from sentry_kafka_management.scripts.topics.describe import (
    describe_topic_partitions,
    list_offsets,
    list_topics,
)
from sentry_kafka_management.scripts.topics.healthcheck import (
    healthcheck_cluster_topics,
)

COMMANDS = [
    apply_configs,
    describe_topic_partitions,
    describe_broker_configs,
    describe_cluster,
    healthcheck_cluster_topics,
    list_topics,
    list_offsets,
    remove_dynamic_configs,
    remove_recorded_dynamic_configs,
    update_config_state,
]


@click.group()
@click.version_option(version=__version__, prog_name="sentry-kafka-management")
def main() -> None:
    """
    CLI entrypoint for sentry-kafka-management.
    """
    pass


for command in COMMANDS:
    main.add_command(command)

if __name__ == "__main__":
    main()
