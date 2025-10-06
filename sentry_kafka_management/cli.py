#!/usr/bin/env python3

from __future__ import annotations

import click

from sentry_kafka_management.scripts.brokers import describe_broker_configs
from sentry_kafka_management.scripts.clusters import describe_cluster
from sentry_kafka_management.scripts.topics import list_topics

COMMANDS = [describe_broker_configs, describe_cluster, list_topics]


@click.group()
def main() -> None:
    """
    CLI entrypoint for sentry-kafka-management.
    """
    pass


for command in COMMANDS:
    main.add_command(command)

if __name__ == "__main__":
    main()
