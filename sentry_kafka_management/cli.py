#!/usr/bin/env python3

from __future__ import annotations

import click
import sentry_sdk

from sentry_kafka_management import __version__
from sentry_kafka_management.scripts.brokers.configs import (
    apply_configs,
    describe_broker_configs,
    remove_dynamic_configs,
)
from sentry_kafka_management.scripts.clusters import (
    describe_cluster,
    get_cluster_controller,
)
from sentry_kafka_management.scripts.latency.consumer_latency import consumer_latency
from sentry_kafka_management.scripts.local.filesystem import (
    remove_recorded_dynamic_configs,
)
from sentry_kafka_management.scripts.local.manage_configs import update_config_state
from sentry_kafka_management.scripts.topics.describe import (
    describe_topic_partitions,
    list_offsets,
    list_topics,
)
from sentry_kafka_management.scripts.topics.healthcheck import (
    healthcheck_cluster_topics,
)
from sentry_kafka_management.scripts.topics.partitions import elect_partition_leaders
from sentry_kafka_management.scripts.topics.placement import compute_topic_placement

SENTRY_DSN = (
    "https://93b0938702b2a9c18ffd9312643a1e5b"
    "@o4510127168028672.ingest.s4s2.sentry.io/4510749467607136"
)

COMMANDS = [
    apply_configs,
    compute_topic_placement,
    consumer_latency,
    describe_topic_partitions,
    describe_broker_configs,
    describe_cluster,
    elect_partition_leaders,
    get_cluster_controller,
    healthcheck_cluster_topics,
    list_topics,
    list_offsets,
    remove_dynamic_configs,
    remove_recorded_dynamic_configs,
    update_config_state,
]


@click.group()
@click.version_option(version=__version__, prog_name="sentry-kafka-management")
@click.pass_context
def main(ctx: click.Context) -> None:
    """
    CLI entrypoint for sentry-kafka-management.
    """
    sentry_sdk.init(dsn=SENTRY_DSN, release=__version__)
    if ctx.invoked_subcommand is not None:
        sentry_sdk.set_tag("command", ctx.invoked_subcommand)


for command in COMMANDS:
    main.add_command(command)

if __name__ == "__main__":
    main()
