#!/usr/bin/env python3

import json
from typing import Sequence

from sentry_kafka_management.actions.topics import list_topics as list_topics_action
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.common import kafka_script_parser
from sentry_kafka_management.connectors.admin import get_admin_client


def list_topics(argv: Sequence[str] | None = None) -> int:
    """List topic names for a cluster from a clusters YAML file."""

    parser = kafka_script_parser(
        description="List Kafka topics using a single clusters configuration file",
        epilog="""
Examples:
  %(prog)s -c config.yml -t topic.yml -n my-cluster
  %(prog)s --config config.yml --cluster production
        """,
    )

    parser.add_argument(
        "-n",
        "--cluster",
        required=True,
        help="Name of the cluster to query",
    )

    args = parser.parse_args(argv)

    config = YamlKafkaConfig(args.config)
    cluster_config = config.get_clusters()[args.cluster]
    client = get_admin_client(cluster_config)
    result = list_topics_action(client)
    print(json.dumps(result, indent=2))
    return 0
