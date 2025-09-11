#!/usr/bin/env python3

import json
from typing import Sequence

from sentry_kafka_management.actions.brokers import (
    describe_broker_configs as describe_broker_configs_action,
)
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.common import kafka_script_parser
from sentry_kafka_management.connectors.admin import get_admin_client


def describe_broker_configs(argv: Sequence[str] | None = None) -> int:
    """Returns all broker configs on a given cluster."""
    parser = kafka_script_parser(
        description="""
List all broker configs in a cluster, including whether they were set dynamically or statically
        """,
        epilog="""
Examples:
  %(prog)s -c config.yml -n my-cluster
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
    result = describe_broker_configs_action(client)
    print(json.dumps(result, indent=2))
    return 0
