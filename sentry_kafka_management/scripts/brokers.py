#!/usr/bin/env python3

import argparse
import json
from typing import Sequence

from sentry_kafka_management.actions.brokers import (
    describe_broker_configs as describe_broker_configs_action,
)
from sentry_kafka_management.brokers import YamlKafkaConfig


def describe_broker_configs(argv: Sequence[str] | None = None) -> int:
    """Returns a list of topics"""
    parser = argparse.ArgumentParser(
        description="List Kafka topics using configuration file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -c config.yml
  %(prog)s -c config.yml -n my-cluster
  %(prog)s --config config.yml --cluster production
        """,
    )

    parser.add_argument(
        "-c", "--cluster-config", required=True, help="Path to the cluster YAML configuration file"
    )

    parser.add_argument(
        "-t", "--topic-config", required=True, help="Path to the topics' YAML configuration file"
    )

    parser.add_argument(
        "-n",
        "--cluster",
        help="Name of the cluster to query (uses first available if not specified)",
    )

    args = parser.parse_args()

    config = YamlKafkaConfig(args.cluster_config, args.topic_config)
    result = describe_broker_configs_action(config.get_clusters()[args.cluster])
    print(json.dumps(result, indent=2))
    return 0
