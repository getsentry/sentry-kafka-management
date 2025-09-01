#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from typing import Sequence

import yaml

from sentry_kafka_management.actions.topics import list_topics
from sentry_kafka_management.brokers import ClusterConfig


def main(argv: Sequence[str] | None = None) -> int:
    """List topic names for a cluster from a clusters YAML file."""
    parser = argparse.ArgumentParser(
        description="List Kafka topics using a single clusters configuration file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -c config.yml
  %(prog)s -c config.yml -n my-cluster
        """,
    )

    parser.add_argument(
        "-c",
        "--config",
        required=True,
        help="Path to the clusters YAML configuration file",
    )

    parser.add_argument(
        "-n",
        "--cluster",
        help="Name of the cluster to query (uses first available if not specified)",
    )

    args = parser.parse_args(argv)

    clusters_raw = yaml.safe_load(Path(args.config).read_text())
    clusters: dict[str, ClusterConfig] = {
        key: ClusterConfig(
            brokers=value["brokers"],
            security_protocol=value.get("security_protocol"),
            sasl_mechanism=value.get("sasl_mechanism"),
            sasl_username=value.get("sasl_username"),
            sasl_password=value.get("sasl_password"),
        )
        for key, value in clusters_raw.items()
    }

    cluster_name = args.cluster or next(iter(clusters.keys()))
    result = list_topics(clusters[cluster_name])
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    main()
