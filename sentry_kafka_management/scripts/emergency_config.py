#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.emergency_config import (
    apply_emergency_config as apply_emergency_config_action,
)
from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client


def parse_config_changes(
    ctx: click.Context, param: click.Parameter, value: str
) -> dict[str, str] | None:
    try:
        return {key: value for key, value in [change.split("=") for change in value.split(",")]}
    except ValueError as e:
        raise click.BadParameter(f"Invalid config: {e}")


def parse_broker_ids(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> list[str] | None:
    if value is None:
        return None
    try:
        return [id.strip() for id in value.split(",")]
    except ValueError as e:
        raise click.BadParameter(f"Invalid broker IDs: {e}")


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the YAML configuration file",
)
@click.option(
    "-n",
    "--cluster",
    required=True,
    help="Name of the cluster",
)
@click.option(
    "--config-changes",
    required=True,
    callback=parse_config_changes,
    help="Comma separated list of configuration changes to apply",
)
@click.option(
    "--broker-ids",
    required=False,
    callback=parse_broker_ids,
    help="Broker IDs to apply config to",
)
def apply_emergency_config(
    config: Path,
    cluster: str,
    config_changes: dict[str, str],
    broker_ids: list[str] | None = None,
) -> None:
    """
    Apply an emergency configuration change to a broker.

    This command applies a dynamic configuration that takes precedence over
    static configs set by salt or other configuration management tools.

    Usage:
        kafka-scripts apply-emergency-config \\
            -c config.yml -n my-cluster \\
            --config-changes 'message.max.bytes=1048588,max.connections=1000' \\
            --broker-ids '0,1,2'
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)

    results = apply_emergency_config_action(
        client,
        config_changes,
        broker_ids,
    )

    click.echo(json.dumps(results, indent=2))

    if any(r["status"] == "error" for r in results):
        raise click.ClickException("One or more config changes failed")

    click.echo("All config changes applied successfully")
