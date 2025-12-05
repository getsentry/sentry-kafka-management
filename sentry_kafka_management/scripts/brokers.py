#!/usr/bin/env python3

import json
from pathlib import Path
from typing import Sequence

import click

from sentry_kafka_management.actions.brokers import apply_configs as apply_config_action
from sentry_kafka_management.actions.brokers import (
    cleanup_config_record,
)
from sentry_kafka_management.actions.brokers import (
    describe_broker_configs as describe_broker_configs_action,
)
from sentry_kafka_management.actions.brokers import (
    read_record_dir,
)
from sentry_kafka_management.actions.brokers import (
    remove_dynamic_configs as remove_dynamic_configs_action,
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


def parse_configs_to_remove(
    ctx: click.Context, param: click.Parameter, value: str
) -> list[str] | None:
    return value.split(",")


def parse_broker_ids(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> list[str] | None:
    if value is None:
        return None
    try:
        broker_ids = [id.strip() for id in value.split(",") if id.strip()]
        return broker_ids if broker_ids else None
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
    help="Name of the cluster to query",
)
def describe_broker_configs(config: Path, cluster: str) -> None:
    """
    List all broker configs on a cluster, including whether they were set dynamically or statically.
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)
    result = describe_broker_configs_action(client)
    click.echo(json.dumps(result, indent=2))


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
    help="Comma separated list of configuration changes to apply, in key=value format",
)
@click.option(
    "--broker-ids",
    required=False,
    callback=parse_broker_ids,
    help=(
        "Comma separated list of broker IDs to apply config to, "
        "if not provided, config will be applied to all brokers in the cluster"
    ),
)
@click.option(
    "--configs-record-dir",
    required=False,
    type=click.Path(exists=True, path_type=Path),
    help=(
        "Path to a directory to record config changes in."
        "Config changes will be recorded as files in the dir with"
        "filenames equal to the config names, each containing the config's value."
        "Will not record invalid configs."
    ),
)
def apply_configs(
    config: Path,
    cluster: str,
    config_changes: dict[str, str],
    broker_ids: list[str] | None = None,
    configs_record_dir: Path | None = None,
) -> None:
    """
    Apply a configuration change to a broker.

    This command applies a dynamic configuration that takes precedence over
    static configs set by salt or other configuration management tools.

    Usage:
        kafka-scripts apply-config -c config.yml -n my-cluster
        --config-changes 'message.max.bytes=1048588,max.connections=1000'
        --broker-ids '0,1,2'
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)

    success, error = apply_config_action(
        client,
        config_changes,
        broker_ids,
        configs_record_dir,
    )

    if success:
        click.echo("Success:")
        click.echo(json.dumps(success, indent=2))
    if error:
        click.echo("Error:")
        click.echo(json.dumps(error, indent=2))
        raise click.ClickException("One or more config changes failed")
    click.echo("All config changes applied successfully")


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
    "--configs-to-remove",
    required=True,
    callback=parse_configs_to_remove,
    help="Comma separated list of config names to remove from dynamic configs.",
)
@click.option(
    "--broker-ids",
    required=False,
    callback=parse_broker_ids,
    help=(
        "Comma separated list of broker IDs to remove config from, "
        "if not provided, config will be removed from all brokers in the cluster"
    ),
)
def remove_dynamic_configs(
    config: Path,
    cluster: str,
    configs_to_remove: Sequence[str],
    broker_ids: list[str] | None = None,
) -> None:
    """
    Removes dynamic configs from a broker.

    When a dynamic config is removed from a broker, the value for that config will
    revert to being either:
    * the static value defined in `server.properties`, if one exists
    * the config default value, if there's no static value defined for it

    Usage:
        kafka-scripts remove-dynamic-configs -c config.yml -n my-cluster
        --configs-to-remove 'message.max.bytes,max.connections'
        --broker-ids '0,1,2'
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)

    success, error = remove_dynamic_configs_action(
        client,
        configs_to_remove,
        broker_ids,
    )

    if success:
        click.echo("Success:")
        click.echo(json.dumps(success, indent=2))
    if error:
        click.echo("Error:")
        click.echo(json.dumps(error, indent=2))
        raise click.ClickException("One or more config removals failed")
    click.echo("All dynamic configs removed successfully")


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
    "-r",
    "--configs-record-dir",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the directory containing config change record files",
)
@click.option(
    "--cleanup-records",
    is_flag=True,
    help="Whether to delete records in the record dir after deleting their respective configs",
)
@click.option(
    "--broker-ids",
    required=False,
    callback=parse_broker_ids,
    help=(
        "Comma separated list of broker IDs to remove recorded configs from, "
        "if not provided, recorded configs will be removed from all brokers in the cluster."
    ),
)
def remove_recorded_dynamic_configs(
    config: Path,
    cluster: str,
    configs_record_dir: Path,
    cleanup_records: bool,
    broker_ids: list[str] | None = None,
) -> None:
    """
    Removes dynamic configs from a broker by reading from config record files at
    the given path. Intended to be used to clean up configs set by the `apply-configs` script
    that were recorded with the `--configs-record-dir` flag.

    When a dynamic config is removed from a broker, the value for that config will
    revert to being either:
    * the static value defined in `server.properties`, if one exists
    * the config default value, if there's no static value defined for it

    Usage:
        kafka-scripts remove-recorded-dynamic-configs -c config.yml -n my-cluster
        --configs-record-dir /emergency-configs
        --broker-ids '0,1,2'
    """
    yaml_config = YamlKafkaConfig(config)
    cluster_config = yaml_config.get_clusters()[cluster]
    client = get_admin_client(cluster_config)

    configs_to_remove = read_record_dir(configs_record_dir)

    success, error = remove_dynamic_configs_action(
        client,
        list(configs_to_remove.keys()),
        broker_ids,
    )

    # optionally delete all record files for configs that were deleted
    if cleanup_records:
        for deleted_config in success:
            cleanup_config_record(configs_record_dir, deleted_config["config_name"])

    if success:
        click.echo("Success:")
        click.echo(json.dumps(success, indent=2))
    if error:
        click.echo("Error:")
        click.echo(json.dumps(error, indent=2))
        raise click.ClickException("One or more config removals failed")
    click.echo("All dynamic configs removed successfully")
