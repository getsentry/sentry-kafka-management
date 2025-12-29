#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.brokers import (
    remove_dynamic_configs as remove_dynamic_configs_action,
)
from sentry_kafka_management.actions.local.filesystem import (
    cleanup_config_record,
    read_record_dir,
)
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.scripts.brokers import parse_broker_ids
from sentry_kafka_management.scripts.config_helpers import get_cluster_config


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
@click.option(
    "--dry-run",
    is_flag=True,
    help="Whether to dry run the config removals, only performs validation",
)
def remove_recorded_dynamic_configs(
    config: Path,
    cluster: str,
    configs_record_dir: Path,
    cleanup_records: bool,
    broker_ids: list[str] | None = None,
    dry_run: bool = False,
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
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)

    configs_to_remove = read_record_dir(configs_record_dir)

    success, error = remove_dynamic_configs_action(
        client,
        list(configs_to_remove.keys()),
        broker_ids,
        dry_run,
    )

    if not dry_run:
        # optionally delete all record files for configs that were deleted
        if cleanup_records:
            for deleted_config in success:
                print(deleted_config)
                cleanup_config_record(configs_record_dir, deleted_config["config_name"])

    if success:
        click.echo("Success:")
        click.echo(json.dumps(success, indent=2))
    if error:
        click.echo("Error:")
        click.echo(json.dumps(error, indent=2))
        raise click.ClickException("One or more config removals failed")
    if dry_run:
        click.echo("Dry run completed successfully")
    else:
        click.echo("All dynamic configs removed successfully")
