#!/usr/bin/env python3

import json
from pathlib import Path

import click

from sentry_kafka_management.actions.local.manage_configs import (
    update_config_state as update_config_state_action,
)
from sentry_kafka_management.connectors.admin import get_admin_client
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
    "--record-dir",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the directory containing emergency config records",
)
@click.option(
    "-p",
    "--properties-file",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Path to the Kafka server.properties file",
)
@click.option(
    "-s",
    "--sasl-credentials-file",
    type=click.Path(exists=True, path_type=Path),
    required=False,
    help="Path to the SASL credentials file",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Whether to dry run the config changes, only performs validation",
)
def update_config_state(
    config: Path,
    cluster: str,
    record_dir: Path,
    properties_file: Path,
    sasl_credentials_file: Path | None = None,
    dry_run: bool = False,
) -> None:
    """
    Updates the config state of the current broker by looking at emergency configs,
    kafka-configs CLI output, and server.properties file.

    This command is intended to be run locally on a broker to manage dynamic
    configs. It will:

    1. Apply emergency configs in record_dir as dynamic configs (highest priority)
    2. Apply configs from server.properties as dynamic configs if active value differs
    3. Remove dynamic configs when the static value matches the desired value from
       server.properties

    Usage:
        kafka-scripts apply-desired-configs -c config.yml -n my-cluster \\
            --record-dir /emergency-configs \\
            --properties-file /etc/kafka/server.properties \\
            --sasl-credentials-file /path/to/client.properties
    """
    cluster_config = get_cluster_config(config, cluster)
    client = get_admin_client(cluster_config)

    success, error = update_config_state_action(
        client,
        record_dir,
        properties_file,
        sasl_credentials_file=sasl_credentials_file,
        dry_run=dry_run,
    )

    if success:
        click.echo("Success:")
        click.echo(json.dumps(success, indent=2))
    if error:
        click.echo("Error:")
        click.echo(json.dumps(error, indent=2))
        raise click.ClickException("One or more config changes failed")
    if not success and not error:
        click.echo("No config changes needed, all configs are already in desired state")
    if dry_run:
        click.echo("Dry run completed successfully")
    else:
        click.echo("All config operations completed successfully")
