from pathlib import Path
from typing import Any

from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from sentry_kafka_management.actions.brokers import (
    apply_configs,
    remove_dynamic_configs,
)
from sentry_kafka_management.actions.local.filesystem import read_record_dir
from sentry_kafka_management.actions.local.kafka_cli import (
    Config,
    get_active_broker_configs,
)
from sentry_kafka_management.actions.local.server_properties import (
    read_server_properties,
)


def update_config_state(
    admin_client: AdminClient,
    record_dir: Path,
    properties_file: Path,
    sasl_credentials_file: Path | None = None,
    dry_run: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Gets the desired configs from reading emergency configs from record_dir,
    parsing output from kafka-configs CLI and parsing server.properties file.

    Updates the configs of the current broker the action is running on to the desired state.

    Adding configs:
    - Emergency configs must always be set as dynamic configs
    - If active value != desired value (from server.properties), set as dynamic config

    Removing configs:
    - If a config has a dynamic value set, but the active static value == desired value,
      remove the dynamic config (since static is correct)
    - Don't remove dynamic configs that come from emergency configs

    Args:
        admin_client: AdminClient instance
        record_dir: Path to the directory containing emergency configs
        properties_file: Path to the server.properties file
        dry_run: Whether to dry run the config changes, only performs validation
    """
    emergency_configs = read_record_dir(record_dir)

    properties_configs = read_server_properties(properties_file)
    # TODO: use node.id if we move to KRaft
    broker_id = int(properties_configs["broker.id"])

    kafka_configs_list: list[Config] = get_active_broker_configs(
        broker_id, sasl_credentials_file=sasl_credentials_file
    )
    kafka_configs: dict[str, Config] = {config.config_name: config for config in kafka_configs_list}

    configs_to_apply: dict[str, str] = {}

    for config_name, config_value in emergency_configs.items():
        # Skip if dynamic value already matches the emergency config
        if config_name in kafka_configs:
            active_config = kafka_configs[config_name]
            # For sensitive configs, we can't reliably compare values (they're masked),
            # so always apply them
            if (
                not active_config.is_sensitive
                and active_config.dynamic_value is not None
                and active_config.dynamic_value == config_value
            ):
                continue
        configs_to_apply[config_name] = config_value

    for config_name, desired_value in properties_configs.items():
        if config_name in emergency_configs:
            continue

        if config_name not in kafka_configs:
            configs_to_apply[config_name] = desired_value
            continue

        active_config = kafka_configs[config_name]

        # If there's a dynamic value set, but the static value matches the desired value,
        # we should remove the dynamic config instead of applying a new value
        if (
            not active_config.is_sensitive
            and active_config.dynamic_value is not None
            and active_config.static_value is not None
            and active_config.static_value == desired_value
        ):
            continue

        # For sensitive configs, always apply since we can't compare masked values
        # For non-sensitive configs, only apply if values differ
        if active_config.is_sensitive or active_config.active_value != desired_value:
            configs_to_apply[config_name] = desired_value

    configs_to_remove: list[str] = []

    for config_name, active_config in kafka_configs.items():
        if active_config.dynamic_value is None:
            continue

        if config_name in emergency_configs:
            continue

        if config_name not in properties_configs:
            continue

        # For sensitive configs, we can't reliably compare values (they're masked),
        # so skip removal logic
        if active_config.is_sensitive:
            continue

        desired_value = properties_configs[config_name]

        if active_config.static_value is not None and active_config.static_value == desired_value:
            configs_to_remove.append(config_name)

    apply_success: list[dict[str, Any]] = []
    apply_errors: list[dict[str, Any]] = []
    remove_success: list[dict[str, Any]] = []
    remove_errors: list[dict[str, Any]] = []

    if configs_to_apply:
        apply_success, apply_errors = apply_configs(
            admin_client, configs_to_apply, [str(broker_id)], dry_run=dry_run
        )

    if configs_to_remove:
        remove_success, remove_errors = remove_dynamic_configs(
            admin_client, configs_to_remove, [str(broker_id)], dry_run=dry_run
        )

    return apply_success + remove_success, apply_errors + remove_errors
