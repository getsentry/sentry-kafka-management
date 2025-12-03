from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Sequence

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    ConfigSource,
)

from sentry_kafka_management.actions.clusters import describe_cluster
from sentry_kafka_management.actions.conf import KAFKA_TIMEOUT


@dataclass
class ConfigChange:
    broker_id: str
    config_name: str | None = None
    old_value: str | None = None
    new_value: str | None = None

    def to_success(self) -> dict[str, Any]:
        return {
            "broker_id": self.broker_id,
            "config_name": self.config_name,
            "status": "success",
            "old_value": self.old_value,
            "new_value": self.new_value,
        }

    def to_error(self, error_message: str) -> dict[str, Any]:
        return {
            "broker_id": self.broker_id,
            "config_name": self.config_name,
            "status": "error",
            "error": error_message,
        }


def describe_broker_configs(
    admin_client: AdminClient,
) -> Sequence[Mapping[str, Any]]:
    """
    Returns configuration for all brokers in a cluster.

    The source field represents whether the config value was set statically or dynamically.
    For the complete list of possible enum values see
    https://github.com/confluentinc/confluent-kafka-python/blob/55b55550acabc51cb75c7ac78190d6db71706690/src/confluent_kafka/admin/_config.py#L47-L59
    """
    broker_resources = [
        ConfigResource(ConfigResource.Type.BROKER, f"{id}")
        for id in admin_client.list_topics().brokers
    ]

    all_configs = []

    for broker_resource in broker_resources:
        configs = {
            k: v.result(KAFKA_TIMEOUT)
            for (k, v) in admin_client.describe_configs([broker_resource]).items()
        }[broker_resource]

        for k, v in configs.items():
            # the confluent library returns the raw int value of the enum instead of a
            # ConfigSource object, so we have to convert it back into a ConfigSource
            source_enum = ConfigSource(v.source) if isinstance(v.source, int) else v.source
            config_item = {
                "config": k,
                "value": v.value,
                "isDefault": v.is_default,
                "isReadOnly": v.is_read_only,
                "source": source_enum.name,
                "broker": broker_resource.name,
            }
            all_configs.append(config_item)

    return all_configs


def record_config(config_name: str, config_value: str, record_dir: Path) -> None:
    """
    Takes a mapping of config names to config values.
    Records each of these in the dir specified by `record_dir`,
    creating files with names equal to the config names, each containing the config's value.
    Will overwrite any existing files.

    Args:
        config_name: Name of the config.
        config_value: Value of the config.
        record_dir: Directory to record configs in.
    """
    assert record_dir.is_dir(), "record_dir must be a directory."
    try:
        with open(record_dir / config_name, "w") as f:
            f.write(config_value)
    except Exception as e:
        print(f"Error recording config {config_name}: {e}")


def _get_config_from_list(
    config_list: Sequence[Mapping[str, Any]],
    config_name: str,
    broker_id: str,
) -> Mapping[str, Any] | None:
    """
    Helper function for finding a config's status on a specific broker
    from a list of all configs across all brokers.
    """
    return next(
        (
            config
            for config in config_list
            if config["config"] == config_name and config["broker"] == broker_id
        ),
        None,
    )


def _update_configs(
    admin_client: AdminClient,
    config_changes_dict: dict[str, list[ConfigChange]],
    update_type: AlterConfigOpType,
    configs_record_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Performs the given update operation on the given brokers
    for the given config changes.
    """
    success: list[dict[str, Any]] = []
    error: list[dict[str, Any]] = []

    config_resources: dict[str, ConfigResource] = {}

    for broker_id, config_changes in config_changes_dict.items():
        for config_change in config_changes:
            if broker_id not in config_resources:
                config_resources[broker_id] = ConfigResource(
                    restype=ConfigResource.Type.BROKER,
                    name=broker_id,
                )
            config_entry = ConfigEntry(
                name=config_change.config_name,
                value=config_change.new_value,
                incremental_operation=update_type,
            )
            config_resources[broker_id].incremental_configs.append(config_entry)

    futures = admin_client.incremental_alter_configs(list(config_resources.values()))
    for config_resource, future in futures.items():
        try:
            future.result(timeout=KAFKA_TIMEOUT)
            for config_change in config_changes_dict[config_resource.name]:
                success.append(config_change.to_success())
                if configs_record_dir and config_change.config_name and config_change.new_value:
                    record_config(
                        config_change.config_name,
                        config_change.new_value,
                        configs_record_dir,
                    )
        except Exception as e:
            error.extend([config_change.to_error(str(e))])
            for config_change in config_changes_dict[config_resource.name]:
                if config_change.config_name and config_change.config_name not in str(e):
                    success.append(config_change.to_success())
                    if configs_record_dir and config_change.config_name and config_change.new_value:
                        record_config(
                            config_change.config_name,
                            config_change.new_value,
                            configs_record_dir,
                        )
    return success, error


def apply_configs(
    admin_client: AdminClient,
    config_changes: MutableMapping[str, str],
    broker_ids: Sequence[str] | None = None,
    configs_record_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Apply a configuration change to a broker.

    Args:
        admin_client: AdminClient instance
        config_changes: Dictionary of config changes to apply
        broker_ids: List of broker IDs to apply config to, if not provided, config will \
            be applied to all brokers in the cluster.

    Returns:
        List of dictionaries with operation details for each config change.
        Each dict contains: `broker_id`, `config_name`, `status`, and either the pair \
        `old_value`, `new_value` if successful or an `error` if unsuccessful.
    """
    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    # validate configs
    config_changes_dict: dict[str, list[ConfigChange]] = {}
    validation_errors: list[dict[str, Any]] = []
    non_valid_configs: set[str] = set()
    current_configs = describe_broker_configs(admin_client)
    for broker_id in broker_ids:
        for config_name, new_value in config_changes.items():
            current_config = _get_config_from_list(
                current_configs,
                config_name,
                broker_id,
            )

            # validate config exists
            if current_config is None:
                validation_errors.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Config '{config_name}' not found on broker {broker_id}"
                    )
                )
                non_valid_configs.add(config_name)
                continue
            # validate config is not read-only when setting
            if current_config["isReadOnly"]:
                validation_errors.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Config '{config_name}' is read-only on broker {broker_id}"
                    )
                )
                non_valid_configs.add(config_name)
                continue
            if broker_id not in config_changes_dict:
                config_changes_dict[broker_id] = []
            config_changes_dict[broker_id].append(
                ConfigChange(
                    broker_id=broker_id,
                    config_name=config_name,
                    old_value=current_config["value"],
                    new_value=new_value,
                )
            )
    for config in non_valid_configs:
        del config_changes[config]

    success, errors = _update_configs(
        admin_client=admin_client,
        config_changes_dict=config_changes_dict,
        update_type=AlterConfigOpType.SET,
        configs_record_dir=configs_record_dir,
    )

    return success, errors + validation_errors


def remove_dynamic_configs(
    admin_client: AdminClient,
    configs_to_remove: Sequence[str],
    broker_ids: Sequence[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Removes any dynamically set values from the given configs
    and switches them back to using either:
    * the static value defined in `server.properties`, if one exists
    * the config default value, if there's no static value defined for it

    Args:
        admin_client: AdminClient instance
        configs_to_remove: List of config changes to remove dynamic configs from
        broker_ids: List of broker IDs to remove the given dynamic configs from

    Returns:
        List of dictionaries with details on each config change.
        Each dict contains: `broker_id`, `config_name`, `status`, `old_value`, and either
        a `new_value` (which will be `None`) or an `error`.
        If the status is "error", `error` will be a string describing the error.
    """
    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    # validate configs
    config_changes_dict: dict[str, list[ConfigChange]] = {}
    valid_broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]
    validation_errors: list[dict[str, Any]] = []
    non_valid_configs: set[str] = set()
    current_configs = describe_broker_configs(admin_client)
    for broker_id in broker_ids:
        for config_name in configs_to_remove:
            current_config = _get_config_from_list(
                current_configs,
                config_name,
                broker_id,
            )

            # validate broker exists
            if broker_id not in valid_broker_ids:
                validation_errors.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Broker {broker_id} not found in cluster"
                    )
                )
                non_valid_configs.add(broker_id)
                continue

            # validate config exists
            if current_config is None:
                validation_errors.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Config '{config_name}' not found on broker {broker_id}"
                    )
                )
                non_valid_configs.add(config_name)
                continue
            # validate config is dynamic before removing
            if current_config["source"] != ConfigSource.DYNAMIC_BROKER_CONFIG.name:
                validation_errors.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Config '{config_name}' is not set dynamically on broker {broker_id}"
                    )
                )
                non_valid_configs.add(config_name)
                continue
            if broker_id not in config_changes_dict:
                config_changes_dict[broker_id] = []
            config_changes_dict[broker_id].append(
                ConfigChange(
                    broker_id=broker_id,
                    config_name=config_name,
                    old_value=current_config["value"],
                    new_value=None,
                )
            )

    success, error = _update_configs(
        admin_client=admin_client,
        config_changes_dict=config_changes_dict,
        update_type=AlterConfigOpType.DELETE,
    )

    return success, error + validation_errors
