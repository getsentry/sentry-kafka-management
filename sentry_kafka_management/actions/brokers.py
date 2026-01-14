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
from sentry_kafka_management.actions.conf import ALLOWED_CONFIGS, KAFKA_TIMEOUT
from sentry_kafka_management.actions.local.filesystem import record_config


@dataclass
class ConfigChange:
    broker_id: str
    config_name: str
    op: str
    from_value: str | None = None
    to_value: str | None = None
    is_sensitive: bool = False

    def to_success(self) -> dict[str, Any]:
        return {
            "broker_id": self.broker_id,
            "config_name": self.config_name,
            "is_sensitive": self.is_sensitive,
            "op": self.op,
            "status": "success",
            "from_value": "*****" if self.is_sensitive else self.from_value,
            "to_value": "*****" if self.is_sensitive else self.to_value,
        }

    def to_error(self, error_message: str) -> dict[str, Any]:
        return {
            "broker_id": self.broker_id,
            "config_name": self.config_name,
            "is_sensitive": self.is_sensitive,
            "op": self.op,
            "status": "error",
            "error": error_message,
            "from_value": "*****" if self.is_sensitive else self.from_value,
            "to_value": "*****" if self.is_sensitive else self.to_value,
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
                "isSensitive": v.is_sensitive,
                "source": source_enum.name,
                "broker": broker_resource.name,
            }
            all_configs.append(config_item)

    return all_configs


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
    config_changes: list[ConfigChange],
    update_type: AlterConfigOpType,
    configs_record_dir: Path | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Performs the given update operation on the given brokers
    for the given config changes.
    """
    success: list[dict[str, Any]] = []
    error: list[dict[str, Any]] = []

    change_list: list[tuple[ConfigChange, ConfigResource]] = []

    for config_change in config_changes:
        broker_id = config_change.broker_id
        config_entry = ConfigEntry(
            name=config_change.config_name,
            value=config_change.to_value,
            incremental_operation=update_type,
        )
        config_resource = ConfigResource(
            restype=ConfigResource.Type.BROKER,
            name=broker_id,
            incremental_configs=[config_entry],
        )
        change_list.append((config_change, config_resource))

    for config_change, config_resource in change_list:
        # we make an AdminClient request for each config change to get better error messages
        # since incremental_alter_configs returns None or throws a generic KafkaException
        # for all config changes if we batch them together
        futures = admin_client.incremental_alter_configs([config_resource])
        for _, future in futures.items():
            try:
                future.result(timeout=KAFKA_TIMEOUT)
                # record the applied value, if we applied a new value and it succeeded
                if configs_record_dir and config_change.config_name and config_change.to_value:
                    record_config(
                        config_change.config_name,
                        config_change.to_value,
                        configs_record_dir,
                    )
                success.append(config_change.to_success())
            except Exception as e:
                error.append(config_change.to_error(str(e)))
    return success, error


def apply_configs(
    admin_client: AdminClient,
    config_changes: MutableMapping[str, str],
    broker_ids: Sequence[str] | None = None,
    configs_record_dir: Path | None = None,
    dry_run: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Apply a configuration change to a broker.

    Args:
        admin_client: AdminClient instance
        config_changes: Dictionary of config changes to apply
        broker_ids: List of broker IDs to apply config to, if not provided, config will \
            be applied to all brokers in the cluster.
        configs_record_dir: Directory to record config changes in.
        dry_run: Whether to dry run the config changes, only performs validation

    Returns:
        List of dictionaries with operation details for each config change.
        Each dict contains: `broker_id`, `config_name`, `op`, `status`, `from_value`, \
        `to_value`, and `error` if unsuccessful.
    """
    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    # validate configs
    config_change_list: list[ConfigChange] = []
    validation_errors: list[dict[str, Any]] = []
    valid_broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]
    current_configs = describe_broker_configs(admin_client)
    for broker_id in broker_ids:
        for config_name, new_value in config_changes.items():
            current_config = _get_config_from_list(
                current_configs,
                config_name,
                broker_id,
            )
            if current_config:
                from_value = current_config["value"]
                is_sensitive = current_config["isSensitive"]
            else:
                from_value = None
                is_sensitive = is_likely_sensitive(config_name)

            # broker and config basic validation
            validate = basic_validation(broker_id, valid_broker_ids, config_name, current_config)
            if validate:
                validation_errors.append(
                    ConfigChange(
                        broker_id=broker_id,
                        config_name=config_name,
                        op="apply",
                        from_value=from_value,
                        to_value=new_value,
                        is_sensitive=is_sensitive,
                    ).to_error(validate)
                )
                continue
            if current_config is None:
                # Config doesn't exist on broker yet, but is in ALLOWED_CONFIGS.
                # Create a placeholder so we can proceed with setting it.
                current_config = {
                    "config": config_name,
                    "value": None,
                    "isReadOnly": False,
                    "isSensitive": is_sensitive,
                    "broker": broker_id,
                }
            if current_config["isReadOnly"]:
                validation_errors.append(
                    ConfigChange(
                        broker_id=broker_id,
                        config_name=config_name,
                        op="apply",
                        from_value=from_value,
                        to_value=new_value,
                        is_sensitive=is_sensitive,
                    ).to_error(f"Config '{config_name}' is read-only on broker {broker_id}")
                )
                continue
            config_change_list.append(
                ConfigChange(
                    broker_id=broker_id,
                    config_name=config_name,
                    op="apply",
                    from_value=current_config["value"],
                    to_value=new_value,
                    is_sensitive=current_config.get("isSensitive", False),
                )
            )

    if dry_run:
        return [c.to_success() for c in config_change_list], validation_errors

    success, errors = _update_configs(
        admin_client=admin_client,
        config_changes=config_change_list,
        update_type=AlterConfigOpType.SET,
        configs_record_dir=configs_record_dir,
    )

    return success, errors + validation_errors


def remove_dynamic_configs(
    admin_client: AdminClient,
    configs_to_remove: Sequence[str],
    broker_ids: Sequence[str] | None = None,
    dry_run: bool = False,
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
        dry_run: Whether to dry run the config removals, only performs validation

    Returns:
        List of dictionaries with details on each config change.
        Each dict contains: `broker_id`, `config_name`, `op`, `status`, `from_value`, \
        `to_value` (which will be `None` for remove operations), and `error` if unsuccessful.
    """
    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    # validate configs
    config_change_list: list[ConfigChange] = []
    valid_broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]
    validation_errors: list[dict[str, Any]] = []
    current_configs = describe_broker_configs(admin_client)
    for broker_id in broker_ids:
        for config_name in configs_to_remove:
            current_config = _get_config_from_list(
                current_configs,
                config_name,
                broker_id,
            )
            if current_config:
                from_value = current_config["value"]
                is_sensitive = current_config["isSensitive"]
            else:
                from_value = None
                is_sensitive = is_likely_sensitive(config_name)

            # broker and config basic validation
            validate = basic_validation(broker_id, valid_broker_ids, config_name, current_config)
            if validate:
                validation_errors.append(
                    ConfigChange(
                        broker_id=broker_id,
                        config_name=config_name,
                        op="remove",
                        from_value=from_value,
                        to_value=None,
                        is_sensitive=is_sensitive,
                    ).to_error(validate)
                )
                continue
            if current_config is None:
                # Config doesn't exist on broker so we can't remove it.
                validation_errors.append(
                    ConfigChange(
                        broker_id=broker_id,
                        config_name=config_name,
                        op="remove",
                        from_value=None,
                        to_value=None,
                        is_sensitive=is_sensitive,
                    ).to_error(f"Config '{config_name}' not found on broker {broker_id}")
                )
                continue
            if current_config["source"] != ConfigSource.DYNAMIC_BROKER_CONFIG.name:
                validation_errors.append(
                    ConfigChange(
                        broker_id=broker_id,
                        config_name=config_name,
                        op="remove",
                        from_value=from_value,
                        to_value=None,
                        is_sensitive=is_sensitive,
                    ).to_error(
                        f"Config '{config_name}' is not set dynamically on broker {broker_id}"
                    )
                )
                continue
            config_change_list.append(
                ConfigChange(
                    broker_id=broker_id,
                    config_name=config_name,
                    op="remove",
                    from_value=current_config["value"],
                    to_value=None,
                    is_sensitive=current_config.get("isSensitive", False),
                )
            )

    if dry_run:
        return [c.to_success() for c in config_change_list], validation_errors

    success, error = _update_configs(
        admin_client=admin_client,
        config_changes=config_change_list,
        update_type=AlterConfigOpType.DELETE,
    )

    return success, error + validation_errors


def basic_validation(
    broker_id: str,
    valid_broker_ids: Sequence[str],
    config_name: str,
    current_config: Mapping[str, Any] | None,
) -> str | None:
    """
    Performs basic validation of a broker and config.

    Allows modification if:
    - The config already exists on the broker, OR
    - The config is in ALLOWED_CONFIGS (for setting new configs)
    """
    if broker_id not in valid_broker_ids:
        return f"Broker {broker_id} not found in cluster"
    if current_config is None and config_name not in ALLOWED_CONFIGS:
        return (
            f"Config '{config_name}' does not exist on broker {broker_id} and is not in "
            "ALLOWED_CONFIGS. To set a config that doesn't exist yet, add it to "
            "ALLOWED_CONFIGS in sentry_kafka_management/actions/conf.py"
        )
    return None


def is_likely_sensitive(config_name: str) -> bool:
    """
    Determines if a config is likely to be sensitive.
    """
    sensitive_patterns = [
        "password",
        "secret",
        "key",
        "token",
        "credential",
        "truststore",
        "keystore",
        "sasl",
        "ssl",
        "jaas",
    ]
    return any(pattern in config_name.lower() for pattern in sensitive_patterns)
