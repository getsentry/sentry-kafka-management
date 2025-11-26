from dataclasses import dataclass
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
    config_name: str
    old_value: str | None = None
    new_value: str | None = None

    def to_sucesss(self) -> dict[str, Any]:
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
    config_changes: Mapping[str, str | None],
    update_type: AlterConfigOpType,
    broker_ids: Sequence[str],
    current_configs: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Performs the given update operation on the given brokers
    for the given config changes.
    """
    success: list[dict[str, Any]] = []
    error: list[dict[str, Any]] = []

    config_resources: list[ConfigResource] = []
    config_map: dict[ConfigResource, ConfigChange] = {}

    for broker_id in broker_ids:
        for config_name, new_value in config_changes.items():
            current_config = _get_config_from_list(
                current_configs,
                config_name,
                broker_id,
            )

            config_entry = ConfigEntry(
                name=config_name,
                value=new_value,
                incremental_operation=update_type,
            )

            config_resource = ConfigResource(
                restype=ConfigResource.Type.BROKER,
                name=broker_id,
                incremental_configs=[config_entry],
            )
            config_resources.append(config_resource)
            config_map[config_resource] = ConfigChange(
                broker_id,
                config_name,
                old_value=current_config.get("value") if current_config else None,
                new_value=new_value,
            )

    if config_resources:
        futures = admin_client.incremental_alter_configs(config_resources)

        for resource, future in futures.items():
            config_change = config_map[resource]
            try:
                future.result(timeout=KAFKA_TIMEOUT)
                success.append(config_change.to_sucesss())
            except Exception as e:
                error.append(config_change.to_error(str(e)))
    return success, error


def apply_configs(
    admin_client: AdminClient,
    config_changes: MutableMapping[str, str],
    broker_ids: Sequence[str] | None = None,
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
    validation_errors: list[dict[str, Any]] = []
    non_valid_configs: set[str] = set()
    current_configs = describe_broker_configs(admin_client)
    for broker_id in broker_ids:
        for config_name in config_changes:
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
    for config in non_valid_configs:
        del config_changes[config]

    success, errors = _update_configs(
        admin_client=admin_client,
        config_changes=config_changes,
        update_type=AlterConfigOpType.SET,
        broker_ids=broker_ids,
        current_configs=current_configs,
    )

    return success, errors + validation_errors
