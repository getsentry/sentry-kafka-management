from dataclasses import dataclass
from typing import Any, Mapping, Sequence, cast

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


def _update_configs(
    admin_client: AdminClient,
    config_changes: Mapping[str, str | None],
    update_type: AlterConfigOpType,
    broker_ids: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Performs the given update operation on the given brokers
    for the given config changes.
    """
    success: list[dict[str, Any]] = []
    error: list[dict[str, Any]] = []

    current_configs = describe_broker_configs(admin_client)

    config_resources: list[ConfigResource] = []
    config_map: dict[ConfigResource, ConfigChange] = {}

    for broker_id in broker_ids:
        for config_name, new_value in config_changes.items():
            current_config = next(
                (
                    config
                    for config in current_configs
                    if config["config"] == config_name and config["broker"] == broker_id
                ),
                None,
            )

            # validate config exists
            if current_config is None:
                error.append(
                    ConfigChange(broker_id, config_name).to_error(
                        f"Config '{config_name}' not found on broker {broker_id}"
                    )
                )
                continue

            if update_type is AlterConfigOpType.SET:
                # validate config is not read only when setting
                if current_config["isReadOnly"]:
                    error.append(
                        ConfigChange(broker_id, config_name).to_error(
                            f"Config '{config_name}' is read-only on broker {broker_id}"
                        )
                    )
                    continue
            if update_type is AlterConfigOpType.DELETE:
                # validate config is set dynamically when deleting
                if current_config["source"] is not ConfigSource.DYNAMIC_BROKER_CONFIG.name:
                    error.append(
                        ConfigChange(broker_id, config_name).to_error(
                            f"Config '{config_name}' is not set dynamically on broker {broker_id}"
                        )
                    )
                    continue

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
                old_value=current_config["value"] if current_config["value"] else None,
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
    config_changes: Mapping[str, str],
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

    cast(dict[str, str | None], config_changes)

    return _update_configs(
        admin_client=admin_client,
        config_changes=config_changes,
        update_type=AlterConfigOpType.SET,
        broker_ids=broker_ids,
    )


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
        config_changes: List of config changes to remove dynamic configs from
        broker_ids: List of broker IDs to remove the given dynamic configs from

    Returns:
        List of dictionaries with details on each config change.
        Each dict contains: `broker_id`, `config_name`, `status`, `old_value`, and either
        a `new_value` or an `error`.
        If the status is "error", `error` will be a string describing the error.
    """
    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    # config values are ignored when deleting, so we set them to None
    config_changes = cast(Mapping[str, str | None], {key: None for key in configs_to_remove})

    return _update_configs(
        admin_client=admin_client,
        config_changes=config_changes,
        update_type=AlterConfigOpType.DELETE,
        broker_ids=broker_ids,
    )
