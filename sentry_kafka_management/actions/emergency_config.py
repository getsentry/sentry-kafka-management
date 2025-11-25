import logging
from typing import Any

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
)

from sentry_kafka_management.actions.brokers import describe_broker_configs
from sentry_kafka_management.actions.clusters import describe_cluster
from sentry_kafka_management.actions.conf import KAFKA_TIMEOUT

logger = logging.getLogger(__name__)


def apply_emergency_config(
    admin_client: AdminClient,
    config_changes: dict[str, str],
    broker_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Apply an emergency configuration change to a broker.

    Args:
        admin_client: AdminClient instance
        config_changes: Dictionary of config changes to apply
        broker_ids: List of broker IDs to apply config to

    Returns:
        List of dictionaries with operation details for each config change.
        Each dict contains: `broker_id`, `config_name`, `status`, `old_value`, and either
        a `new_value` or an `error`.
        If the status is "error", `error` will be a string describing the error.
        If the status is "success", `new_value` will be the new value of the config.
    """

    if broker_ids is None:
        broker_ids = [broker["id"] for broker in describe_cluster(admin_client)]

    results: list[dict[str, Any]] = []

    current_configs = describe_broker_configs(admin_client)

    config_resources: list[ConfigResource] = []
    config_map: dict[ConfigResource, tuple[str, str, str, str]] = {}

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
                results.append(
                    {
                        "broker_id": broker_id,
                        "config_name": config_name,
                        "status": "error",
                        "error": f"Config '{config_name}' not found on broker {broker_id}",
                    }
                )
                continue

            # validate config is not read only
            if current_config["isReadOnly"]:
                results.append(
                    {
                        "broker_id": broker_id,
                        "config_name": config_name,
                        "status": "error",
                        "error": f"Config '{config_name}' is read only on broker {broker_id}",
                    }
                )
                continue

            old_value = current_config["value"]

            config_entry = ConfigEntry(
                name=config_name,
                value=new_value,
                incremental_operation=AlterConfigOpType.SET,
            )

            config_resource = ConfigResource(
                restype=ConfigResource.Type.BROKER,
                name=broker_id,
                incremental_configs=[config_entry],
            )
            config_resources.append(config_resource)
            config_map[config_resource] = (broker_id, config_name, old_value, new_value)

    if config_resources:
        futures = admin_client.incremental_alter_configs(config_resources)

        for resource, future in futures.items():
            broker_id, config_name, old_value, new_value = config_map[resource]
            try:
                future.result(timeout=KAFKA_TIMEOUT)
                results.append(
                    {
                        "broker_id": broker_id,
                        "config_name": config_name,
                        "status": "success",
                        "old_value": old_value,
                        "new_value": new_value,
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "broker_id": broker_id,
                        "config_name": config_name,
                        "status": "error",
                        "old_value": old_value,
                        "error": str(e),
                    }
                )

    return results
