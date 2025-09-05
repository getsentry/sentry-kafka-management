from typing import Any, Mapping, Sequence

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    ConfigResource,
)

KAFKA_TIMEOUT = 5


def describe_broker_configs(
    admin_client: AdminClient,
) -> Sequence[Mapping[str, Any]]:
    """
    Returns configuration for all brokers in a cluster.
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
            config_item = {
                "config": k,
                "value": v.value,
                "isDefault": v.is_default,
                "isReadOnly": v.is_read_only,
                "broker": broker_resource.name,
            }
            all_configs.append(config_item)

    return all_configs
