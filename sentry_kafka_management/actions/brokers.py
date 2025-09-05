from typing import Any

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConfigResource,
)

from sentry_kafka_management.brokers import ClusterConfig
from sentry_kafka_management.connectors.admin import get_admin_client

KAFKA_TIMEOUT = 5


def describe_broker_configs(
    kafka_config: ClusterConfig,
) -> list[dict[str, Any]]:
    """
    Returns configuration for all brokers in a cluster.
    """
    admin_client = get_admin_client(kafka_config)
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
