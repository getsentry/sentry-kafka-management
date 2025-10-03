from typing import Any, Mapping, Sequence

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    ConfigResource,
    ConfigSource,
)

from sentry_kafka_management.actions.conf import KAFKA_TIMEOUT


def list_topics(admin_client: AdminClient) -> list[str]:
    """
    List all topics in the given Kafka cluster.
    """
    # list_topics() returns TopicMetadata, we need to extract topic names
    topic_metadata = admin_client.list_topics()
    return list(topic_metadata.topics.keys())


def describe_topic_configs(
    admin_client: AdminClient,
) -> Sequence[Mapping[str, Any]]:
    """
    Returns configuration for all topics in a cluster.
    """
    topic_resources = [
        ConfigResource(ConfigResource.Type.TOPIC, f"{name}")
        for name in admin_client.list_topics().topics
    ]

    all_configs = []

    for topic_resource in topic_resources:
        configs = {
            k: v.result(KAFKA_TIMEOUT)
            for (k, v) in admin_client.describe_configs([topic_resource]).items()
        }[topic_resource]

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
                "topic": topic_resource.name,
            }
            all_configs.append(config_item)

    return all_configs
