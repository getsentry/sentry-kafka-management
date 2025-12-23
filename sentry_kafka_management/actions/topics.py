from typing import Any, Mapping, Sequence

from confluent_kafka import (  # type: ignore[import-untyped]
    KafkaException,
    TopicCollection,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    ConfigResource,
    ConfigSource,
    OffsetSpec,
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


def list_offsets(admin_client: AdminClient, topic: str) -> list[dict[str, Any]]:
    """
    Returns the earliest and latest stored offsets for every partition of a topic.
    """
    try:
        topics = admin_client.describe_topics(TopicCollection([topic]))
        topic_description = topics[topic].result(KAFKA_TIMEOUT)
    except KafkaException as e:
        raise ValueError(f"Topic '{topic}' does not exist or cannot be accessed") from e

    topic_partitions = [TopicPartition(topic, p.id) for p in topic_description.partitions]

    earliest_offsets = admin_client.list_offsets(
        {tp: OffsetSpec.earliest() for tp in topic_partitions}
    )

    latest_offsets = admin_client.list_offsets({tp: OffsetSpec.latest() for tp in topic_partitions})

    result = []

    for tp in topic_partitions:
        try:
            earliest_offset = earliest_offsets[tp].result(KAFKA_TIMEOUT).offset
            latest_offset = latest_offsets[tp].result(KAFKA_TIMEOUT).offset
        except KafkaException as e:
            raise ValueError(f"Failed to retrieve offsets for topic '{topic}'") from e

        result.append(
            {
                "topic": tp.topic,
                "partition": tp.partition,
                "earliest_offset": earliest_offset,
                "latest_offset": latest_offset,
            }
        )

    return result
