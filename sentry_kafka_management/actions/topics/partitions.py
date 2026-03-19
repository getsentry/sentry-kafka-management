from typing import Sequence

from confluent_kafka import (  # type: ignore[import-untyped]
    ElectionType,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from sentry_kafka_management.actions.conf import KAFKA_TIMEOUT
from sentry_kafka_management.actions.topics.describe import describe_topic_partitions


def elect_partition_leaders(
    admin_client: AdminClient,
    topics: Sequence[str] = [],
    partitions: Sequence[TopicPartition] = [],
) -> tuple[list[dict[str, str | int]], list[dict[str, str | int]]]:
    """
    Triggers a leader election on the given cluster for the given topics/partitions.
    If neither `topics` or `partitions` are passed, will trigger an election for all partitions
    on the cluster.

    Returns a tuple containing a list of partition election success responses,
    and a list of election error responses.

    Args:
        admin_client: A Confluent admin client object.
        topics: Optional list of topics. If passed, leader election will be
                performed on all partitions in the given topics.
        partitions: Optional list of Confluent `TopicPartition` objects.
                    If passed, leader election will be performed on all given partitions.
    """
    if topics:
        topics_info = [describe_topic_partitions(admin_client, t) for t in topics]
        topic_partitions = [
            TopicPartition(topic=p["topic"], partition=p["id"])
            for topic in topics_info
            for p in topic
        ]
        partitions = list(partitions) + topic_partitions
    election = admin_client.elect_leaders(
        election_type=ElectionType.PREFERRED,
        partitions=partitions if partitions else None,
    )
    try:
        election_results = election.result(KAFKA_TIMEOUT)
    except KafkaException as e:
        raise ValueError("Failed to perform election for the given partitions.") from e

    success: list[dict[str, str | int]] = []
    errors: list[dict[str, str | int]] = []
    for p, err in election_results.items():
        # Confluent client returns an error when requesting election on partitions
        # that already have the correct leader
        if not err or err == KafkaError.ELECTION_NOT_NEEDED:
            success.append(
                {
                    "topic": p.topic,
                    "id": p.partition,
                }
            )
        else:
            # Error message is extracted differently for KafkaError vs KafkaException
            if isinstance(err, KafkaError):
                error_msg = err.str()
            else:
                error_msg = str(err)
            errors.append(
                {
                    "topic": p.topic,
                    "id": p.partition,
                    "error": error_msg,
                }
            )
    return (success, errors)
