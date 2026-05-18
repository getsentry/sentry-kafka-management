from __future__ import annotations

import time
from dataclasses import dataclass

from confluent_kafka import (  # type: ignore[import-untyped]
    OFFSET_INVALID,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
    ConsumerGroupListing,
)

from sentry_kafka_management.actions.latency.metrics import (
    MetricsBackend,
    emit_topic_consumer_latency,
)
from sentry_kafka_management.brokers import ClusterConfig, YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.connectors.kafka_config import build_broker_config


@dataclass
class TopicConsumerLatency:
    cluster_name: str
    topic_name: str
    group_id: str
    latency_ms: float


def list_consumer_group_ids(admin: AdminClient) -> list[str]:
    """Get all consumer group IDs on the cluster."""
    result = admin.list_consumer_groups(request_timeout=10.0).result()

    errors: list[KafkaException] = result.errors
    valid: list[ConsumerGroupListing] = result.valid

    for error in errors:
        raise error

    group_ids: list[str] = []

    for listing in valid:
        group_ids.append(listing.group_id)

    return group_ids


def get_committed_offsets(admin: AdminClient, group_id: str) -> list[TopicPartition]:
    """Get the committed offsets for a consumer group."""
    result: ConsumerGroupTopicPartitions = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group_id)]
    )[group_id].result()

    committed: list[TopicPartition] = []

    for partition in result.topic_partitions:
        if partition.error is None:
            committed.append(partition)

    return committed


def read_timestamp_ms(consumer: Consumer, topic: str, partition: int, offset: int) -> int:
    """Read the timestamp of a message from a Kafka topic."""
    consumer.assign([TopicPartition(topic, partition, offset)])
    deadline = time.monotonic() + 10.0

    while time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        error = msg.error()
        if error is not None:
            raise KafkaException(error)

        ts_type, ts_ms = msg.timestamp()

        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            raise ValueError(
                f"Timestamp not available for {topic}[{partition}] at offset {offset}"
            )

        if ts_ms < 0:
            raise ValueError(
                f"Invalid timestamp {ts_ms} for {topic}[{partition}] at offset {offset}"
            )

        return int(ts_ms)

    raise TimeoutError(
        f"Timed out reading timestamp for {topic}[{partition}] at offset {offset}"
    )


def get_partition_latency(
    consumer: Consumer, topic: str, partition: int, committed_offset: int
) -> float:
    """Get the latency of a partition."""

    low, high = consumer.get_watermark_offsets(
        TopicPartition(topic, partition), timeout=10.0, cached=False
    )

    if high == OFFSET_INVALID:
        raise ValueError(f"No valid high watermark for {topic}[{partition}]")

    if high == low or committed_offset >= high:
        return 0.0 # Empty partition or caught up

    # Committed offset can age out of retention (committed_offset < low),
    # so clamp to low and use the oldest message in the partition.
    measured_offset = max(committed_offset, low)

    ts_ms = read_timestamp_ms(consumer, topic, partition, measured_offset)

    now_ms = int(time.time() * 1000)
    return float(now_ms - ts_ms)


def get_cluster_latency(
    cluster_name: str, config: ClusterConfig, topics: list[str]
) -> list[TopicConsumerLatency]:
    consumer_group_id = f"consumer-latency-group-{cluster_name}"

    admin = get_admin_client(config)

    consumer = Consumer(
        {
            "enable.auto.commit": False,
            "group.id": consumer_group_id,
            **build_broker_config(config),
        }
    )

    checked_topics = set(topics)
    scans: list[TopicConsumerLatency] = []

    try:
        group_ids = list_consumer_group_ids(admin)
        for group_id in group_ids:
            if group_id == consumer_group_id:
                continue

            partitions_by_topic: dict[str, list[tuple[int, int]]] = {}

            for tp in get_committed_offsets(admin, group_id):
                if tp.topic in checked_topics:
                    partitions_by_topic.setdefault(tp.topic, []).append(
                        (tp.partition, int(tp.offset))
                    )

            for topic, partitions in partitions_by_topic.items():
                latency_ms = 0.0
                has_topic_latency = False

                for partition, committed_offset in partitions:
                    partition_latency_ms = get_partition_latency(
                        consumer,
                        topic,
                        partition,
                        committed_offset,
                    )

                    latency_ms = max(latency_ms, partition_latency_ms)
                    has_topic_latency = True

                if not has_topic_latency:
                    continue

                scans.append(
                    TopicConsumerLatency(
                        cluster_name=cluster_name,
                        group_id=group_id,
                        topic_name=topic,
                        latency_ms=latency_ms,
                    )
                )
    finally:
        consumer.close()

    return scans


def run_latency_metrics(
    config: YamlKafkaConfig,
    metrics: MetricsBackend,
) -> None:
    for cluster_name, cluster_config in config.get_clusters().items():
        topics = list(config.get_topics_config(cluster_name))
        for scan in get_cluster_latency(cluster_name, cluster_config, topics):
            emit_topic_consumer_latency(metrics, scan)
