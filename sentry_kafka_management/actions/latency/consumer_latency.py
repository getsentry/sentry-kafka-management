from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Mapping

from confluent_kafka import (  # type: ignore[import-untyped]
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
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
from sentry_kafka_management.brokers import ClusterConfig, TopicConfig, YamlKafkaConfig
from sentry_kafka_management.connectors.admin import get_admin_client
from sentry_kafka_management.connectors.kafka_config import build_broker_config

logger = logging.getLogger(__name__)

RETRYABLE_ERRORS = frozenset(
    {
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError.NOT_COORDINATOR,
        KafkaError._WAIT_COORD,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    }
)


@dataclass
class TopicConsumerLatency:
    cluster_name: str
    topic_name: str
    group_id: str
    latency_ms: float
    partition: int


@dataclass
class ConsumerLatencyResult:
    scans: list[TopicConsumerLatency]
    errors: list[Exception] | None = None


class ConsumerGroupListingError(Exception):
    """
    Raised when listing consumer groups returns one or more errors.
    """

    def __init__(
        self,
        errors: list[KafkaException],
        valid: list[ConsumerGroupListing] | None = None,
    ) -> None:
        formatted = "; ".join(str(error) for error in errors)
        super().__init__(f"Failed to list consumer groups ({len(errors)} error(s)): {formatted}")
        self.errors = errors
        self.valid = valid or []


def list_consumer_group_ids(admin: AdminClient) -> list[str]:
    """Get all consumer group IDs on the cluster."""
    result = admin.list_consumer_groups().result()

    errors: list[KafkaException] = result.errors
    valid: list[ConsumerGroupListing] = result.valid

    group_ids: list[str] = [listing.group_id for listing in valid]

    if errors:
        raise ConsumerGroupListingError(errors, valid=valid)

    return group_ids


def get_committed_offsets(admin: AdminClient, group_id: str) -> list[TopicPartition]:
    """Get the committed offsets for a consumer group."""
    result: ConsumerGroupTopicPartitions = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group_id)]
    )[group_id].result()

    committed: list[TopicPartition] = []

    for partition in result.topic_partitions:
        if partition.error is not None:
            raise KafkaException(partition.error)
        committed.append(partition)

    return committed


def read_timestamp_ms(
    consumer: Consumer,
    topic: str,
    partition: int,
    offset: int,
    timeout: int,
) -> int:
    """Read the timestamp of a message from a Kafka topic."""
    consumer.assign([TopicPartition(topic, partition, offset)])
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        error = msg.error()

        if error is not None:
            if error.code() not in RETRYABLE_ERRORS:
                raise KafkaException(error)
            else:
                continue

        ts_type, ts_ms = msg.timestamp()

        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            raise ValueError(f"Timestamp not available for {topic}[{partition}] at offset {offset}")

        if ts_ms < 0:
            raise ValueError(
                f"Invalid timestamp {ts_ms} for {topic}[{partition}] at offset {offset}"
            )

        logger.info(
            "Read message timestamp for topic=%s partition=%s offset=%s: ts_type=%s ts_ms=%s",
            topic,
            partition,
            offset,
            ts_type,
            ts_ms,
        )
        return int(ts_ms)

    raise TimeoutError(f"Timed out reading timestamp for {topic}[{partition}] at offset {offset}")


def get_partition_latency(
    consumer: Consumer,
    topic: str,
    partition: int,
    committed_offset: int,
    retention_ms: int,
    timeout: int,
) -> float:
    """Get the latency of a partition."""

    logger.info(
        "Collecting latency for topic=%s partition=%s committed_offset=%s",
        topic,
        partition,
        committed_offset,
    )

    low, high = consumer.get_watermark_offsets(
        TopicPartition(topic, partition), timeout=timeout, cached=False
    )

    logger.info(
        "Watermarks for topic=%s partition=%s: low=%s high=%s committed_offset=%s",
        topic,
        partition,
        low,
        high,
        committed_offset,
    )

    if high <= committed_offset:
        return 0.0  # Caught up regardless of retention

    if committed_offset < low:
        return float(retention_ms)  # Aged out of retention

    poll_start = time.monotonic()
    ts_ms = read_timestamp_ms(consumer, topic, partition, committed_offset, timeout)
    poll_ms = (time.monotonic() - poll_start) * 1000.0

    logger.info(
        "Polled message for topic=%s partition=%s in poll_ms=%.1f",
        topic,
        partition,
        poll_ms,
    )

    now_ms = int(time.time() * 1000)
    latency_ms = float(now_ms - ts_ms)

    logger.info(
        "Computed latency for topic=%s partition=%s: now_ms=%s ts_ms=%s latency_ms=%.1f",
        topic,
        partition,
        now_ms,
        ts_ms,
        latency_ms,
    )

    return latency_ms


def get_cluster_latency(
    cluster_name: str,
    config: ClusterConfig,
    topics: Mapping[str, TopicConfig],
    timeout: int,
) -> ConsumerLatencyResult:
    consumer_group_id = f"consumer-latency-group-{cluster_name}"
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []
    consumer: Consumer | None = None

    retentions_by_topic: dict[str, int] = {}

    for topic_name, topic_config in topics.items():
        retention_ms = topic_config["settings"].get("retention.ms")
        if retention_ms is None:
            retentions_by_topic[topic_name] = 604800000
        else:
            retentions_by_topic[topic_name] = int(retention_ms)

    try:
        admin = get_admin_client(config)
        consumer = Consumer(
            {
                "enable.auto.commit": False,
                "group.id": consumer_group_id,
                **build_broker_config(config),
            }
        )

        try:
            group_ids = list_consumer_group_ids(admin)
        except ConsumerGroupListingError as e:
            errors.extend(e.errors)
            group_ids = [listing.group_id for listing in e.valid]

        for group_id in group_ids:
            if group_id == consumer_group_id:
                continue

            try:
                committed_offsets = get_committed_offsets(admin, group_id)
            except Exception as e:
                errors.append(e)
                continue

            for tp in committed_offsets:
                if tp.topic not in retentions_by_topic:
                    continue

                retention_ms = retentions_by_topic[tp.topic]
                try:
                    latency_ms = get_partition_latency(
                        consumer,
                        tp.topic,
                        tp.partition,
                        int(tp.offset),
                        retention_ms,
                        timeout,
                    )
                except Exception as e:
                    errors.append(e)
                    continue

                scans.append(
                    TopicConsumerLatency(
                        cluster_name=cluster_name,
                        group_id=group_id,
                        topic_name=tp.topic,
                        latency_ms=latency_ms,
                        partition=tp.partition,
                    )
                )
    except Exception as e:
        errors.append(e)
    finally:
        if consumer is not None:
            consumer.close()

    return ConsumerLatencyResult(scans=scans, errors=errors or None)


def record_consumer_group_latency(
    config: YamlKafkaConfig,
    metrics: MetricsBackend,
    timeout: int = 10,
) -> ConsumerLatencyResult:
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []

    for cluster_name, cluster_config in config.get_clusters().items():
        topics = config.get_topics_config(cluster_name)
        result = get_cluster_latency(cluster_name, cluster_config, topics, timeout)

        for scan in result.scans:
            emit_topic_consumer_latency(metrics, scan)
            scans.append(scan)

        if result.errors:
            errors.extend(result.errors)

    return ConsumerLatencyResult(scans=scans, errors=errors or None)
