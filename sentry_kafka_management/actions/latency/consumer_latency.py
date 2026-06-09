from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

DEFAULT_MAX_WORKERS = 128

RETRYABLE_ERRORS = frozenset(
    {
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError.NOT_COORDINATOR,
        KafkaError._WAIT_COORD,
        KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    }
)


@dataclass
class _PartitionScan:
    group_id: str
    topic: str
    partition: int
    committed_offset: int
    retention_ms: int


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


def get_committed_offsets(
    admin: AdminClient, group_ids: list[str]
) -> dict[str, list[TopicPartition] | Exception]:
    """Get committed offsets for one or more consumer groups."""
    results: dict[str, list[TopicPartition] | Exception] = {}

    futures = {}

    for group_id in group_ids:
        try:
            partitions = [ConsumerGroupTopicPartitions(group_id)]
            offsets = admin.list_consumer_group_offsets(partitions)
            futures[group_id] = offsets[group_id]
        except Exception as e:
            results[group_id] = e

    for group_id, future in futures.items():
        try:
            cgtp: ConsumerGroupTopicPartitions = future.result()
            committed: list[TopicPartition] = []
            for partition in cgtp.topic_partitions:
                if partition.error is not None:
                    raise KafkaException(partition.error)
                committed.append(partition)
            results[group_id] = committed
        except Exception as e:
            results[group_id] = e
    return results


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

    watermark_start = time.monotonic()
    low, high = consumer.get_watermark_offsets(
        TopicPartition(topic, partition), timeout=timeout, cached=False
    )
    watermark_ms = (time.monotonic() - watermark_start) * 1000.0

    logger.info(
        "Watermark lookup for topic=%s partition=%s took %.1fms",
        topic,
        partition,
        watermark_ms,
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
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> ConsumerLatencyResult:
    consumer_group_id = f"consumer-latency-group-{cluster_name}"
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []

    retentions_by_topic: dict[str, int] = {}

    for topic_name, topic_config in topics.items():
        retention_ms = topic_config["settings"].get("retention.ms")
        if retention_ms is None:
            retentions_by_topic[topic_name] = 604800000
        else:
            retentions_by_topic[topic_name] = int(retention_ms)

    consumer_config = {
        "queued.min.messages": 1,
        "queued.max.messages.kbytes": 1,
        "enable.auto.commit": False,
        "group.id": consumer_group_id,
        **build_broker_config(config),
    }

    t0 = time.monotonic()

    try:
        admin = get_admin_client(config)

        try:
            group_ids = list_consumer_group_ids(admin)
        except ConsumerGroupListingError as e:
            errors.extend(e.errors)
            group_ids = [listing.group_id for listing in e.valid]

        scan_group_ids = [g for g in group_ids if g != consumer_group_id]
        offsets_by_group = get_committed_offsets(admin, scan_group_ids)

        work: list[_PartitionScan] = []
        for group_id, committed_or_exc in offsets_by_group.items():
            if isinstance(committed_or_exc, Exception):
                errors.append(committed_or_exc)
                continue
            for tp in committed_or_exc:
                if tp.topic not in retentions_by_topic:
                    continue
                work.append(
                    _PartitionScan(
                        group_id=group_id,
                        topic=tp.topic,
                        partition=tp.partition,
                        committed_offset=int(tp.offset),
                        retention_ms=retentions_by_topic[tp.topic],
                    )
                )

        if work:
            worker_count = min(len(work), max_workers)

            consumers: list[Consumer] = []
            consumers_lock = threading.Lock()
            consumer_local = threading.local()

            def get_consumer() -> Consumer:
                consumer = getattr(consumer_local, "consumer", None)
                if consumer is None:
                    consumer = Consumer(dict(consumer_config))
                    consumer_local.consumer = consumer
                    with consumers_lock:
                        consumers.append(consumer)
                return consumer

            def scan(item: _PartitionScan) -> TopicConsumerLatency:
                latency_ms = get_partition_latency(
                    get_consumer(),
                    item.topic,
                    item.partition,
                    item.committed_offset,
                    item.retention_ms,
                    timeout,
                )
                return TopicConsumerLatency(
                    cluster_name=cluster_name,
                    group_id=item.group_id,
                    topic_name=item.topic,
                    latency_ms=latency_ms,
                    partition=item.partition,
                )

            try:
                with ThreadPoolExecutor(
                    max_workers=worker_count,
                    thread_name_prefix=f"latency-{cluster_name}",
                ) as executor:
                    futures = [executor.submit(scan, item) for item in work]
                    for future in as_completed(futures):
                        try:
                            scans.append(future.result())
                        except Exception as e:
                            errors.append(e)
            finally:
                if consumers:
                    with ThreadPoolExecutor(
                        max_workers=len(consumers),
                        thread_name_prefix=f"clr-close-{cluster_name}",
                    ) as close_executor:
                        close_futures = [
                            close_executor.submit(consumer.close) for consumer in consumers
                        ]
                        for close_future in as_completed(close_futures):
                            try:
                                close_future.result()
                            except Exception as e:
                                errors.append(e)
    except Exception as e:
        errors.append(e)

    logger.info(f"Time taken to scan cluster {cluster_name}: {time.monotonic() - t0:.3f}s")

    return ConsumerLatencyResult(scans=scans, errors=errors or None)


def record_consumer_group_latency(
    config: YamlKafkaConfig,
    metrics: MetricsBackend,
    timeout: int = 10,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> ConsumerLatencyResult:
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []
    clusters = config.get_clusters()

    for cluster_name, cluster_config in clusters.items():
        try:
            topics = config.get_topics_config(cluster_name)
            result = get_cluster_latency(cluster_name, cluster_config, topics, timeout, max_workers)
        except Exception as e:
            errors.append(e)
            continue

        for scan in result.scans:
            emit_topic_consumer_latency(metrics, scan)
            scans.append(scan)
        if result.errors:
            errors.extend(result.errors)

    return ConsumerLatencyResult(scans=scans, errors=errors or None)
