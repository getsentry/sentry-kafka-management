from __future__ import annotations

import logging
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
class PartitionScan:
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
    errors: list[Exception]

    def __init__(
        self,
        scans: list[TopicConsumerLatency],
        errors: list[Exception] | None = None,
    ) -> None:
        self.scans = scans
        self.errors = errors or []


class ConsumerGroupListingError(Exception):
    """
    Raised when listing consumer groups returns one or more errors.
    """

    def __init__(
        self,
        errors: list[KafkaException],
        valid: list[ConsumerGroupListing],
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


def read_committed_timestamps(
    consumer: Consumer,
    scans: list[PartitionScan],
    timeout: int,
) -> tuple[dict[tuple[str, int], int], list[Exception]]:
    """Read the message timestamp at each scan's committed offset."""
    timestamps: dict[tuple[str, int], int] = {}
    errors: list[Exception] = []

    pending = {(scan.topic, scan.partition) for scan in scans}
    consumer.assign(
        [TopicPartition(scan.topic, scan.partition, scan.committed_offset) for scan in scans]
    )

    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        key = (msg.topic(), msg.partition())
        if key not in pending:
            continue

        error = msg.error()
        if error is not None:
            if error.code() in RETRYABLE_ERRORS:
                continue
            errors.append(KafkaException(error))
            pending.discard(key)
            continue

        ts_type, ts_ms = msg.timestamp()
        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            errors.append(ValueError(f"Timestamp not available for {key[0]}[{key[1]}]"))
        elif ts_ms < 0:
            errors.append(ValueError(f"Invalid timestamp {ts_ms} for {key[0]}[{key[1]}]"))
        else:
            timestamps[key] = int(ts_ms)
        pending.discard(key)

    for topic, partition in pending:
        errors.append(TimeoutError(f"Timed out reading timestamp for {topic}[{partition}]"))

    return timestamps, errors


def get_consumer_group_latency(
    admin: AdminClient,
    consumer_config: Mapping[str, object],
    cluster_name: str,
    group_id: str,
    retentions_by_topic: Mapping[str, int],
    timeout: int,
) -> ConsumerLatencyResult:
    """
    Scan every partition for a single consumer group on one dedicated consumer.
    """
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []

    committed = get_committed_offsets(admin, [group_id])[group_id]
    if isinstance(committed, Exception):
        errors.append(committed)
        return ConsumerLatencyResult(scans=scans, errors=errors)

    group_scans = [
        PartitionScan(
            group_id=group_id,
            topic=tp.topic,
            partition=tp.partition,
            committed_offset=int(tp.offset),
            retention_ms=retentions_by_topic[tp.topic],
        )
        for tp in committed
        if tp.topic in retentions_by_topic
    ]

    if not group_scans:
        return ConsumerLatencyResult(scans=scans, errors=errors)

    consumer = Consumer(dict(consumer_config))
    try:
        to_read: list[PartitionScan] = []
        for item in group_scans:
            try:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(item.topic, item.partition), timeout=timeout, cached=False
                )
            except Exception as e:
                errors.append(e)
                continue

            if high <= item.committed_offset:
                latency_ms = 0.0  # Caught up regardless of retention
            elif item.committed_offset < low:
                latency_ms = float(item.retention_ms)  # Aged out of retention
            else:
                to_read.append(item)
                continue

            scans.append(
                TopicConsumerLatency(
                    cluster_name=cluster_name,
                    group_id=item.group_id,
                    topic_name=item.topic,
                    latency_ms=latency_ms,
                    partition=item.partition,
                )
            )

        if to_read:
            timestamps, read_errors = read_committed_timestamps(consumer, to_read, timeout)
            errors.extend(read_errors)
            now_ms = int(time.time() * 1000)
            for item in to_read:
                ts_ms = timestamps.get((item.topic, item.partition))
                if ts_ms is None:
                    continue
                scans.append(
                    TopicConsumerLatency(
                        cluster_name=cluster_name,
                        group_id=item.group_id,
                        topic_name=item.topic,
                        latency_ms=float(now_ms - ts_ms),
                        partition=item.partition,
                    )
                )
    finally:
        try:
            consumer.close()
        except Exception as e:
            errors.append(e)

    return ConsumerLatencyResult(scans=scans, errors=errors)


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
        "fetch.queue.backoff.ms": 10,
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

        if not scan_group_ids:
            return ConsumerLatencyResult(scans=scans, errors=errors)

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"latency-{cluster_name}",
        ) as executor:
            futures = [
                executor.submit(
                    get_consumer_group_latency,
                    admin,
                    consumer_config,
                    cluster_name,
                    group_id,
                    retentions_by_topic,
                    timeout,
                )
                for group_id in scan_group_ids
            ]
            for future in as_completed(futures):
                try:
                    group_result = future.result()
                    scans.extend(group_result.scans)
                    errors.extend(group_result.errors)
                except Exception as e:
                    errors.append(e)
    except Exception as e:
        errors.append(e)

    logger.info(f"Time taken to scan cluster {cluster_name}: {time.monotonic() - t0:.3f}s")

    return ConsumerLatencyResult(scans=scans, errors=errors)


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

    return ConsumerLatencyResult(scans=scans, errors=errors)
