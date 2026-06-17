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


def scan_partition_latencies(
    consumer: Consumer,
    scans: list[PartitionScan],
    timeout: int,
    stop_event: threading.Event | None = None,
) -> tuple[dict[tuple[str, int], float], list[Exception]]:
    """Classify each partition's consumer latency from a single poll loop."""
    latencies: dict[tuple[str, int], float] = {}
    errors: list[Exception] = []

    retention_by_key = {(scan.topic, scan.partition): scan.retention_ms for scan in scans}
    pending = set(retention_by_key)
    consumer.assign(
        [TopicPartition(scan.topic, scan.partition, scan.committed_offset) for scan in scans]
    )

    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        if stop_event is not None and stop_event.is_set():
            return latencies, errors
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        msg_topic = msg.topic()
        msg_partition = msg.partition()

        key = (msg_topic, msg_partition)
        if key not in pending:
            continue

        # We only need one message per partition to record latency. Our
        # consumer is assigned every partition at once, so without
        # pausing the broker keeps fetching from partitions we've already
        # scanned and poll() returns useless messages. These messages can
        # flood the queue and prevent unscanned partitions from being fetched.
        # Pausing scanned partitions prevents this blocking from happening.

        error = msg.error()
        if error is not None:
            code = error.code()
            if code in RETRYABLE_ERRORS:
                continue
            if code == KafkaError._PARTITION_EOF:
                latencies[key] = 0.0
            elif code == KafkaError._AUTO_OFFSET_RESET:
                latencies[key] = float(retention_by_key[key])
            else:
                errors.append(KafkaException(error))
            pending.discard(key)
            consumer.pause([TopicPartition(msg_topic, msg_partition)])
            continue

        ts_type, ts_ms = msg.timestamp()
        if ts_type == TIMESTAMP_NOT_AVAILABLE:
            errors.append(ValueError(f"Timestamp not available for {key[0]}[{key[1]}]"))
        elif ts_ms < 0:
            errors.append(ValueError(f"Invalid timestamp {ts_ms} for {key[0]}[{key[1]}]"))
        else:
            latencies[key] = float(int(time.time() * 1000) - int(ts_ms))
        pending.discard(key)
        consumer.pause([TopicPartition(msg_topic, msg_partition)])

    for topic, partition in pending:
        errors.append(TimeoutError(f"Timed out reading timestamp for {topic}[{partition}]"))

    return latencies, errors


def get_consumer_group_latency(
    admin: AdminClient,
    consumer_config: Mapping[str, object],
    cluster_name: str,
    group_id: str,
    retentions_by_topic: Mapping[str, int],
    timeout: int,
    stop_event: threading.Event | None = None,
) -> ConsumerLatencyResult:
    """
    Scan every partition for a single consumer group on one dedicated consumer.
    """
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []

    if stop_event is not None and stop_event.is_set():
        return ConsumerLatencyResult(scans=scans, errors=errors)

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
        latencies, scan_errors = scan_partition_latencies(
            consumer, group_scans, timeout, stop_event
        )
        errors.extend(scan_errors)
        for item in group_scans:
            latency_ms = latencies.get((item.topic, item.partition))
            if latency_ms is None:
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
    stop_event: threading.Event | None = None,
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

    # enable.partition.eof: ensures that _PARTITION_EOF is raised on poll consumer is caught up
    # auto.offset.reset: ensures _AUTO_OFFSET_RESET is raised on poll consumer is out of retention

    consumer_config = {
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "error",
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
                    stop_event,
                )
                for group_id in scan_group_ids
            ]
            try:
                for future in as_completed(futures):
                    if stop_event is not None and stop_event.is_set():
                        break
                    try:
                        group_result = future.result()
                        scans.extend(group_result.scans)
                        errors.extend(group_result.errors)
                    except Exception as e:
                        errors.append(e)
            finally:
                if stop_event is not None and stop_event.is_set():
                    for future in futures:
                        future.cancel()
    except Exception as e:
        errors.append(e)

    logger.info(f"Time taken to scan cluster {cluster_name}: {time.monotonic() - t0:.3f}s")

    return ConsumerLatencyResult(scans=scans, errors=errors)


def record_consumer_group_latency(
    config: YamlKafkaConfig,
    metrics: MetricsBackend,
    timeout: int = 10,
    max_workers: int = DEFAULT_MAX_WORKERS,
    stop_event: threading.Event | None = None,
    clusters: tuple[str, ...] | None = None,
) -> ConsumerLatencyResult:
    scans: list[TopicConsumerLatency] = []
    errors: list[Exception] = []

    available_clusters = config.get_clusters()
    if clusters:
        selected = set(clusters)
        cluster_items = {name: cfg for name, cfg in available_clusters.items() if name in selected}
    else:
        cluster_items = dict(available_clusters)

    for cluster_name, cluster_config in cluster_items.items():
        if stop_event is not None and stop_event.is_set():
            break
        try:
            topics = config.get_topics_config(cluster_name)
            result = get_cluster_latency(
                cluster_name, cluster_config, topics, timeout, max_workers, stop_event
            )
        except Exception as e:
            errors.append(e)
            continue

        for scan in result.scans:
            emit_topic_consumer_latency(metrics, scan)
            scans.append(scan)
        if result.errors:
            errors.extend(result.errors)

    return ConsumerLatencyResult(scans=scans, errors=errors)
