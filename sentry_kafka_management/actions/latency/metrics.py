from __future__ import annotations

from enum import Enum
from typing import Protocol

from datadog.dogstatsd.base import DogStatsd

Tags = dict[str, str]

METRICS_PREFIX = "kafka.topic"
SENDER_QUEUE_SIZE = 100000
SENDER_QUEUE_TIMEOUT = 0


class Metric(Enum):
    CONSUMER_LATENCY = "latency"


class TopicConsumerLatency(Protocol):
    cluster_name: str
    topic_name: str
    group_id: str
    latency_ms: float


class MetricsBackend(Protocol):
    def histogram(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        raise NotImplementedError


def _combine_tags(base_tags: Tags, tags: Tags | None = None) -> Tags:
    if tags is None:
        return base_tags
    return {**base_tags, **tags}


class DatadogMetricsBackend(MetricsBackend):
    def __init__(self, host: str, port: int, tags: Tags | None = None) -> None:
        self.datadog_client = DogStatsd(
            host=host,
            port=port,
            namespace=METRICS_PREFIX.strip("."),
            constant_tags=[],
        )
        self.datadog_client.enable_background_sender(  # type: ignore[no-untyped-call]
            sender_queue_size=SENDER_QUEUE_SIZE,
            sender_queue_timeout=SENDER_QUEUE_TIMEOUT,
        )
        self.__tags = tags or {}

    def __datadog_tags_kw(self, tags: Tags | None) -> list[str] | None:
        combined = _combine_tags(self.__tags, tags)
        return [f"{key}:{value}" for key, value in combined.items()] if combined else None

    def histogram(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        self.datadog_client.histogram(name, value, tags=self.__datadog_tags_kw(tags))


def create_topic_consumer_latency_tags(scan: TopicConsumerLatency) -> Tags:
    return {
        "cluster": scan.cluster_name,
        "consumer_group": scan.group_id,
        "topic": scan.topic_name,
    }


def emit_topic_consumer_latency(metrics: MetricsBackend, scan: TopicConsumerLatency) -> None:
    metrics.histogram(
        Metric.CONSUMER_LATENCY.value,
        scan.latency_ms,
        tags=create_topic_consumer_latency_tags(scan),
    )
