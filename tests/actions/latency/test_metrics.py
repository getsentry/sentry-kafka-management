from dataclasses import dataclass

from sentry_kafka_management.actions.latency.metrics import (
    Metric,
    MetricsBackend,
    Tags,
    create_topic_consumer_latency_tags,
    emit_topic_consumer_latency,
)


@dataclass
class FakeTopicConsumerLatency:
    cluster_name: str
    topic_name: str
    group_id: str
    latency_ms: float


class FakeMetricsBackend(MetricsBackend):
    def __init__(self) -> None:
        self.histograms: list[tuple[str, int | float, Tags | None]] = []

    def histogram(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        self.histograms.append((name, value, tags))


def test_create_topic_consumer_latency_tags() -> None:
    scan = FakeTopicConsumerLatency(
        cluster_name="cluster1",
        topic_name="topic1",
        group_id="consumer-group1",
        latency_ms=100.0,
    )

    assert create_topic_consumer_latency_tags(scan) == {
        "cluster": "cluster1",
        "consumer_group": "consumer-group1",
        "topic": "topic1",
    }


def test_emit_topic_consumer_latency() -> None:
    scan = FakeTopicConsumerLatency(
        cluster_name="cluster1",
        topic_name="topic1",
        group_id="consumer-group1",
        latency_ms=100.0,
    )
    metrics = FakeMetricsBackend()

    emit_topic_consumer_latency(metrics, scan)

    assert metrics.histograms == [
        (
            Metric.CONSUMER_LATENCY.value,
            100.0,
            {
                "cluster": "cluster1",
                "consumer_group": "consumer-group1",
                "topic": "topic1",
            },
        )
    ]
