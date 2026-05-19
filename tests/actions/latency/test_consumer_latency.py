from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import (  # type: ignore[import-untyped]
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from sentry_kafka_management.actions.latency.consumer_latency import (
    RETENTION_TIME_MS,
    ConsumerGroupListingError,
    TopicConsumerLatency,
    get_cluster_latency,
    get_committed_offsets,
    get_partition_latency,
    list_consumer_group_ids,
    read_timestamp_ms,
    record_consumer_group_latency,
)
from sentry_kafka_management.actions.latency.metrics import MetricsBackend, Tags
from sentry_kafka_management.brokers import ClusterConfig

CLUSTER_CONFIG = ClusterConfig(
    brokers=["broker1:9092"],
    security_protocol=None,
    sasl_mechanism=None,
    sasl_username=None,
    sasl_password=None,
    password_is_plaintext=False,
)


class FakeMetricsBackend(MetricsBackend):
    def __init__(self) -> None:
        self.histograms: list[tuple[str, int | float, Tags | None]] = []

    def histogram(self, name: str, value: int | float, tags: Tags | None = None) -> None:
        self.histograms.append((name, value, tags))


def _make_group_listing(group_id: str) -> Mock:
    listing = Mock()
    listing.group_id = group_id
    return listing


def _make_list_groups_result(valid: list[Mock], errors: list[KafkaException] | None = None) -> Mock:
    future = Mock()
    future.result.return_value = Mock(valid=valid, errors=errors or [])
    return future


def _make_committed_offsets_future(
    partitions: list[TopicPartition],
) -> Mock:
    future = Mock()
    future.result.return_value = Mock(topic_partitions=partitions)
    return future


def _make_message(
    timestamp: tuple[int, int] | None = None,
    error: KafkaError | None = None,
) -> Mock:
    msg = Mock()
    msg.error.return_value = error
    if timestamp is not None:
        msg.timestamp.return_value = timestamp
    return msg


def test_list_consumer_group_ids_returns_all_ids() -> None:
    admin = Mock()
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a"), _make_group_listing("group-b")],
    )

    assert list_consumer_group_ids(admin) == ["group-a", "group-b"]
    admin.list_consumer_groups.assert_called_once()


def test_list_consumer_group_ids_returns_empty_when_no_groups() -> None:
    admin = Mock()
    admin.list_consumer_groups.return_value = _make_list_groups_result(valid=[])

    assert list_consumer_group_ids(admin) == []


def test_list_consumer_group_ids_raises_on_errors() -> None:
    admin = Mock()
    errors = [KafkaException("error1"), KafkaException("error2")]
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a")],
        errors=errors,
    )

    with pytest.raises(ConsumerGroupListingError) as excinfo:
        list_consumer_group_ids(admin)

    assert excinfo.value.errors == errors
    assert "2 error(s)" in str(excinfo.value)


def test_get_committed_offsets_returns_partitions() -> None:
    admin = Mock()
    tp0 = TopicPartition("topic-a", 0, 100)
    tp1 = TopicPartition("topic-a", 1, 200)
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future([tp0, tp1])
    }

    assert get_committed_offsets(admin, "group-a") == [tp0, tp1]


def test_get_committed_offsets_empty() -> None:
    admin = Mock()
    admin.list_consumer_group_offsets.return_value = {"group-a": _make_committed_offsets_future([])}

    assert get_committed_offsets(admin, "group-a") == []


def test_get_committed_offsets_raises_on_partition_error() -> None:
    admin = Mock()

    bad_tp = Mock(spec=["topic", "partition", "offset", "error"])
    bad_tp.topic = "topic-a"
    bad_tp.partition = 0
    bad_tp.offset = 100
    bad_tp.error = KafkaError(KafkaError.UNKNOWN, "error")
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future([bad_tp])
    }

    with pytest.raises(KafkaException):
        get_committed_offsets(admin, "group-a")


def test_read_timestamp_ms_returns_create_time_timestamp() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 1_700_000_000_000))

    assert read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10) == 1_700_000_000_000

    consumer.assign.assert_called_once()
    (assigned,) = consumer.assign.call_args.args
    assert assigned == [TopicPartition("topic-a", 0, 42)]


def test_read_timestamp_ms_returns_log_append_time_timestamp() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        timestamp=(TIMESTAMP_LOG_APPEND_TIME, 1_700_000_000_500)
    )

    assert read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10) == 1_700_000_000_500


def test_read_timestamp_ms_skips_empty_polls_then_returns() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        None,
        None,
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 1_700_000_000_000)),
    ]

    assert read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10) == 1_700_000_000_000
    assert consumer.poll.call_count == 3


def test_read_timestamp_ms_raises_on_message_error() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        error=KafkaError(KafkaError.UNKNOWN, "fetch failed"),
    )

    with pytest.raises(KafkaException):
        read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10)


def test_read_timestamp_ms_retries_on_retryable_error() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        _make_message(error=KafkaError(KafkaError.REQUEST_TIMED_OUT, "request timed out")),
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 1_700_000_000_000)),
    ]

    assert read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10) == 1_700_000_000_000
    assert consumer.poll.call_count == 2


def test_read_timestamp_ms_raises_when_timestamp_not_available() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        timestamp=(TIMESTAMP_NOT_AVAILABLE, 0),
    )

    with pytest.raises(ValueError, match="Timestamp not available"):
        read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10)


def test_read_timestamp_ms_raises_on_negative_timestamp() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        timestamp=(TIMESTAMP_CREATE_TIME, -1),
    )

    with pytest.raises(ValueError, match="Invalid timestamp"):
        read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10)


def test_read_timestamp_ms_raises_on_timeout() -> None:
    consumer = Mock()
    consumer.poll.return_value = None

    with patch("time.monotonic", side_effect=[0.0, 0.5, 100.0]):
        with pytest.raises(TimeoutError, match="Timed out"):
            read_timestamp_ms(consumer, "topic-a", 0, 42, timeout=10)


def test_get_partition_latency_returns_zero_for_empty_partition() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (50, 50)

    assert get_partition_latency(consumer, "topic-a", 0, committed_offset=50, timeout=10) == 0.0
    consumer.poll.assert_not_called()


def test_get_partition_latency_returns_zero_when_consumer_caught_up() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (10, 100)

    assert get_partition_latency(consumer, "topic-a", 0, committed_offset=100, timeout=10) == 0.0
    consumer.poll.assert_not_called()


def test_get_partition_latency_returns_zero_when_committed_past_high() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (10, 100)

    assert get_partition_latency(consumer, "topic-a", 0, committed_offset=999, timeout=10) == 0.0


def test_get_partition_latency_raises_when_high_watermark_invalid() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (0, OFFSET_INVALID)

    with pytest.raises(ValueError, match="No valid high watermark"):
        get_partition_latency(consumer, "topic-a", 0, committed_offset=0, timeout=10)


def test_get_partition_latency_reads_committed_offset_when_in_range() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (10, 100)
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 1_700_000_000_000))

    with patch("time.time", return_value=1_700_000_000.5):
        latency_ms = get_partition_latency(consumer, "topic-a", 0, committed_offset=50, timeout=10)

    assert latency_ms == 500.0
    (assigned,) = consumer.assign.call_args.args
    assert assigned == [TopicPartition("topic-a", 0, 50)]


def test_get_partition_latency_returns_retention_when_committed_below_low_watermark() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (100, 200)

    assert (
        get_partition_latency(consumer, "topic-a", 0, committed_offset=5, timeout=10)
        == RETENTION_TIME_MS
    )
    consumer.poll.assert_not_called()


def test_get_partition_latency_returns_zero_for_uncommitted_offset() -> None:
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (10, 100)

    assert (
        get_partition_latency(consumer, "topic-a", 0, committed_offset=OFFSET_INVALID, timeout=10)
        == 0.0
    )
    consumer.poll.assert_not_called()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_skips_own_consumer_group(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    own_group = "consumer-latency-group-cluster1"
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing(own_group)],
    )

    result = get_cluster_latency("cluster1", CLUSTER_CONFIG, topics=["topic-a"], timeout=10)

    assert result == []
    admin.list_consumer_group_offsets.assert_not_called()
    mock_consumer_cls.return_value.close.assert_called_once()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_filters_unconfigured_topics(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a")],
    )
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future(
            [
                TopicPartition("topic-a", 0, 50),
                TopicPartition("other-topic", 0, 100),
            ]
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.get_watermark_offsets.return_value = (10, 100)
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 0))

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency("cluster1", CLUSTER_CONFIG, topics=["topic-a"], timeout=10)

    assert [scan.topic_name for scan in result] == ["topic-a"]
    for call in consumer.get_watermark_offsets.call_args_list:
        assert call.args[0].topic == "topic-a"


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_emits_max_partition_latency_per_topic(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a")],
    )
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future(
            [
                TopicPartition("topic-a", 0, 50),
                TopicPartition("topic-a", 1, 50),
            ]
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.get_watermark_offsets.return_value = (0, 100)

    consumer.poll.side_effect = [
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 800)),
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 0)),
    ]

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency("cluster1", CLUSTER_CONFIG, topics=["topic-a"], timeout=10)

    assert result == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=1000.0,
        )
    ]


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_skips_groups_with_no_matching_topics(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a")],
    )
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future(
            [TopicPartition("other-topic", 0, 100)],
        ),
    }

    result = get_cluster_latency("cluster1", CLUSTER_CONFIG, topics=["topic-a"], timeout=10)

    assert result == []
    mock_consumer_cls.return_value.get_watermark_offsets.assert_not_called()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_closes_consumer_on_error(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.side_effect = KafkaException("boom")

    with pytest.raises(KafkaException):
        get_cluster_latency("cluster1", CLUSTER_CONFIG, topics=["topic-a"], timeout=10)

    mock_consumer_cls.return_value.close.assert_called_once()


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_emits_one_histogram_per_scan(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {
        "cluster1": CLUSTER_CONFIG,
        "cluster2": CLUSTER_CONFIG,
    }
    config.get_topics_config.side_effect = lambda name: {
        "cluster1": {"topic-a": {}},
        "cluster2": {"topic-b": {}},
    }[name]
    mock_get_cluster_latency.side_effect = [
        [
            TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0),
            TopicConsumerLatency("cluster1", "topic-a", "group-b", 200.0),
        ],
        [TopicConsumerLatency("cluster2", "topic-b", "group-c", 50.0)],
    ]
    metrics = FakeMetricsBackend()

    scans = record_consumer_group_latency(config, metrics)

    assert len(scans) == 3
    assert [latency for _, latency, _ in metrics.histograms] == [100.0, 200.0, 50.0]
    cluster_tags = [tags["cluster"] for _, _, tags in metrics.histograms if tags]
    assert cluster_tags == ["cluster1", "cluster1", "cluster2"]


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_emits_nothing_when_no_clusters_have_latency(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {"cluster1": CLUSTER_CONFIG}
    config.get_topics_config.return_value = {"topic-a": {}}
    mock_get_cluster_latency.return_value = []
    metrics = FakeMetricsBackend()

    assert record_consumer_group_latency(config, metrics) == []
    assert metrics.histograms == []
