from __future__ import annotations

import threading
from concurrent.futures import Future
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import (  # type: ignore[import-untyped]
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConsumerGroupListing,
    ListConsumerGroupsResult,
)

from sentry_kafka_management.actions.latency.consumer_latency import (
    ConsumerGroupListingError,
    ConsumerLatencyResult,
    PartitionScan,
    TopicConsumerLatency,
    get_cluster_latency,
    get_committed_offsets,
    list_consumer_group_ids,
    record_consumer_group_latency,
    scan_partition_latencies,
)
from sentry_kafka_management.actions.latency.metrics import MetricsBackend, Tags
from sentry_kafka_management.brokers import ClusterConfig, TopicConfig

RETENTION_MS = 86_400_000


def _topic_config(retention_ms: int = RETENTION_MS, partitions: int = 1) -> TopicConfig:
    return TopicConfig(
        partitions=partitions,
        placement=1,
        replication_factor=1,
        settings={"retention.ms": retention_ms},
    )


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


def _make_group_listing(group_id: str) -> ConsumerGroupListing:
    return ConsumerGroupListing(group_id=group_id, is_simple_consumer_group=False)


def _make_list_groups_result(
    valid: list[ListConsumerGroupsResult], errors: list[KafkaException] | None = None
) -> Future[ListConsumerGroupsResult]:
    result = ListConsumerGroupsResult()
    result.valid = valid
    result.errors = errors
    future = Future[ListConsumerGroupsResult]()
    future.set_result(result)
    return future


def _make_committed_offsets_future(
    group_id: str,
    partitions: list[TopicPartition],
) -> Future[ConsumerGroupTopicPartitions]:
    future = Future[ConsumerGroupTopicPartitions]()
    future.set_result(ConsumerGroupTopicPartitions(group_id=group_id, topic_partitions=partitions))
    return future


def _make_errored_committed_offsets_future(exc: Exception) -> Future[ConsumerGroupTopicPartitions]:
    future: Future[ConsumerGroupTopicPartitions] = Future()
    future.set_exception(exc)
    return future


def _make_errored_topic_partition(topic: str, partition: int, offset: int) -> Mock:
    bad_tp = Mock(spec=["topic", "partition", "offset", "error"])
    bad_tp.topic = topic
    bad_tp.partition = partition
    bad_tp.offset = offset
    bad_tp.error = KafkaError(KafkaError.UNKNOWN, "error")
    return bad_tp


def _make_message(
    timestamp: tuple[int, int] | None = None,
    error: KafkaError | None = None,
    topic: str = "topic-a",
    partition: int = 0,
) -> Mock:
    msg = Mock()
    msg.error.return_value = error
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    if timestamp is not None:
        msg.timestamp.return_value = timestamp
    return msg


def _scan(
    topic: str = "topic-a",
    partition: int = 0,
    committed_offset: int = 42,
    retention_ms: int = RETENTION_MS,
) -> PartitionScan:
    return PartitionScan(
        group_id="group-a",
        topic=topic,
        partition=partition,
        committed_offset=committed_offset,
        retention_ms=retention_ms,
    )


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
    assert [listing.group_id for listing in excinfo.value.valid] == ["group-a"]
    assert "2 error(s)" in str(excinfo.value)


@pytest.mark.parametrize(
    ("partitions", "expected", "raises"),
    [
        pytest.param(
            [
                TopicPartition("topic-a", 0, 100),
                TopicPartition("topic-a", 1, 200),
            ],
            [
                TopicPartition("topic-a", 0, 100),
                TopicPartition("topic-a", 1, 200),
            ],
            None,
            id="returns_partitions",
        ),
        pytest.param([], [], None, id="empty"),
        pytest.param(
            [_make_errored_topic_partition("topic-a", 0, 100)],
            None,
            KafkaException,
            id="raises_on_partition_error",
        ),
    ],
)
def test_get_committed_offsets(
    partitions: list[TopicPartition | Mock],
    expected: list[TopicPartition] | None,
    raises: type[Exception] | None,
) -> None:
    admin = Mock()
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future("group-a", partitions)
    }

    result = get_committed_offsets(admin, ["group-a"])

    if raises is not None:
        assert isinstance(result["group-a"], raises)
    else:
        assert result["group-a"] == expected


def test_scan_partition_latencies_returns_latency_from_create_time() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 1_700_000_000_000))

    with patch("time.time", return_value=1_700_000_000.5):
        latencies, errors = scan_partition_latencies(
            consumer, [_scan(committed_offset=42)], timeout=10
        )

    assert latencies == {("topic-a", 0): 500.0}
    assert errors == []
    consumer.assign.assert_called_once()
    (assigned,) = consumer.assign.call_args.args
    assert assigned == [TopicPartition("topic-a", 0, 42)]


def test_scan_partition_latencies_returns_latency_from_log_append_time() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        timestamp=(TIMESTAMP_LOG_APPEND_TIME, 1_700_000_000_500)
    )

    with patch("time.time", return_value=1_700_000_001.0):
        latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {("topic-a", 0): 500.0}
    assert errors == []


def test_scan_partition_latencies_caught_up_partition_is_zero() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        error=KafkaError(KafkaError._PARTITION_EOF, "reached end of partition"),
    )

    latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {("topic-a", 0): 0.0}
    assert errors == []


def test_scan_partition_latencies_aged_out_uses_retention() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        error=KafkaError(KafkaError._AUTO_OFFSET_RESET, "Offset out of range"),
    )

    latencies, errors = scan_partition_latencies(consumer, [_scan(retention_ms=5_000)], timeout=10)

    assert latencies == {("topic-a", 0): 5_000.0}
    assert errors == []


def test_scan_partition_latencies_assigns_all_partitions_in_one_batch() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 10), partition=0),
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 20), partition=1),
    ]

    scans = [_scan(partition=0, committed_offset=5), _scan(partition=1, committed_offset=7)]
    with patch("time.time", return_value=1.0):
        latencies, errors = scan_partition_latencies(consumer, scans, timeout=10)

    assert latencies == {("topic-a", 0): 990.0, ("topic-a", 1): 980.0}
    assert errors == []
    consumer.assign.assert_called_once()
    (assigned,) = consumer.assign.call_args.args
    assert assigned == [TopicPartition("topic-a", 0, 5), TopicPartition("topic-a", 1, 7)]


def test_scan_partition_latencies_skips_empty_polls_then_returns() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        None,
        None,
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 5)),
    ]

    with patch("time.time", return_value=1.0):
        latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {("topic-a", 0): 995.0}
    assert consumer.poll.call_count == 3


def test_scan_partition_latencies_records_message_error() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(
        error=KafkaError(KafkaError.UNKNOWN, "fetch failed"),
    )

    latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {}
    assert len(errors) == 1
    assert isinstance(errors[0], KafkaException)


def test_scan_partition_latencies_retries_on_retryable_error() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        _make_message(error=KafkaError(KafkaError.REQUEST_TIMED_OUT, "request timed out")),
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 9)),
    ]

    with patch("time.time", return_value=1.0):
        latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {("topic-a", 0): 991.0}
    assert errors == []
    assert consumer.poll.call_count == 2


def test_scan_partition_latencies_records_timestamp_not_available() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_NOT_AVAILABLE, 0))

    latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {}
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "Timestamp not available" in str(errors[0])


def test_scan_partition_latencies_records_negative_timestamp() -> None:
    consumer = Mock()
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, -1))

    latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {}
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "Invalid timestamp" in str(errors[0])


def test_scan_partition_latencies_pauses_each_partition_once_resolved() -> None:
    consumer = Mock()
    consumer.poll.side_effect = [
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 10), partition=0),
        _make_message(error=KafkaError(KafkaError._PARTITION_EOF, "eof"), partition=1),
    ]

    scans = [_scan(partition=0, committed_offset=5), _scan(partition=1, committed_offset=9)]
    with patch("time.time", return_value=1.0):
        scan_partition_latencies(consumer, scans, timeout=10)

    paused = [call.args[0] for call in consumer.pause.call_args_list]
    assert paused == [[TopicPartition("topic-a", 0)], [TopicPartition("topic-a", 1)]]


def test_scan_partition_latencies_records_timeout_for_unread_partitions() -> None:
    consumer = Mock()
    consumer.poll.return_value = None

    with patch("time.monotonic", side_effect=[0.0, 0.5, 100.0]):
        latencies, errors = scan_partition_latencies(consumer, [_scan()], timeout=10)

    assert latencies == {}
    assert len(errors) == 1
    assert isinstance(errors[0], TimeoutError)


def test_scan_partition_latencies_returns_early_when_stopped() -> None:
    consumer = Mock()
    consumer.poll.return_value = None
    stop_event = threading.Event()
    stop_event.set()

    latencies, errors = scan_partition_latencies(
        consumer, [_scan()], timeout=10, stop_event=stop_event
    )

    assert latencies == {}
    assert errors == []
    consumer.poll.assert_not_called()


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_stops_between_clusters(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {
        "cluster1": CLUSTER_CONFIG,
        "cluster2": CLUSTER_CONFIG,
    }
    config.get_topics_config.return_value = {"topic-a": {}}
    stop_event = threading.Event()

    def stop_after_first(
        cluster_name: str, *_args: object, **_kwargs: object
    ) -> ConsumerLatencyResult:
        stop_event.set()
        return ConsumerLatencyResult(
            scans=[TopicConsumerLatency(cluster_name, "topic-a", "group-a", 1.0, partition=0)],
        )

    mock_get_cluster_latency.side_effect = stop_after_first
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics, stop_event=stop_event)

    assert mock_get_cluster_latency.call_count == 1
    assert len(result.scans) == 1


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

    result = get_cluster_latency(
        "cluster1", CLUSTER_CONFIG, topics={"topic-a": _topic_config()}, timeout=10
    )

    assert result.scans == []
    assert len(result.errors) == 0
    admin.list_consumer_group_offsets.assert_not_called()
    mock_consumer_cls.assert_not_called()


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
            "group-a",
            [
                TopicPartition("topic-a", 0, 50),
                TopicPartition("other-topic", 0, 100),
            ],
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 0))

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency(
            "cluster1", CLUSTER_CONFIG, topics={"topic-a": _topic_config()}, timeout=10
        )

    assert [scan.topic_name for scan in result.scans] == ["topic-a"]
    assert len(result.errors) == 0
    for call in consumer.assign.call_args_list:
        for assigned in call.args[0]:
            assert assigned.topic == "topic-a"


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_reports_latency_per_partition(
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
            "group-a",
            [
                TopicPartition("topic-a", 0, 50),
                TopicPartition("topic-a", 1, 50),
            ],
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.poll.side_effect = [
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 800), partition=0),
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 0), partition=1),
    ]

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency(
            "cluster1",
            CLUSTER_CONFIG,
            topics={"topic-a": _topic_config(partitions=1)},
            timeout=10,
        )

    assert result.scans == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=200.0,
            partition=0,
        ),
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=1000.0,
            partition=1,
        ),
    ]
    assert len(result.errors) == 0


@patch("sentry_kafka_management.actions.latency.consumer_latency.ThreadPoolExecutor")
@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_caps_scan_workers(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
    mock_executor_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a")],
    )
    partitions = [TopicPartition("topic-a", p, 50) for p in range(10)]
    admin.list_consumer_group_offsets.return_value = {
        "group-a": _make_committed_offsets_future("group-a", partitions),
    }

    consumer = mock_consumer_cls.return_value
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 800))

    executor = MagicMock()
    mock_executor_cls.return_value.__enter__.return_value = executor

    def submit(_fn: object, *args: object) -> Future[ConsumerLatencyResult]:
        future: Future[ConsumerLatencyResult] = Future()
        future.set_result(
            ConsumerLatencyResult(
                scans=[
                    TopicConsumerLatency(
                        cluster_name="cluster1",
                        group_id="group-a",
                        topic_name="topic-a",
                        latency_ms=0.0,
                        partition=0,
                    )
                ],
            )
        )
        return future

    executor.submit.side_effect = submit

    with patch("time.time", return_value=1.0):
        get_cluster_latency(
            "cluster1",
            CLUSTER_CONFIG,
            topics={"topic-a": _topic_config(partitions=10)},
            timeout=10,
            max_workers=4,
        )

    scan_call = mock_executor_cls.call_args_list[0]
    assert scan_call.kwargs["max_workers"] == 4


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_skips_uncommitted_partitions(
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
            "group-a",
            [TopicPartition("topic-a", 0, 100)],
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.poll.return_value = _make_message(
        error=KafkaError(KafkaError._PARTITION_EOF, "reached end of partition"),
    )

    result = get_cluster_latency(
        "cluster1",
        CLUSTER_CONFIG,
        topics={"topic-a": _topic_config(partitions=2)},
        timeout=10,
    )

    assert result.scans == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=0.0,
            partition=0,
        ),
    ]
    assert len(result.errors) == 0
    consumer.assign.assert_called_once()


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
            "group-a",
            [TopicPartition("other-topic", 0, 100)],
        ),
    }

    result = get_cluster_latency(
        "cluster1", CLUSTER_CONFIG, topics={"topic-a": _topic_config()}, timeout=10
    )

    assert result.scans == []
    assert len(result.errors) == 0
    mock_consumer_cls.return_value.poll.assert_not_called()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_closes_consumer_on_error(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.side_effect = KafkaException("boom")

    result = get_cluster_latency(
        "cluster1", CLUSTER_CONFIG, topics={"topic-a": _topic_config()}, timeout=10
    )

    assert result.scans == []
    assert result.errors is not None
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], KafkaException)

    mock_consumer_cls.assert_not_called()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_continues_after_committed_offsets_error(
    mock_get_admin: MagicMock,
    mock_consumer_cls: MagicMock,
) -> None:
    admin = Mock()
    mock_get_admin.return_value = admin
    admin.list_consumer_groups.return_value = _make_list_groups_result(
        valid=[_make_group_listing("group-a"), _make_group_listing("group-b")],
    )
    offsets_by_group = {
        "group-a": _make_committed_offsets_future("group-a", [TopicPartition("topic-a", 0, 50)]),
        "group-b": _make_errored_committed_offsets_future(KafkaException("boom")),
    }

    def list_offsets(
        reqs: list[ConsumerGroupTopicPartitions],
    ) -> dict[str, Future[ConsumerGroupTopicPartitions]]:
        group_id = reqs[0].group_id
        return {group_id: offsets_by_group[group_id]}

    admin.list_consumer_group_offsets.side_effect = list_offsets

    consumer = mock_consumer_cls.return_value
    consumer.poll.return_value = _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 800))

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency(
            "cluster1", CLUSTER_CONFIG, topics={"topic-a": _topic_config()}, timeout=10
        )

    assert result.errors is not None
    assert len(result.errors) == 1
    assert result.scans == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=200.0,
            partition=0,
        ),
    ]
    mock_consumer_cls.return_value.close.assert_called_once()


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_continues_after_partition_latency_error(
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
            "group-a",
            [
                TopicPartition("topic-a", 0, 50),
                TopicPartition("topic-a", 1, 50),
            ],
        ),
    }

    consumer = mock_consumer_cls.return_value
    consumer.poll.side_effect = [
        _make_message(timestamp=(TIMESTAMP_CREATE_TIME, 800), partition=0),
        _make_message(error=KafkaError(KafkaError.UNKNOWN, "fetch failed"), partition=1),
    ]

    with patch("time.time", return_value=1.0):
        result = get_cluster_latency(
            "cluster1",
            CLUSTER_CONFIG,
            topics={"topic-a": _topic_config(partitions=2)},
            timeout=10,
        )

    assert result.errors is not None
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], KafkaException)
    assert result.scans == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-a",
            latency_ms=200.0,
            partition=0,
        ),
    ]


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_uses_per_topic_retention_when_aged_out(
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
            "group-a",
            [
                TopicPartition("topic-short", 0, 5),
                TopicPartition("topic-long", 0, 5),
            ],
        ),
    }
    mock_consumer_cls.return_value.poll.side_effect = [
        _make_message(
            error=KafkaError(KafkaError._AUTO_OFFSET_RESET, "Offset out of range"),
            topic="topic-short",
            partition=0,
        ),
        _make_message(
            error=KafkaError(KafkaError._AUTO_OFFSET_RESET, "Offset out of range"),
            topic="topic-long",
            partition=0,
        ),
    ]

    result = get_cluster_latency(
        "cluster1",
        CLUSTER_CONFIG,
        topics={
            "topic-short": _topic_config(retention_ms=5_000),
            "topic-long": _topic_config(retention_ms=86_400_000),
        },
        timeout=10,
    )

    assert len(result.errors) == 0
    assert {(s.topic_name, s.latency_ms) for s in result.scans} == {
        ("topic-short", 5_000.0),
        ("topic-long", 86_400_000.0),
    }


@patch("sentry_kafka_management.actions.latency.consumer_latency.Consumer")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_admin_client")
def test_get_cluster_latency_uses_default_retention_when_unset(
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
            "group-a",
            [TopicPartition("topic-no-retention", 0, 5)],
        ),
    }
    mock_consumer_cls.return_value.poll.return_value = _make_message(
        error=KafkaError(KafkaError._AUTO_OFFSET_RESET, "Offset out of range"),
        topic="topic-no-retention",
        partition=0,
    )
    no_retention_topic = TopicConfig(
        partitions=1,
        placement=1,
        replication_factor=1,
        settings={"cleanup.policy": "delete"},
    )

    result = get_cluster_latency(
        "cluster1",
        CLUSTER_CONFIG,
        topics={"topic-no-retention": no_retention_topic},
        timeout=10,
    )

    assert len(result.errors) == 0
    assert result.scans == [
        TopicConsumerLatency(
            cluster_name="cluster1",
            group_id="group-a",
            topic_name="topic-no-retention",
            latency_ms=604_800_000.0,
            partition=0,
        ),
    ]


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
    cluster_results = {
        "cluster1": ConsumerLatencyResult(
            scans=[
                TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0, partition=0),
                TopicConsumerLatency("cluster1", "topic-a", "group-b", 200.0, partition=1),
            ],
        ),
        "cluster2": ConsumerLatencyResult(
            scans=[
                TopicConsumerLatency("cluster2", "topic-b", "group-c", 50.0, partition=2),
            ],
        ),
    }
    mock_get_cluster_latency.side_effect = lambda name, *a, **k: cluster_results[name]
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics)

    assert len(result.scans) == 3
    assert len(result.errors) == 0
    assert [latency for _, latency, _ in metrics.histograms] == [100.0, 200.0, 50.0]
    cluster_tags = [tags["cluster"] for _, _, tags in metrics.histograms if tags]
    assert cluster_tags == ["cluster1", "cluster1", "cluster2"]
    partition_tags = [tags["partition"] for _, _, tags in metrics.histograms if tags]
    assert partition_tags == ["0", "1", "2"]


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_scans_only_selected_clusters(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {
        "cluster1": CLUSTER_CONFIG,
        "cluster2": CLUSTER_CONFIG,
    }
    config.get_topics_config.return_value = {"topic-a": {}}
    mock_get_cluster_latency.side_effect = lambda name, *a, **k: ConsumerLatencyResult(
        scans=[TopicConsumerLatency(name, "topic-a", "group-a", 1.0, partition=0)],
    )
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics, clusters=("cluster2",))

    scanned = [call.args[0] for call in mock_get_cluster_latency.call_args_list]
    assert scanned == ["cluster2"]
    assert [scan.cluster_name for scan in result.scans] == ["cluster2"]


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_scans_all_clusters_by_default(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {
        "cluster1": CLUSTER_CONFIG,
        "cluster2": CLUSTER_CONFIG,
    }
    config.get_topics_config.return_value = {"topic-a": {}}
    mock_get_cluster_latency.return_value = ConsumerLatencyResult(scans=[])
    metrics = FakeMetricsBackend()

    record_consumer_group_latency(config, metrics)

    scanned = {call.args[0] for call in mock_get_cluster_latency.call_args_list}
    assert scanned == {"cluster1", "cluster2"}


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_emits_nothing_when_no_clusters_have_latency(
    mock_get_cluster_latency: MagicMock,
) -> None:
    config = Mock()
    config.get_clusters.return_value = {"cluster1": CLUSTER_CONFIG}
    config.get_topics_config.return_value = {"topic-a": {}}
    mock_get_cluster_latency.return_value = ConsumerLatencyResult(scans=[])
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics)

    assert result.scans == []
    assert len(result.errors) == 0
    assert metrics.histograms == []


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_emits_good_clusters_with_errors(
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
    cluster_results = {
        "cluster1": ConsumerLatencyResult(
            scans=[TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0, partition=0)],
        ),
        "cluster2": ConsumerLatencyResult(
            scans=[
                TopicConsumerLatency("cluster2", "topic-b", "group-b", 50.0, partition=1),
            ],
            errors=[KafkaException("boom")],
        ),
    }
    mock_get_cluster_latency.side_effect = lambda name, *a, **k: cluster_results[name]
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics)

    assert result.errors is not None
    assert len(result.errors) == 1
    assert result.scans == [
        TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0, partition=0),
        TopicConsumerLatency("cluster2", "topic-b", "group-b", 50.0, partition=1),
    ]
    assert [latency for _, latency, _ in metrics.histograms] == [100.0, 50.0]
    cluster_tags = [tags["cluster"] for _, _, tags in metrics.histograms if tags]
    assert cluster_tags == ["cluster1", "cluster2"]


@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
def test_record_consumer_group_latency_emits_good_clusters_on_total_cluster_failure(
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
    cluster_results = {
        "cluster1": ConsumerLatencyResult(
            scans=[TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0, partition=0)],
        ),
        "cluster2": ConsumerLatencyResult(scans=[], errors=[KafkaException("cluster unreachable")]),
    }
    mock_get_cluster_latency.side_effect = lambda name, *a, **k: cluster_results[name]
    metrics = FakeMetricsBackend()

    result = record_consumer_group_latency(config, metrics)

    assert result.errors is not None
    assert len(result.errors) == 1
    assert result.scans == [
        TopicConsumerLatency("cluster1", "topic-a", "group-a", 100.0, partition=0),
    ]
    assert [latency for _, latency, _ in metrics.histograms] == [100.0]
