from unittest.mock import Mock, patch

import pytest
from confluent_kafka import (  # type: ignore[import-untyped]
    ElectionType,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from sentry_kafka_management.actions.topics.partitions import elect_partition_leaders


def test_elect_partition_leaders_cluster_wide() -> None:
    mock_client = Mock()
    mock_future = Mock()
    mock_future.result.return_value = {}
    mock_client.elect_leaders.return_value = mock_future

    success, errors = elect_partition_leaders(mock_client)

    mock_client.elect_leaders.assert_called_once_with(
        election_type=ElectionType.PREFERRED,
        partitions=None,
    )
    assert success == []
    assert errors == []


def test_elect_partition_leaders_explicit_partitions() -> None:
    mock_client = Mock()
    tp0 = TopicPartition("my-topic", 0)
    tp1 = TopicPartition("my-topic", 1)
    err = KafkaError(KafkaError.UNKNOWN, "unknown error")
    mock_future = Mock()
    mock_future.result.return_value = {tp0: None, tp1: err}
    mock_client.elect_leaders.return_value = mock_future

    success, errors = elect_partition_leaders(
        mock_client, partitions=[TopicPartition("my-topic", 0), TopicPartition("my-topic", 1)]
    )

    mock_client.elect_leaders.assert_called_once_with(
        election_type=ElectionType.PREFERRED,
        partitions=[TopicPartition("my-topic", 0), TopicPartition("my-topic", 1)],
    )
    assert success == [{"topic": "my-topic", "id": 0}]
    assert errors == [{"topic": "my-topic", "id": 1, "error": "unknown error"}]


def test_elect_partition_leaders_election_not_needed_counts_as_success() -> None:
    """Partitions that already have the preferred leader report ELECTION_NOT_NEEDED."""
    mock_client = Mock()
    tp = TopicPartition("my-topic", 0)
    mock_future = Mock()
    mock_future.result.return_value = {tp: KafkaError.ELECTION_NOT_NEEDED}
    mock_client.elect_leaders.return_value = mock_future

    success, errors = elect_partition_leaders(mock_client, partitions=[tp])

    assert success == [{"topic": "my-topic", "id": 0}]
    assert errors == []


@patch("sentry_kafka_management.actions.topics.partitions.describe_topic_partitions")
def test_elect_partition_leaders_with_topics(mock_describe: Mock) -> None:
    mock_describe.side_effect = [
        [{"topic": "topic-a", "id": 0}, {"topic": "topic-a", "id": 1}],
        [{"topic": "topic-b", "id": 0}],
    ]
    mock_client = Mock()
    mock_future = Mock()
    mock_future.result.return_value = {
        TopicPartition("topic-a", 0): None,
        TopicPartition("topic-a", 1): None,
        TopicPartition("topic-b", 0): None,
    }
    mock_client.elect_leaders.return_value = mock_future

    success, errors = elect_partition_leaders(mock_client, topics=["topic-a", "topic-b"])

    assert mock_describe.call_count == 2
    mock_client.elect_leaders.assert_called_once_with(
        election_type=ElectionType.PREFERRED,
        partitions=[
            TopicPartition("topic-a", 0),
            TopicPartition("topic-a", 1),
            TopicPartition("topic-b", 0),
        ],
    )
    assert len(success) == 3
    assert errors == []


@patch("sentry_kafka_management.actions.topics.partitions.describe_topic_partitions")
def test_elect_partition_leaders_topics_plus_explicit_partitions(mock_describe: Mock) -> None:
    mock_describe.return_value = [{"topic": "t1", "id": 0}]
    extra = TopicPartition("t2", 5)
    mock_client = Mock()
    mock_future = Mock()
    mock_future.result.return_value = {
        TopicPartition("t1", 0): None,
        extra: None,
    }
    mock_client.elect_leaders.return_value = mock_future

    elect_partition_leaders(mock_client, topics=["t1"], partitions=[extra])

    mock_client.elect_leaders.assert_called_once_with(
        election_type=ElectionType.PREFERRED,
        partitions=[extra, TopicPartition("t1", 0)],
    )


def test_elect_partition_leaders_kafka_exception() -> None:
    mock_client = Mock()
    mock_future = Mock()
    mock_future.result.side_effect = KafkaException("election failed")
    mock_client.elect_leaders.return_value = mock_future

    with pytest.raises(ValueError):
        elect_partition_leaders(mock_client, partitions=[TopicPartition("t", 0)])
