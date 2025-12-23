from typing import Any
from unittest.mock import Mock

import pytest
from confluent_kafka import (  # type: ignore[import-untyped]
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConfigResource,
    ConfigSource,
    OffsetSpec,
)

from sentry_kafka_management.actions.topics import (
    describe_topic_configs,
    list_offsets,
    list_topics,
)


def _create_mock_partition(partition_id: int) -> Mock:
    """Create a mock partition with the given ID."""
    mock_partition = Mock()
    mock_partition.id = partition_id
    return mock_partition


def _create_mock_topic_description(partitions: list[Mock]) -> Mock:
    """Create a mock topic description with the given partitions."""
    mock_description = Mock()
    mock_description.partitions = partitions
    return mock_description


def _create_mock_future(result_value: Mock) -> Mock:
    """Create a mock future that returns the given value."""
    mock_future = Mock()
    mock_future.result.return_value = result_value
    return mock_future


def _create_mock_offset_result(offset: int) -> Mock:
    """Create a mock offset result with the given offset value."""
    mock_offset = Mock()
    mock_offset.offset = offset
    return mock_offset


def test_list_topics() -> None:
    """Test listing topics."""
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.topics = {"test_topic": Mock()}

    result = list_topics(mock_client)
    mock_client.list_topics.assert_called_once()
    assert result == ["test_topic"]


def test_list_offsets() -> None:
    """Test listing offsets."""
    mock_client = Mock()

    # Set up mock partitions and topic description
    partitions = [_create_mock_partition(0), _create_mock_partition(1)]
    topic_description = _create_mock_topic_description(partitions)
    mock_client.describe_topics.return_value = {
        "test-topic": _create_mock_future(topic_description)
    }

    # Set up mock offset results
    earliest_offsets = {
        0: _create_mock_future(_create_mock_offset_result(0)),
        1: _create_mock_future(_create_mock_offset_result(5)),
    }

    latest_offsets = {
        0: _create_mock_future(_create_mock_offset_result(100)),
        1: _create_mock_future(_create_mock_offset_result(150)),
    }

    # Set up side effect to return appropriate offset
    def list_offsets_side_effect(
        offset_specs: dict[TopicPartition, Any],
    ) -> dict[TopicPartition, Mock]:
        result = {}

        for tp, offset_spec in offset_specs.items():
            # Check if we're asking for earliest or latest offsets
            if offset_spec == OffsetSpec.earliest():
                result[tp] = earliest_offsets[tp.partition]
            else:
                result[tp] = latest_offsets[tp.partition]

        return result

    mock_client.list_offsets.side_effect = list_offsets_side_effect

    result = list_offsets(mock_client, "test-topic")

    # Verify the result
    assert len(result) == 2

    assert result[0] == {
        "topic": "test-topic",
        "partition": 0,
        "earliest_offset": 0,
        "latest_offset": 100,
    }

    assert result[1] == {
        "topic": "test-topic",
        "partition": 1,
        "earliest_offset": 5,
        "latest_offset": 150,
    }

    # Verify that calls were made
    mock_client.describe_topics.assert_called_once()
    assert mock_client.list_offsets.call_count == 2


def test_list_offsets_nonexistent_topic() -> None:
    """Test listing offsets for a topic that doesn't exist."""
    mock_client = Mock()

    # Mock 'describe_topics' to raise 'KafkaException'
    mock_topic_future = Mock()
    mock_topic_future.result.side_effect = KafkaException("Topic not found")

    mock_client.describe_topics.return_value = {"nonexistent-topic": mock_topic_future}

    message = "Topic 'nonexistent-topic' does not exist or cannot be accessed"
    with pytest.raises(ValueError, match=message):
        list_offsets(mock_client, "nonexistent-topic")


def test_describe_topic_configs() -> None:
    expected = [
        {
            "config": "segment.bytes",
            "value": "300000",
            "source": "DYNAMIC_TOPIC_CONFIG",
            "isDefault": True,
            "isReadOnly": False,
            "topic": "test_topic",
        },
    ]

    # admin client mock
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.topics = {"test_topic": Mock()}
    mock_client.describe_configs.return_value = Mock()

    # mocking the future returned as the config value of describe_configs
    conf_value_mock = Mock()
    conf_value_mock.value = "300000"
    conf_value_mock.is_default = True
    conf_value_mock.is_read_only = False
    conf_value_mock.source = ConfigSource.DYNAMIC_TOPIC_CONFIG

    # mocking the config returned by describe_configs
    conf_mock = Mock()
    conf_mock.result.return_value = {"segment.bytes": conf_value_mock}
    mock_client.describe_configs.return_value.items.return_value = [
        (ConfigResource(ConfigResource.Type.TOPIC, "test_topic"), conf_mock)
    ]

    result = describe_topic_configs(mock_client)
    mock_client.describe_configs.assert_called_once()
    assert result == expected
