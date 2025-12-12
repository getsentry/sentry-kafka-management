from unittest.mock import Mock

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConfigResource,
    ConfigSource,
)

from sentry_kafka_management.actions.topics import describe_topic_configs, list_topics


def test_list_topics() -> None:
    """Test listing topics."""
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.topics = {"test_topic": Mock()}

    result = list_topics(mock_client)
    mock_client.list_topics.assert_called_once()
    assert result == ["test_topic"]


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
