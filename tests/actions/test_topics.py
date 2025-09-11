from unittest.mock import Mock
from sentry_kafka_management.actions.topics import list_topics


def test_list_topics() -> None:
    """Test listing topics."""
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.topics = {"test_topic": Mock()}

    result = list_topics(mock_client)
    mock_client.list_topics.assert_called_once()
    assert result == ["test_topic"]
