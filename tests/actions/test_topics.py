from unittest.mock import Mock, patch
from sentry_kafka_management.actions.topics import list_topics
from sentry_kafka_management.brokers import ClusterConfig


@patch("sentry_kafka_management.actions.topics.get_admin_client")
def test_list_topics(mock_admin_client: Mock) -> None:
    """Test listing topics."""
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.topics = {"test_topic": Mock()}
    mock_admin_client.return_value = mock_client

    cluster_config: ClusterConfig = {
        "brokers": ["broker1:9092", "broker2:9092"],
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": None,
        "sasl_username": None,
        "sasl_password": None
    }

    result = list_topics(cluster_config)
    mock_client.list_topics.assert_called_once()
    assert result == ["test_topic"]
