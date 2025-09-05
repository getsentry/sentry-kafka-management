from unittest.mock import Mock, patch

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConfigResource,
)

from sentry_kafka_management.actions.brokers import describe_broker_configs
from sentry_kafka_management.brokers import ClusterConfig


@patch("sentry_kafka_management.actions.brokers.get_admin_client")
def test_describe_broker_configs(mock_admin_client: Mock) -> None:
    """Test listing topics."""
    expected = [
        {
            "config": "num.network.threads",
            "value": "3",
            "isDefault": True,
            "isReadOnly": False,
            "broker": "0",
        },
    ]

    # admin client mock
    mock_client = Mock()
    mock_client.list_topics.return_value = Mock()
    mock_client.list_topics.return_value.brokers = {"0": Mock()}
    mock_client.describe_configs.return_value = Mock()

    # mocking the future returned as the config value of describe_configs
    conf_value_mock = Mock()
    conf_value_mock.value = "3"
    conf_value_mock.is_default = True
    conf_value_mock.is_read_only = False

    # mocking the config returned by describe_configs
    conf_mock = Mock()
    conf_mock.result.return_value = {"num.network.threads": conf_value_mock}
    mock_client.describe_configs.return_value.items.return_value = [
        (ConfigResource(ConfigResource.Type.BROKER, "0"), conf_mock)
    ]

    mock_admin_client.return_value = mock_client

    cluster_config: ClusterConfig = {
        "brokers": ["broker1:9092", "broker2:9092"],
        "security_protocol": "PLAINTEXT",
        "sasl_mechanism": None,
        "sasl_username": None,
        "sasl_password": None
    }

    result = describe_broker_configs(cluster_config)
    mock_client.describe_configs.assert_called_once()
    assert result == expected
