from unittest.mock import Mock, patch

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    ConfigResource,
    ConfigSource,
)

from sentry_kafka_management.actions.brokers import (
    apply_config,
    describe_broker_configs,
)


def test_describe_broker_configs() -> None:
    """Test describing broker configs."""
    expected = [
        {
            "config": "num.network.threads",
            "value": "3",
            "source": "DYNAMIC_BROKER_CONFIG",
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
    conf_value_mock.source = ConfigSource.DYNAMIC_BROKER_CONFIG

    # mocking the config returned by describe_configs
    conf_mock = Mock()
    conf_mock.result.return_value = {"num.network.threads": conf_value_mock}
    mock_client.describe_configs.return_value.items.return_value = [
        (ConfigResource(ConfigResource.Type.BROKER, "0"), conf_mock)
    ]

    result = describe_broker_configs(mock_client)
    mock_client.describe_configs.assert_called_once()
    assert result == expected


def test_apply_config_success() -> None:
    """Test successful config application."""
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers.describe_cluster") as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]

        mock_describe_broker_configs.return_value = [
            {
                "config": "message.max.bytes",
                "value": "1000000",
                "source": "DEFAULT_CONFIG",
                "isDefault": True,
                "isReadOnly": False,
                "broker": "0",
            }
        ]

        def mock_incremental_alter(resources: list[ConfigResource]) -> dict[ConfigResource, Mock]:
            futures_dict = {}
            for resource in resources:
                future_mock = Mock()
                future_mock.result.return_value = None
                futures_dict[resource] = future_mock
            return futures_dict

        mock_client.incremental_alter_configs.side_effect = mock_incremental_alter

        config_changes = {"message.max.bytes": "2000000"}
        success, error = apply_config(mock_client, config_changes, ["0"])

        assert len(success) == 1
        assert len(error) == 0
        assert success[0]["status"] == "success"
        assert success[0]["broker_id"] == "0"
        assert success[0]["config_name"] == "message.max.bytes"
        assert success[0]["old_value"] == "1000000"
        assert success[0]["new_value"] == "2000000"
        mock_client.incremental_alter_configs.assert_called_once()


def test_apply_config_readonly() -> None:
    """Test that read-only configs are rejected."""
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers.describe_cluster") as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]

        mock_describe_broker_configs.return_value = [
            {
                "config": "log.dir",
                "value": "/var/kafka/logs",
                "source": "STATIC_BROKER_CONFIG",
                "isDefault": False,
                "isReadOnly": True,  # Read-only!
                "broker": "0",
            }
        ]

        config_changes = {"log.dir": "/new/path"}
        success, error = apply_config(mock_client, config_changes, ["0"])

        assert len(success) == 0
        assert len(error) == 1
        assert error[0]["status"] == "error"
        assert "read only" in error[0]["error"]
        mock_client.incremental_alter_configs.assert_not_called()
