from unittest.mock import Mock, patch

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AlterConfigOpType,
    ConfigResource,
    ConfigSource,
)

from sentry_kafka_management.actions.brokers import (
    ConfigChange,
    _update_configs,
    apply_configs,
    describe_broker_configs,
    remove_dynamic_configs,
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


def test_update_config_apply() -> None:
    """Test _update_configs() can set configs successfully."""
    mock_client = Mock()

    def mock_incremental_alter(resources: list[ConfigResource]) -> dict[ConfigResource, Mock]:
        futures_dict = {}
        for resource in resources:
            future_mock = Mock()
            future_mock.result.return_value = None
            futures_dict[resource] = future_mock
        return futures_dict

    mock_client.incremental_alter_configs.side_effect = mock_incremental_alter

    config_changes = [
        ConfigChange(
            broker_id="0",
            config_name="message.max.bytes",
            old_value="1000000",
            new_value="2000000",
        )
    ]
    success, error = _update_configs(
        mock_client,
        config_changes,
        AlterConfigOpType.SET,
    )

    assert len(success) == 1
    assert len(error) == 0
    assert success[0]["status"] == "success"
    assert success[0]["broker_id"] == "0"
    assert success[0]["config_name"] == "message.max.bytes"
    assert success[0]["old_value"] == "1000000"
    assert success[0]["new_value"] == "2000000"
    mock_client.incremental_alter_configs.assert_called_once()


def test_apply_configs_success() -> None:
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers._update_configs") as mock_update,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
    ):
        # _update_configs has to return something
        mock_update.return_value = ([], [])

        current_configs = [
            {
                "config": "message.max.bytes",
                "value": "1000000",
                "source": "STATIC_BROKER_CONFIG",
                "isDefault": False,
                "isReadOnly": False,
                "broker": "0",
            }
        ]
        mock_describe_broker_configs.return_value = current_configs
        apply_configs(
            mock_client, config_changes={"message.max.bytes": "2000000"}, broker_ids=["0"]
        )
        mock_update.assert_called_once_with(
            admin_client=mock_client,
            config_changes=[
                ConfigChange(
                    broker_id="0",
                    config_name="message.max.bytes",
                    old_value="1000000",
                    new_value="2000000",
                )
            ],
            update_type=AlterConfigOpType.SET,
        )


def test_apply_config_validation() -> None:
    """Test that applies to read-only configs are rejected."""
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers.describe_cluster") as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
        patch("sentry_kafka_management.actions.brokers._update_configs") as mock_update,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]
        mock_update.return_value = ([], [])

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
        success, error = apply_configs(mock_client, config_changes, ["0"])

        assert len(success) == 0
        assert len(error) == 1
        assert error[0]["status"] == "error"
        mock_client.incremental_alter_configs.assert_not_called()


def test_remove_dynamic_configs_success() -> None:
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers._update_configs") as mock_update,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
        patch("sentry_kafka_management.actions.brokers.describe_cluster") as mock_describe_cluster,
    ):
        # _update_configs has to return something
        mock_update.return_value = ([], [])
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]

        current_configs = [
            {
                "config": "message.max.bytes",
                "value": "1000000",
                "source": "DYNAMIC_BROKER_CONFIG",
                "isDefault": False,
                "isReadOnly": False,
                "broker": "0",
            }
        ]
        mock_describe_broker_configs.return_value = current_configs
        remove_dynamic_configs(
            mock_client, configs_to_remove=["message.max.bytes"], broker_ids=["0"]
        )
        mock_update.assert_called_once_with(
            admin_client=mock_client,
            config_changes=[
                ConfigChange(
                    broker_id="0",
                    config_name="message.max.bytes",
                    old_value="1000000",
                    new_value=None,
                )
            ],
            update_type=AlterConfigOpType.DELETE,
        )


def test_remove_dynamic_configs_validation() -> None:
    """Test that deletes of non-dynamic configs are rejected."""
    mock_client = Mock()

    with (
        patch("sentry_kafka_management.actions.brokers.describe_cluster") as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.brokers.describe_broker_configs"
        ) as mock_describe_broker_configs,
        patch("sentry_kafka_management.actions.brokers._update_configs") as mock_update,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]
        mock_update.return_value = ([], [])

        mock_describe_broker_configs.return_value = [
            {
                "config": "max.message.bytes",
                "value": "/var/kafka/logs",
                "source": "STATIC_BROKER_CONFIG",  # Not dynamic!
                "isDefault": False,
                "isReadOnly": False,
                "broker": "0",
            }
        ]

        success, error = remove_dynamic_configs(mock_client, ["max.message.bytes"], ["0"])

        assert len(success) == 0
        assert len(error) == 1
        assert error[0]["status"] == "error"
        mock_client.incremental_alter_configs.assert_not_called()
