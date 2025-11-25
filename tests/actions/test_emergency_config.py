from unittest.mock import Mock, patch

from sentry_kafka_management.actions.emergency_config import apply_emergency_config


def test_apply_emergency_config_success() -> None:
    """Test successful emergency config application."""
    # Setup mock admin client
    mock_client = Mock()

    # Mock describe_cluster (for default broker_ids)
    with (
        patch(
            "sentry_kafka_management.actions.emergency_config.describe_cluster"
        ) as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.emergency_config.describe_broker_configs"
        ) as mock_describe_broker_configs,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]

        # Mock current config
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

        # Mock incremental_alter_configs - it needs to return the actual resources passed to it
        def mock_incremental_alter(resources):
            futures_dict = {}
            for resource in resources:
                future_mock = Mock()
                future_mock.result.return_value = None
                futures_dict[resource] = future_mock
            return futures_dict

        mock_client.incremental_alter_configs.side_effect = mock_incremental_alter

        # Execute
        config_changes = {"message.max.bytes": "2000000"}
        results = apply_emergency_config(mock_client, config_changes, ["0"])

        # Assert
        assert len(results) == 1
        assert results[0]["status"] == "success"
        assert results[0]["broker_id"] == "0"
        assert results[0]["config_name"] == "message.max.bytes"
        assert results[0]["old_value"] == "1000000"
        assert results[0]["new_value"] == "2000000"
        mock_client.incremental_alter_configs.assert_called_once()


def test_apply_emergency_config_readonly() -> None:
    """Test that read-only configs are rejected."""
    mock_client = Mock()

    with (
        patch(
            "sentry_kafka_management.actions.emergency_config.describe_cluster"
        ) as mock_describe_cluster,
        patch(
            "sentry_kafka_management.actions.emergency_config.describe_broker_configs"
        ) as mock_describe_broker_configs,
    ):
        mock_describe_cluster.return_value = [
            {"id": "0", "host": "localhost", "port": 9092, "rack": None, "isController": True}
        ]

        # Mock read-only config
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
        results = apply_emergency_config(mock_client, config_changes, ["0"])

        assert len(results) == 1
        assert results[0]["status"] == "error"
        assert "read only" in results[0]["error"]
        mock_client.incremental_alter_configs.assert_not_called()
