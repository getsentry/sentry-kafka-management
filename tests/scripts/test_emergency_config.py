from unittest.mock import Mock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.emergency_config import apply_emergency_config


def test_apply_emergency_config_command_success() -> None:
    """Test the CLI command with successful config application."""
    runner = CliRunner()

    with runner.isolated_filesystem():
        # Create a temporary test.yml file
        with open("test.yml", "w") as f:
            f.write("test: config")

        with (
            patch(
                "sentry_kafka_management.scripts.emergency_config.YamlKafkaConfig"
            ) as mock_yaml_config,
            patch(
                "sentry_kafka_management.scripts.emergency_config.get_admin_client"
            ) as mock_get_client,
            patch(
                "sentry_kafka_management.scripts.emergency_config.apply_emergency_config_action"
            ) as mock_action,
        ):
            mock_yaml_config.return_value.get_clusters.return_value = {"test-cluster": {}}
            mock_get_client.return_value = Mock()

            mock_action.return_value = [
                {
                    "broker_id": "0",
                    "config_name": "message.max.bytes",
                    "status": "success",
                    "old_value": "1000000",
                    "new_value": "2000000",
                }
            ]

            result = runner.invoke(
                apply_emergency_config,
                [
                    "-c",
                    "test.yml",
                    "-n",
                    "test-cluster",
                    "--config-changes",
                    "message.max.bytes=2000000",
                    "--broker-ids",
                    "0",
                ],
            )

            assert result.exit_code == 0
            assert "success" in result.output.lower()
            mock_action.assert_called_once()


def test_apply_emergency_config_command_failure() -> None:
    """Test the CLI command with failed config application."""
    runner = CliRunner()

    with (
        patch(
            "sentry_kafka_management.scripts.emergency_config.YamlKafkaConfig"
        ) as mock_yaml_config,
        patch(
            "sentry_kafka_management.scripts.emergency_config.get_admin_client"
        ) as mock_get_client,
        patch(
            "sentry_kafka_management.scripts.emergency_config.apply_emergency_config_action"
        ) as mock_action,
    ):
        mock_yaml_config.return_value.get_clusters.return_value = {"test-cluster": {}}
        mock_get_client.return_value = Mock()

        mock_action.return_value = [
            {
                "broker_id": "0",
                "config_name": "invalid.config",
                "status": "error",
                "error": "Config 'invalid.config' not found on broker 0",
            }
        ]

        result = runner.invoke(
            apply_emergency_config,
            [
                "-c",
                "test.yml",
                "-n",
                "test-cluster",
                "--config-changes",
                "invalid.config=value",
            ],
        )

        assert result.exit_code != 0
        assert "error" in result.output.lower()
