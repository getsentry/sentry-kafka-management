from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.brokers_local.filesystem import (
    remove_recorded_dynamic_configs,
)


def test_remove_recorded_config_command() -> None:
    """Test the CLI command for recorded config removal."""
    runner = CliRunner()

    with runner.isolated_filesystem():
        with open("test.yml", "w") as f:
            f.write("test: config")
        dir_path = Path.cwd() / "emergency-configs"
        dir_path.mkdir(exist_ok=True)

        with (
            patch(
                "sentry_kafka_management.scripts.brokers_local.filesystem.get_cluster_config"
            ) as mock_get_cluster,
            patch(
                "sentry_kafka_management.scripts.brokers_local.filesystem.get_admin_client"
            ) as mock_get_client,
            patch(
                "sentry_kafka_management.scripts.brokers_local.filesystem.remove_dynamic_configs_action"  # noqa: E501
            ) as mock_action,
            patch(
                "sentry_kafka_management.scripts.brokers_local.filesystem.read_record_dir"
            ) as mock_read,
            patch("sentry_kafka_management.scripts.brokers_local.filesystem.cleanup_config_record"),
        ):
            mock_get_cluster.return_value = {}
            mock_get_client.return_value = Mock()

            mock_read.return_value = {"message.max.bytes": "1000000"}
            mock_action.return_value = (
                [
                    {
                        "broker_id": "0",
                        "config_name": "message.max.bytes",
                        "status": "success",
                        "old_value": "1000000",
                        "new_value": None,
                    }
                ],
                [],
            )
            result = runner.invoke(
                remove_recorded_dynamic_configs,
                [
                    "-c",
                    "test.yml",
                    "-n",
                    "test-cluster",
                    "--configs-record-dir",
                    dir_path.as_posix(),
                    "--broker-ids",
                    "0",
                    "--cleanup-records",
                ],
            )
            assert result.exit_code == 0
            mock_action.assert_called_once()
