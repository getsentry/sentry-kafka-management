import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.clusters import (
    describe_cluster,
    get_cluster_controller,
)


@patch("sentry_kafka_management.scripts.clusters.get_admin_client")
@patch("sentry_kafka_management.scripts.clusters.describe_cluster_action")
def test_describe_cluster(
    mock_describe_action: MagicMock,
    mock_get_admin: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_describe_action.return_value = [
        {
            "id": "0",
            "host": "test-broker-0",
            "port": 9092,
            "rack": "a",
            "isController": True,
        },
        {
            "id": "1",
            "host": "test-broker-1",
            "port": 9092,
            "rack": "b",
            "isController": False,
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        describe_cluster, ["--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code == 0
    parsed_output = json.loads(result.output)
    assert len(parsed_output) == 2
    assert parsed_output[0]["id"] == "0"
    assert parsed_output[0]["isController"] is True
    assert parsed_output[1]["id"] == "1"
    assert parsed_output[1]["isController"] is False
    mock_describe_action.assert_called_once()


@patch("sentry_kafka_management.scripts.clusters.get_admin_client")
@patch("sentry_kafka_management.scripts.clusters.get_cluster_controller_action")
def test_get_cluster_controller(
    mock_get_controller_action: MagicMock,
    mock_get_admin: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_get_controller_action.return_value = "1"

    runner = CliRunner()
    result = runner.invoke(
        get_cluster_controller, ["--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code == 0
    assert result.output.strip() == "1"
    mock_get_controller_action.assert_called_once()
