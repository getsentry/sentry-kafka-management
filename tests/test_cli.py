import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import click.testing

from sentry_kafka_management.cli import main as cli


@patch("sentry_kafka_management.scripts.topics.describe.list_topics_action")
@patch("sentry_kafka_management.scripts.topics.describe.get_admin_client")
def test_cli_topics_list(
    mock_get_admin: MagicMock, mock_action: MagicMock, temp_config: Path
) -> None:
    """Test the topics list command."""
    mock_topics = ["topic1", "topic2"]
    mock_action.return_value = mock_topics

    runner = click.testing.CliRunner()
    result = runner.invoke(
        cli, ["list-topics", "--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code == 0
    mock_action.assert_called_once()
    parsed_output = json.loads(result.output)
    assert parsed_output == mock_topics


@patch("sentry_kafka_management.scripts.brokers.configs.describe_broker_configs_action")
@patch("sentry_kafka_management.scripts.brokers.configs.get_admin_client")
def test_cli_brokers_describe_configs(
    mock_get_admin: MagicMock, mock_action: MagicMock, temp_config: Path
) -> None:
    """Test the brokers describe-configs command."""
    mock_configs = [
        {
            "config": "num.network.threads",
            "value": "3",
            "source": "DYNAMIC_BROKER_CONFIG",
            "isDefault": True,
            "isReadOnly": False,
            "broker": "0",
        },
    ]
    mock_action.return_value = mock_configs

    runner = click.testing.CliRunner()
    result = runner.invoke(
        cli, ["describe-broker-configs", "--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code == 0
    mock_action.assert_called_once()
    parsed_output = json.loads(result.output)
    assert parsed_output == mock_configs
