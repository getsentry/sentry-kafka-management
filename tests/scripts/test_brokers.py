import json
from pathlib import Path
from unittest.mock import patch

import click.testing

from sentry_kafka_management.scripts.brokers import describe_broker_configs


def test_describe_broker_configs(temp_config: Path) -> None:
    with patch(
        "sentry_kafka_management.scripts.brokers.describe_broker_configs_action",
    ) as mock_action:
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
        result = runner.invoke(describe_broker_configs, [
            "--config", str(temp_config),
            "--cluster", "cluster1"
        ])

        assert result.exit_code == 0
        mock_action.assert_called_once()
        printed_output = result.output
        parsed_output = json.loads(printed_output)
        assert parsed_output == mock_configs
