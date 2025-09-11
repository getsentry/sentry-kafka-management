import json
from pathlib import Path
from unittest.mock import patch

from sentry_kafka_management.scripts.brokers import describe_broker_configs


def test_describe_broker_configs(temp_clusters_config: Path, temp_topics_config: Path) -> None:
    with patch(
        "sentry_kafka_management.scripts.brokers.describe_broker_configs_action",
    ) as mock_action, patch("builtins.print") as mock_print:
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
        custom_argv = [
            "--cluster-config",
            str(temp_clusters_config),
            "--topic-config",
            str(temp_topics_config),
            "--cluster",
            "cluster1"
        ]
        result = describe_broker_configs(custom_argv)

        assert result == 0
        mock_action.assert_called_once()
        mock_print.assert_called_once()
        printed_output = mock_print.call_args[0][0]
        parsed_output = json.loads(printed_output)
        assert parsed_output == mock_configs
