from pathlib import Path
from unittest.mock import patch, Mock

from sentry_kafka_management.cli import main as cli


# TODO: we should turn this into a parameterized test case
# since 90% of the test for each subcommand is the same
def test_cli(temp_config: Path) -> None:
    mock_list_topics = Mock()
    mock_describe_configs = Mock()
    mock_functions = {
        "get-topics": mock_list_topics,
        "describe-broker-configs": mock_describe_configs,
    }
    with patch(
        "sentry_kafka_management.cli.FUNCTIONS",
        mock_functions,
    ):
        res = cli([
            "get-topics",
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1"
        ])
        mock_list_topics.assert_called_once_with([
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1"
        ])
        assert res == 0

        res = cli([
            "describe-broker-configs",
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1"
        ])
        mock_describe_configs.assert_called_once_with([
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1"
        ])
        assert res == 0
