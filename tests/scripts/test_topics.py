import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics import list_topics


@patch("sentry_kafka_management.scripts.topics.list_topics_action")
@patch("sentry_kafka_management.scripts.topics.get_admin_client")
def test_list_topics(mock_get_admin: MagicMock, mock_action: MagicMock, temp_config: Path) -> None:
    mock_topics = [
        "topic1",
        "topic2",
    ]
    mock_action.return_value = mock_topics

    runner = CliRunner()
    result = runner.invoke(list_topics, ["--config", str(temp_config), "--cluster", "cluster1"])

    assert result.exit_code == 0
    mock_action.assert_called_once()
    parsed_output = json.loads(result.output)
    assert parsed_output == mock_topics
