import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics import list_topics


@patch("sentry_kafka_management.scripts.topics.get_admin_client")
def test_list_topics(mock_get_admin: MagicMock, temp_config: Path) -> None:
    mock_client = MagicMock()
    mock_client.list_topics.return_value.topics = {
        "topic1": MagicMock(),
        "topic2": MagicMock(),
    }
    mock_get_admin.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(list_topics, ["--config", str(temp_config), "--cluster", "cluster1"])

    assert result.exit_code == 0
    parsed_output = json.loads(result.output)
    assert parsed_output == ["topic1", "topic2"]
