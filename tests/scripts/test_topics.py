import json
from pathlib import Path
from unittest.mock import patch

from sentry_kafka_management.scripts.topics import list_topics


def test_list_topics(temp_config: Path) -> None:
    with patch(
        "sentry_kafka_management.scripts.topics.list_topics_action",
    ) as mock_action, patch("builtins.print") as mock_print:

        mock_topics = [
            "topic1",
            "topic2",
        ]
        mock_action.return_value = mock_topics
        custom_argv = [
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1"
        ]
        result = list_topics(custom_argv)

        assert result == 0
        mock_action.assert_called_once()
        mock_print.assert_called_once()
        printed_output = mock_print.call_args[0][0]
        parsed_output = json.loads(printed_output)
        assert parsed_output == mock_topics
