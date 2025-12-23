import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics import list_offsets, list_topics


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


@patch("sentry_kafka_management.scripts.topics.get_admin_client")
def test_list_offsets(mock_get_admin: MagicMock, temp_config: Path) -> None:
    mock_client = MagicMock()
    mock_partition = MagicMock()
    mock_partition.id = 0

    mock_topic_description = MagicMock()
    mock_topic_description.partitions = [mock_partition]

    mock_future = MagicMock()
    mock_future.result.return_value = mock_topic_description

    mock_client.describe_topics.return_value = {"topic1": mock_future}

    def list_offsets_side_effect(offset_specs: dict[Any, Any]) -> dict[Any, Any]:
        result = {}

        for tp in offset_specs:
            mock_offset_result = MagicMock()
            mock_offset_result.result.return_value.offset = 100
            result[tp] = mock_offset_result

        return result

    mock_client.list_offsets.side_effect = list_offsets_side_effect
    mock_get_admin.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(
        list_offsets,
        ["--config", str(temp_config), "--cluster", "cluster1", "--topic", "topic1"],
    )

    assert result.exit_code == 0
    parsed_output = json.loads(result.output)
    assert len(parsed_output) == 1
    assert parsed_output[0]["topic"] == "topic1"
    assert parsed_output[0]["partition"] == 0
