import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics.describe import (
    describe_topic_partitions,
    list_offsets,
    list_topics,
)


@patch("sentry_kafka_management.scripts.topics.describe.get_admin_client")
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


@patch("sentry_kafka_management.scripts.topics.describe.get_admin_client")
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


@patch("sentry_kafka_management.scripts.topics.describe.get_admin_client")
def test_describe_topic_partitions(mock_get_admin: MagicMock, temp_config: Path) -> None:
    mock_client = MagicMock()
    mock_partition = MagicMock()
    mock_partition.id = 0
    mock_partition.leader = MagicMock()
    mock_partition.leader.id = 1
    mock_partition.replicas = [MagicMock(id=1), MagicMock(id=2)]
    mock_partition.isr = [MagicMock(id=1), MagicMock(id=2)]

    mock_topic_result = MagicMock()
    mock_topic_result.name = "topic1"
    mock_topic_result.partitions = [mock_partition]

    mock_future = MagicMock()
    mock_future.result.return_value = mock_topic_result
    mock_client.describe_topics.return_value = {"topic1": mock_future}
    mock_get_admin.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(
        describe_topic_partitions,
        ["--config", str(temp_config), "--cluster", "cluster1", "--topic", "topic1"],
    )

    assert result.exit_code == 0
    parsed_output = json.loads(result.output)
    assert len(parsed_output) == 1
    assert parsed_output[0]["topic"] == "topic1"
    assert parsed_output[0]["id"] == 0
    assert parsed_output[0]["leader"] == 1
    assert parsed_output[0]["replicas"] == [1, 2]
    assert parsed_output[0]["isr"] == [1, 2]
