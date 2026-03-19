import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics.partitions import elect_partition_leaders


@patch("sentry_kafka_management.scripts.topics.partitions.get_admin_client")
@patch("sentry_kafka_management.scripts.topics.partitions.elect_partition_leaders_action")
def test_elect_partition_leaders_cli(
    mock_action: MagicMock,
    mock_get_admin: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_action.return_value = (
        [{"topic": "t1", "id": 0}],
        [{"topic": "t1", "id": 1, "error": "some error"}],
    )

    runner = CliRunner()
    result = runner.invoke(
        elect_partition_leaders,
        ["--config", str(temp_config), "--cluster", "cluster1"],
    )

    assert result.exit_code == 0
    mock_action.assert_called_once_with(
        admin_client=mock_get_admin.return_value,
        topics=(),
    )
    parsed = json.loads(result.output)
    assert parsed == [
        [{"topic": "t1", "id": 0}],
        [{"topic": "t1", "id": 1, "error": "some error"}],
    ]


@patch("sentry_kafka_management.scripts.topics.partitions.get_admin_client")
@patch("sentry_kafka_management.scripts.topics.partitions.elect_partition_leaders_action")
def test_elect_partition_leaders_cli_with_topics(
    mock_action: MagicMock,
    mock_get_admin: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_action.return_value = ([{"topic": "a", "id": 0}, {"topic": "b", "id": 0}], [])

    runner = CliRunner()
    result = runner.invoke(
        elect_partition_leaders,
        [
            "--config",
            str(temp_config),
            "--cluster",
            "cluster1",
            "--topic",
            "a",
            "--topic",
            "b",
        ],
    )

    assert result.exit_code == 0
    mock_action.assert_called_once_with(
        admin_client=mock_get_admin.return_value,
        topics=("a", "b"),
    )
    parsed = json.loads(result.output)
    assert parsed == [
        [
            {"topic": "a", "id": 0},
            {"topic": "b", "id": 0},
        ],
        [],
    ]
