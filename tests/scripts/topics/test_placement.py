from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.topics.placement import compute_topic_placement


@patch("sentry_kafka_management.scripts.topics.placement.describe_cluster")
@patch("sentry_kafka_management.scripts.topics.placement.get_admin_client")
def test_broker_not_found_in_cluster(
    mock_get_admin: MagicMock,
    mock_describe_cluster: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_describe_cluster.return_value = [
        {"id": "0", "host": "other-broker", "port": 9092},
    ]

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        result = runner.invoke(
            compute_topic_placement,
            ["--config", str(temp_config), "--cluster", "cluster1", "--output-dir", tmp],
        )

    assert result.exit_code == 1
    assert "not found in cluster" in result.output


@patch("sentry_kafka_management.scripts.topics.placement.describe_topic_partitions_action")
@patch("sentry_kafka_management.scripts.topics.placement.list_topics_action")
@patch("sentry_kafka_management.scripts.topics.placement.describe_cluster")
@patch("sentry_kafka_management.scripts.topics.placement.get_admin_client")
def test_output_yaml_format(
    mock_get_admin: MagicMock,
    mock_describe_cluster: MagicMock,
    mock_list_topics: MagicMock,
    mock_describe_partitions: MagicMock,
    temp_config: Path,
) -> None:
    mock_get_admin.return_value = MagicMock()
    mock_describe_cluster.return_value = [
        {"id": "0", "host": "kafka-0.zone-a.c.project.internal", "port": 9092},
        {"id": "1", "host": "kafka-1.zone-b.c.project.internal", "port": 9092},
        {"id": "2", "host": "kafka-2.zone-c.c.project.internal", "port": 9092},
    ]

    mock_list_topics.return_value = ["events"]
    mock_describe_partitions.return_value = [{"topic": "events", "id": i} for i in range(32)]

    runner = CliRunner()
    with runner.isolated_filesystem() as tmp:
        result = runner.invoke(
            compute_topic_placement,
            ["--config", str(temp_config), "--cluster", "cluster1", "--output-dir", tmp],
        )

        assert result.exit_code == 0, result.output

        output_file = Path(tmp) / "events.yaml"
        assert output_file.exists()
        content = output_file.read_text()

    lines = content.strip().split("\n")
    assert lines[0] == "cluster: cluster1"
    assert lines[1] == "partitions: 32"
    assert lines[2] == "placement:"
    assert lines[3] == "  staticAssignments:"
    assert lines[-1] == "  strategy: static"
    assert content.endswith("\n")

    assignments = lines[4:-1]
    assert len(assignments) == 32
    for assignment in assignments:
        assert assignment.startswith("  - [")
        assert assignment.endswith("]")
