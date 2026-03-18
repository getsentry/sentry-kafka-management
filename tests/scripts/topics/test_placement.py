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
        {"id": "0", "host": "other-broker", "port": 9092, "rack": "zone-a"},
    ]

    runner = CliRunner()
    result = runner.invoke(
        compute_topic_placement, ["--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code != 0
