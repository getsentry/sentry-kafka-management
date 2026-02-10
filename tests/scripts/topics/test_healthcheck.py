import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.actions.topics.healthcheck import (
    HealthResponse,
    HealthResponseReason,
)
from sentry_kafka_management.scripts.topics.healthcheck import (
    HealthcheckTimeoutError,
    healthcheck_cluster_topics,
)


@patch("sentry_kafka_management.scripts.topics.healthcheck.get_admin_client")
def test_healthcheck_cluster_topics(mock_get_admin: MagicMock, temp_config: Path) -> None:
    mock_client = MagicMock()
    mock_client.list_topics.return_value.topics = {
        "topic1": MagicMock(),
        "topic2": MagicMock(),
    }
    mock_client.describe_topic_partitions.side_effect = [
        [
            {
                "topic": "topic1",
                "id": "0",
                "leader": "0",
                "replicas": ["0", "1", "2"],
                "isr": ["2", "1", "0"],
            }
        ],
        [
            {
                "topic": "topic2",
                "id": "0",
                "leader": "2",
                "replicas": ["2", "1", "0"],
                "isr": ["2", "0", "1"],
            }
        ],
    ]
    mock_get_admin.return_value = mock_client

    runner = CliRunner()
    result = runner.invoke(
        healthcheck_cluster_topics, ["--config", str(temp_config), "--cluster", "cluster1"]
    )

    assert result.exit_code == 0
    parsed_output = json.loads(result.output)
    assert parsed_output == {"healthy": True, "reason": [HealthResponseReason.healthy()]}


@patch("sentry_kafka_management.scripts.topics.healthcheck.healthcheck_cluster_topics_actions")
@patch("sentry_kafka_management.scripts.topics.healthcheck.get_admin_client")
@patch("sentry_kafka_management.scripts.topics.healthcheck.time.time", side_effect=[0, 1, 2])
@patch("sentry_kafka_management.scripts.topics.healthcheck.time.sleep")
def test_healthcheck_cluster_topics_timeout(
    mock_sleep: MagicMock,
    mock_time: MagicMock,
    mock_get_admin: MagicMock,
    mock_healthcheck_actions: MagicMock,
    temp_config: Path,
) -> None:
    # Action always returns unhealthy so the loop runs until timeout
    unhealthy = HealthResponse(
        healthy=False,
        reason=[HealthResponseReason.outside_isr([])],
    )
    mock_healthcheck_actions.return_value = unhealthy

    runner = CliRunner()
    result = runner.invoke(
        healthcheck_cluster_topics,
        # start_time=0, timeout=2, meaning healthcheck loop runs once before timing out
        ["--config", str(temp_config), "--cluster", "cluster1", "--timeout", "2"],
    )

    assert result.exit_code != 0
    assert result.exception is not None
    assert isinstance(result.exception, HealthcheckTimeoutError)
