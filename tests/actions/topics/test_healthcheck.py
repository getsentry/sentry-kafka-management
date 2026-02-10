from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sentry_kafka_management.actions.topics.healthcheck import (
    HealthResponse,
    HealthResponseReason,
    Partition,
    healthcheck_cluster_topics,
)

# Used in tests to pass a Partition to HealthResponseReason
TOPIC1_0_PARTITION = Partition("topic1", "0", "0", ["0", "1", "2"], ["0", "1", "2"])
TOPIC2_0_PARTITION = Partition("topic2", "0", "0", ["0", "1", "2"], ["0", "1", "2"])


@pytest.mark.parametrize(
    "topics, partitions_side_effect, expected",
    [
        pytest.param(
            ["topic1", "topic2"],
            [
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
            ],
            HealthResponse(healthy=True, reason=[HealthResponseReason.healthy()]),
            id="healthy_topics",
        ),
        pytest.param(
            ["topic1"],
            [
                [
                    {
                        "topic": "topic1",
                        "id": "0",
                        "leader": "0",
                        "replicas": ["0", "1", "2"],
                        "isr": ["2", "1", "0"],
                    },
                    {
                        "topic": "topic1",
                        "id": "1",
                        "leader": "2",
                        "replicas": ["2", "3", "4"],
                        "isr": ["2", "4", "3"],
                    },
                    {
                        "topic": "topic1",
                        "id": "2",
                        "leader": "6",
                        "replicas": ["6", "4", "5"],
                        "isr": ["4", "6", "5"],
                    },
                ],
            ],
            HealthResponse(healthy=True, reason=[HealthResponseReason.healthy()]),
            id="healthy_topics_multiple_partitions",
        ),
        pytest.param(
            ["topic1"],
            [
                [
                    {
                        "topic": "topic1",
                        "id": "0",
                        "leader": "0",
                        "replicas": ["0", "1", "2"],
                        "isr": ["2", "0"],
                    }
                ],
            ],
            HealthResponse(
                healthy=False,
                reason=[HealthResponseReason.outside_isr([TOPIC1_0_PARTITION])],
            ),
            id="not_enough_isr",
        ),
        pytest.param(
            ["topic1"],
            [
                [
                    {
                        "topic": "topic1",
                        "id": "0",
                        "leader": "2",
                        "replicas": ["0", "1", "2"],
                        "isr": ["2", "0", "1"],
                    }
                ],
            ],
            HealthResponse(
                healthy=False,
                reason=[HealthResponseReason.not_preferred_leaders([TOPIC1_0_PARTITION])],
            ),
            id="not_preferred_leader",
        ),
        pytest.param(
            ["topic1", "topic2"],
            [
                [
                    {
                        "topic": "topic1",
                        "id": "0",
                        "leader": "2",
                        "replicas": ["0", "1", "2"],
                        "isr": ["2", "0", "1"],
                    }
                ],
                [
                    {
                        "topic": "topic2",
                        "id": "0",
                        "leader": "0",
                        "replicas": ["0", "1", "2"],
                        "isr": ["2", "0"],
                    }
                ],
            ],
            HealthResponse(
                healthy=False,
                reason=[
                    HealthResponseReason.outside_isr([TOPIC2_0_PARTITION]),
                    HealthResponseReason.not_preferred_leaders([TOPIC1_0_PARTITION]),
                ],
            ),
            id="not_preferred_and_not_isr",
        ),
    ],
)
def test_healthcheck_cluster_topics(
    topics: list[str],
    partitions_side_effect: list[dict[str, Any]],
    expected: HealthResponse,
    mock_admin_client: MagicMock,
) -> None:
    with (
        patch(
            "sentry_kafka_management.actions.topics.healthcheck.list_topics",
            return_value=topics,
        ),
        patch(
            "sentry_kafka_management.actions.topics.healthcheck.describe_topic_partitions",
            side_effect=partitions_side_effect,
        ),
    ):
        res = healthcheck_cluster_topics(mock_admin_client)
    assert res == expected
