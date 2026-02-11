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
TOPIC1_0_HEALTHY_PARTITION = Partition("topic1", "0", "0", ["0", "1", "2"], ["0", "1", "2"])
TOPIC1_1_HEALTHY_PARTITION = Partition("topic1", "1", "2", ["2", "3", "4"], ["2", "3", "4"])
TOPIC1_2_HEALTHY_PARTITION = Partition("topic1", "2", "6", ["6", "5", "4"], ["4", "5", "6"])
TOPIC2_0_HEALTHY_PARTITION = Partition("topic2", "0", "0", ["0", "1", "2"], ["0", "1", "2"])
TOPIC1_0_ISR_PARTITION = Partition("topic1", "0", "0", ["0", "1", "2"], ["0", "1"])
TOPIC2_0_LEADER_PARTITION = Partition("topic2", "0", "2", ["0", "1", "2"], ["0", "1", "2"])


@pytest.mark.parametrize(
    "topics, partitions_side_effect, expected",
    [
        pytest.param(
            ["topic1", "topic2"],
            [
                [TOPIC1_0_HEALTHY_PARTITION.to_json()],
                [TOPIC2_0_HEALTHY_PARTITION.to_json()],
            ],
            HealthResponse(healthy=True, reason=[HealthResponseReason.healthy()]),
            id="healthy_topics",
        ),
        pytest.param(
            ["topic1"],
            [
                [
                    TOPIC1_0_HEALTHY_PARTITION.to_json(),
                    TOPIC1_1_HEALTHY_PARTITION.to_json(),
                    TOPIC1_2_HEALTHY_PARTITION.to_json(),
                ],
            ],
            HealthResponse(healthy=True, reason=[HealthResponseReason.healthy()]),
            id="healthy_topic_multiple_partitions",
        ),
        pytest.param(
            ["topic1"],
            [
                [TOPIC1_0_ISR_PARTITION.to_json()],
            ],
            HealthResponse(
                healthy=False,
                reason=[HealthResponseReason.outside_isr([TOPIC1_0_ISR_PARTITION])],
            ),
            id="not_enough_isr",
        ),
        pytest.param(
            ["topic1"],
            [
                [TOPIC2_0_LEADER_PARTITION.to_json()],
            ],
            HealthResponse(
                healthy=False,
                reason=[HealthResponseReason.not_preferred_leaders([TOPIC2_0_LEADER_PARTITION])],
            ),
            id="not_preferred_leader",
        ),
        pytest.param(
            ["topic1", "topic2"],
            [
                [TOPIC1_0_ISR_PARTITION.to_json()],
                [TOPIC2_0_LEADER_PARTITION.to_json()],
            ],
            HealthResponse(
                healthy=False,
                reason=[
                    HealthResponseReason.outside_isr([TOPIC1_0_ISR_PARTITION]),
                    HealthResponseReason.not_preferred_leaders([TOPIC2_0_LEADER_PARTITION]),
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
