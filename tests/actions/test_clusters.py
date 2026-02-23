from unittest.mock import MagicMock, Mock, patch

from confluent_kafka import Node  # type: ignore[import-untyped]
from confluent_kafka.admin import DescribeClusterResult  # type: ignore[import-untyped]

from sentry_kafka_management.actions.clusters import (
    describe_cluster,
    get_cluster_controller,
)


def test_describe_cluster() -> None:
    """Test describing cluster configuration."""
    controller = Node(id=1, host=Mock(), port=Mock(), rack=Mock())

    node1 = Node(id=1, host=Mock(), port=Mock(), rack=Mock())
    node2 = Node(id=2, host=Mock(), port=Mock(), rack=Mock())

    cluster_result = DescribeClusterResult(
        controller=controller, nodes=[node1, node2], cluster_id=Mock()
    )

    mock_future = Mock()
    mock_future.result.return_value = cluster_result

    mock_client = Mock()
    mock_client.describe_cluster.return_value = mock_future

    expected = [
        {
            "id": node1.id_string,
            "host": node1.host,
            "port": node1.port,
            "rack": node1.rack,
            "isController": node1.id == controller.id,
        },
        {
            "id": node2.id_string,
            "host": node2.host,
            "port": node2.port,
            "rack": node2.rack,
            "isController": node2.id == controller.id,
        },
    ]

    result = describe_cluster(mock_client)
    mock_client.describe_cluster.assert_called_once()
    assert result == expected


@patch(
    "sentry_kafka_management.actions.clusters.describe_cluster",
    return_value=[
        {
            "id": "0",
            "host": "test-host-1",
            "port": "9092",
            "rack": "a",
            "isController": True,
        },
        {
            "id": "1",
            "host": "test-host-2",
            "port": "9092",
            "rack": "b",
            "isController": False,
        },
    ],
)
def test_get_cluster_controller(mock_describe: MagicMock) -> None:
    res = get_cluster_controller(Mock())
    assert res == "0"
