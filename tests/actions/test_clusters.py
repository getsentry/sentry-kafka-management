from unittest.mock import Mock

from confluent_kafka import Node
from confluent_kafka.admin import DescribeClusterResult

from sentry_kafka_management.actions.clusters import describe_cluster


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
