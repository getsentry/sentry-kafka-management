from unittest.mock import Mock

from sentry_kafka_management.actions.clusters import describe_cluster


def test_describe_cluster() -> None:
    """Test describing cluster configuration."""
    mock_controller = Mock()
    mock_controller.id = 1

    mock_node1 = Mock()
    mock_node1.id = 1
    mock_node1.id_string = "1"
    mock_node1.host = "broker1.example.com"
    mock_node1.port = 9092
    mock_node1.rack = "rack1"

    mock_node2 = Mock()
    mock_node2.id = 2
    mock_node2.id_string = "2"
    mock_node2.host = "broker2.example.com"
    mock_node2.port = 9092
    mock_node2.rack = "rack2"

    mock_node3 = Mock()
    mock_node3.id = 3
    mock_node3.id_string = "3"
    mock_node3.host = "broker3.example.com"
    mock_node3.port = 9092
    mock_node3.rack = None

    mock_cluster_result = Mock()
    mock_cluster_result.controller = mock_controller
    mock_cluster_result.nodes = [mock_node1, mock_node2, mock_node3]

    mock_cluster_result.result.return_value = mock_cluster_result

    mock_client = Mock()
    mock_client.describe_cluster.return_value = mock_cluster_result

    expected = [
        {
            "id": "1",
            "host": "broker1.example.com",
            "port": 9092,
            "rack": "rack1",
            "isController": True,
        },
        {
            "id": "2",
            "host": "broker2.example.com",
            "port": 9092,
            "rack": "rack2",
            "isController": False,
        },
        {
            "id": "3",
            "host": "broker3.example.com",
            "port": 9092,
            "rack": None,
            "isController": False,
        },
    ]

    result = describe_cluster(mock_client)
    mock_client.describe_cluster.assert_called_once()
    assert result == expected
