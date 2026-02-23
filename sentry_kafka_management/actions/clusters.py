from typing import Any, cast

from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from sentry_kafka_management.actions.conf import KAFKA_TIMEOUT


def describe_cluster(
    admin_client: AdminClient,
) -> list[dict[str, Any]]:
    """
    Returns configuration of a cluster.
    """

    res = admin_client.describe_cluster().result(KAFKA_TIMEOUT)
    controller = res.controller
    return [
        {
            "id": node.id_string,
            "host": node.host,
            "port": node.port,
            "rack": node.rack,
            "isController": node.id == controller.id,
        }
        for node in res.nodes
    ]


def get_cluster_controller(admin_client: AdminClient) -> str:
    cluster_data = describe_cluster(admin_client)
    for broker in cluster_data:
        if broker["isController"]:
            return cast(str, broker["id"])
    raise RuntimeError("No controller found for the cluster.")
