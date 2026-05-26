from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from sentry_kafka_management.brokers import ClusterConfig
from sentry_kafka_management.connectors.kafka_config import build_broker_config


def get_admin_client(kafka_config: ClusterConfig) -> AdminClient:
    """
    Returns an admin client for the given Kafka cluster.
    """

    return AdminClient(build_broker_config(kafka_config))
