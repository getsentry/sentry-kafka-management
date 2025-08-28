from sentry_kafka_management.brokers import ClusterConfig
from sentry_kafka_management.connectors.admin import get_admin_client


def list_topics(kafka_config: ClusterConfig) -> list[str]:
    """
    List all topics in the given Kafka cluster.
    """
    admin_client = get_admin_client(kafka_config)
    # list_topics() returns TopicMetadata, we need to extract topic names
    topic_metadata = admin_client.list_topics()
    return list(topic_metadata.topics.keys())
