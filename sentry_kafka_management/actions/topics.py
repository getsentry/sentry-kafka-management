from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]


def list_topics(admin_client: AdminClient) -> list[str]:
    """
    List all topics in the given Kafka cluster.
    """
    # list_topics() returns TopicMetadata, we need to extract topic names
    topic_metadata = admin_client.list_topics()
    return list(topic_metadata.topics.keys())
