import os

from confluent_kafka.admin import AdminClient  # type: ignore[import-untyped]

from sentry_kafka_management.brokers import ClusterConfig


def get_admin_client(kafka_config: ClusterConfig) -> AdminClient:
    """
    Returns an admin client for the given Kafka cluster.
    """

    broker_config = {
        "bootstrap.servers": ",".join(kafka_config["brokers"]),
    }

    if kafka_config["security_protocol"]:
        broker_config["security.protocol"] = kafka_config["security_protocol"]

    if kafka_config["sasl_mechanism"]:
        broker_config["sasl.mechanism"] = kafka_config["sasl_mechanism"]

    if kafka_config["sasl_username"]:
        broker_config["sasl.username"] = kafka_config["sasl_username"]

    if kafka_config["sasl_password"]:
        broker_config["sasl.password"] = os.path.expandvars(kafka_config["sasl_password"])

    return AdminClient(broker_config)
