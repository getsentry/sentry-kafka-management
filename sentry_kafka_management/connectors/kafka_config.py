import os

from sentry_kafka_management.brokers import ClusterConfig


def build_broker_config(kafka_config: ClusterConfig) -> dict[str, str]:
    """
    Builds a confluent-kafka client configuration dict from a ClusterConfig.
    """

    broker_config: dict[str, str] = {
        "bootstrap.servers": ",".join(kafka_config["brokers"]),
    }

    if kafka_config["security_protocol"]:
        broker_config["security.protocol"] = kafka_config["security_protocol"]

    if kafka_config["sasl_mechanism"]:
        broker_config["sasl.mechanism"] = kafka_config["sasl_mechanism"]

    if kafka_config["sasl_username"]:
        broker_config["sasl.username"] = kafka_config["sasl_username"]

    if kafka_config["sasl_password"]:
        if kafka_config["password_is_plaintext"]:
            broker_config["sasl.password"] = kafka_config["sasl_password"]
        else:
            broker_config["sasl.password"] = os.path.expandvars(kafka_config["sasl_password"])

    return broker_config
