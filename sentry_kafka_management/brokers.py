from abc import ABC
from pathlib import Path
from typing import Any, Mapping, Sequence, TypedDict

import yaml


class ClusterConfig(TypedDict):
    """
    Represents the configuration of a Kafka cluster.

    Fields:
        brokers: Broker connection strings
        security_protocol: Security protocol to use when connecting to the cluster
        sasl_mechanism: If using SASL security, which SASL mechanism to use
        sasl_username: If using SASL security, username of the kafka user
        sasl_password: If using SASL security, either the name of an env var containing
                       the password (default)or the password itself in plaintext
                       (if `password_is_plaintext=True`).
        password_is_plaintext: If True, `sasl_password` is interpreted as being the
                               plaintext password.
                               If False, `sasl_password` is interpreted as an environment var
                               containing the password. Defaults to False.
    """

    brokers: Sequence[str]
    security_protocol: str | None
    sasl_mechanism: str | None
    sasl_username: str | None
    sasl_password: str | None
    password_is_plaintext: bool


class TopicConfig(TypedDict):
    """
    Represents the configuration of a Kafka topic.
    """

    partitions: int
    placement: Any  # TODO: Add a structure for placement
    replication_factor: int
    settings: Mapping[str, Any]


class KafkaConfig(ABC):
    """
    Provides an entry point to the Kafka fleet configuration.

    There can be multiple implementations for different ways
    to store the config.

    Hopefully one day we will be able to consolidate on one
    """

    def get_clusters(self) -> Mapping[str, ClusterConfig]:
        """
        Returns the clsuters configuration. Specifically this
        is needed to connect to clusters.
        """
        raise NotImplementedError

    def get_topics_config(
        self,
        cluster_name: str,
    ) -> Mapping[str, TopicConfig]:
        """
        Returns the topics configuration for a cluster.
        This is not the actual production configuration. This is
        the configuration as per config files.
        """
        raise NotImplementedError


class YamlKafkaConfig(KafkaConfig):
    """
    Loads the Kafka config from a YAML file.
    """

    def __init__(self, conf_path: Path):
        conf: Sequence[Mapping[str, Any]] = yaml.safe_load(conf_path.read_text())
        self.__clusters = {
            cluster["name"]: ClusterConfig(
                brokers=cluster["brokers"],
                security_protocol=cluster.get("security_protocol"),
                sasl_mechanism=cluster.get("sasl_mechanism"),
                sasl_username=cluster.get("sasl_username"),
                sasl_password=cluster.get("sasl_password"),
                password_is_plaintext=cluster.get("password_is_plaintext", False),
            )
            for cluster in conf
        }

        self.__topics = {
            cluster["name"]: {
                topic["name"]: TopicConfig(
                    partitions=topic["partitions"],
                    placement=topic["placement"],
                    replication_factor=topic["replication_factor"],
                    settings=topic["settings"],
                )
                for topic in cluster["topics"]
            }
            # only generate this block if the cluster has a `topics` section
            for cluster in conf
            if "topics" in cluster
        }

    def get_clusters(self) -> Mapping[str, ClusterConfig]:
        return self.__clusters

    def get_topics_config(
        self,
        cluster_name: str,
    ) -> Mapping[str, TopicConfig]:
        return self.__topics[cluster_name]
