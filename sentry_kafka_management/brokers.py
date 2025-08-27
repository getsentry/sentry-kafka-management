from abc import ABC
from pathlib import Path
from typing import Any, Mapping, Sequence, TypedDict

import yaml


class ClusterConfig(TypedDict):
    """
    Represents the configuration of a Kafka cluster.
    """

    brokers: Sequence[str]
    security_protocol: str | None
    sasl_mechanism: str | None
    sasl_username: str | None
    sasl_password: str | None


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

    def __init__(self, clusters_path: Path, topics_path: Path):
        clusters = yaml.safe_load(clusters_path.read_text())
        self.__clusters = {
            key: ClusterConfig(
                brokers=cluster["brokers"],
                security_protocol=cluster.get("security_protocol"),
                sasl_mechanism=cluster.get("sasl_mechanism"),
                sasl_username=cluster.get("sasl_username"),
                sasl_password=cluster.get("sasl_password"),
            )
            for key, cluster in clusters.items()
        }

        topics = yaml.safe_load(topics_path.read_text())
        self.__topics = {
            cluster_name: {
                key: TopicConfig(
                    partitions=topic["partitions"],
                    placement=topic["placement"],
                    replication_factor=topic["replication_factor"],
                    settings=topic["settings"],
                )
                for key, topic in topics.items()
            }
            for cluster_name, topics in topics.items()
        }

    def get_clusters(self) -> Mapping[str, ClusterConfig]:
        return self.__clusters

    def get_topics_config(
        self,
        cluster_name: str,
    ) -> Mapping[str, TopicConfig]:
        return self.__topics[cluster_name]
