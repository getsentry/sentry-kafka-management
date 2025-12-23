from pathlib import Path

from sentry_kafka_management.brokers import ClusterConfig, YamlKafkaConfig


def get_cluster_config(config: Path, cluster: str) -> ClusterConfig:
    yaml_config = YamlKafkaConfig(config)
    return yaml_config.get_clusters()[cluster]
