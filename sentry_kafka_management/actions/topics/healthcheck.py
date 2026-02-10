from dataclasses import asdict, dataclass
from typing import Iterable

from confluent_kafka.admin import (  # type: ignore[import-untyped]
    AdminClient,
)

from sentry_kafka_management.actions.topics.describe import (
    describe_topic_partitions,
    list_topics,
)


@dataclass(frozen=True)
class Partition:
    """
    Represents a single partition in a Kafka topic.
    """

    topic: str
    id: str
    leader: str
    replicas: list[str]
    isr: list[str]

    def __hash__(self) -> int:
        return hash((self.topic, self.id, self.leader, tuple(self.replicas), tuple(self.isr)))


class HealthResponseReason:
    """
    Generates entries for the `reason` field of a `HealthReponse`.
    """

    @staticmethod
    def healthy() -> str:
        return "Cluster is healthy."

    @staticmethod
    def outside_isr(partitions: Iterable[Partition]) -> str:
        return f"Partitions {[(p.topic, p.id) for p in partitions]} are missing ISR."

    @staticmethod
    def not_preferred_leaders(partitions: Iterable[Partition]) -> str:
        return (
            f"Partitions {[(p.topic, p.id) for p in partitions]} "
            "do not have their preferred replica as leader."
        )


@dataclass()
class HealthResponse:
    """
    Used by `PartitionHealthChecker` to represent if a cluster is healthy or not.

    Args:
        healthy: Whether the cluster is in a healthy state or not.
        reason: Description(s) of why the cluster health is True/False.
    """

    healthy: bool
    reason: list[str]

    def to_json(self) -> dict[str, str]:
        return asdict(self)


class PartitionHealthChecker:
    """
    Used in `healthcheck_cluster_topics` to track the health of a cluster's partitions.

    Args:
        leader_is_preferred: Tracks which partitions have their preferred leader as leader.
        partitions_outside_isr: Tracks any partitions which have ISR != partition replicas.
    """

    def __init__(self) -> None:
        self.leader_is_preferred: dict[Partition, bool] = {}
        self.partitions_outside_isr: set[Partition] = set()

    def check_partition(self, partition: Partition) -> None:
        """
        Checks the given partition's leadership and ISR and records if its unhealthy.
        """
        self.leader_is_preferred[partition] = partition.leader == partition.replicas[0]
        if set(partition.replicas) != set(partition.isr):
            self.partitions_outside_isr.add(partition)

    def is_healthy(self) -> HealthResponse:
        """
        Returns healthy response if there are no recorded partitions outside ISR
        and every partition's leader is the preferred leader.
        """
        healthy = True
        reason = []
        if self.partitions_outside_isr:
            healthy = False
            reason.append(HealthResponseReason.outside_isr(self.partitions_outside_isr))
        if not all(self.leader_is_preferred.values()):
            not_preferred = filter(
                lambda x: not self.leader_is_preferred[x], self.leader_is_preferred.keys()
            )
            healthy = False
            reason.append(HealthResponseReason.not_preferred_leaders(not_preferred))

        if healthy:
            reason = [HealthResponseReason.healthy()]
        return HealthResponse(healthy=healthy, reason=reason)


def healthcheck_cluster_topics(
    admin_client: AdminClient,
) -> HealthResponse:
    """
    Does a healthcheck against a Kafka cluster's topics by ensuring that:
    * all topics have the expected number of in-sync replicas
    * all brokers have a roughly equal amount of partition leaders across all topics
      in the cluster (+/- the number of brokers in the cluster)

    Args:
        admin_client: A Confluent API admin client.
    """
    health_checker = PartitionHealthChecker()
    for topic in list_topics(admin_client):
        partition_data = describe_topic_partitions(admin_client, topic)
        for p in partition_data:
            partition = Partition(
                p["topic"],
                p["id"],
                p["leader"],
                p["replicas"],
                p["isr"],
            )
            health_checker.check_partition(partition)
    return health_checker.is_healthy()
