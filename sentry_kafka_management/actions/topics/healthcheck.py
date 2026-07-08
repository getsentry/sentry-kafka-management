from dataclasses import asdict, dataclass
from typing import Any, Iterable

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

    def to_json(self) -> dict[str, str]:
        return asdict(self)


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
    not_preferred_leaders: set[Partition]
    partitions_outside_isr: set[Partition]

    def to_json(self) -> dict[str, Any]:
        """
        Converts `healthy` & `reason` to a dict.
        Skips the other fields for simplicity of output.
        """
        return {"healthy": self.healthy, "reason": self.reason}


class PartitionHealthChecker:
    """
    Used in `healthcheck_cluster_topics` to track the health of a cluster's partitions.
    """

    def __init__(self) -> None:
        # Tracks which partitions have a leader other than their preferred replica.
        self.not_preferred_leaders: set[Partition] = set()
        # Tracks which partitions have ISR != partition replicas.
        self.partitions_outside_isr: set[Partition] = set()

    def check_partition(self, partition: Partition) -> None:
        """
        Checks the given partition's leadership and ISR and records if its unhealthy.
        """
        if partition.leader != partition.replicas[0]:
            self.not_preferred_leaders.add(partition)
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
        if self.not_preferred_leaders:
            healthy = False
            reason.append(HealthResponseReason.not_preferred_leaders(self.not_preferred_leaders))

        if healthy:
            reason = [HealthResponseReason.healthy()]
        return HealthResponse(
            healthy=healthy,
            reason=reason,
            not_preferred_leaders=self.not_preferred_leaders,
            partitions_outside_isr=self.partitions_outside_isr,
        )


def healthcheck_cluster_topics(
    admin_client: AdminClient,
) -> HealthResponse:
    """
    Does a healthcheck against a Kafka cluster's topics by ensuring that:
    * all topics have the expected number of in-sync replicas
    * all topics in the cluster have their preferred replica as partition leader

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
