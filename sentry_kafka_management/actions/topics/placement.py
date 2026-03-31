"""
Topic placement algorithms for Kafka clusters.

A topic placement is a list of assignments given to each partition of a topic.

A "slice" is a group of brokers, one per availability zone. The current slice size is hardcoded
in SLICE_SIZE as 3, this is because each Kafka cluster spans across 3 availability zones.

For example, a cluster with 9 brokers (3 per zone) has 3 slices:
    Slice 0: [0, 1, 2]
    Slice 1: [3, 4, 5]
    Slice 2: [6, 7, 8]

Since each slice has 3 brokers, 1 per availability zone, a slice can have 3 different assignments,
each assignment places the partition leader in one of the three brokers and the replicas in
the other two brokers. This way, assigning a partition to a slice guarantees its leader and replicas
span all availability zones.

For example, if our slice is [0, 1, 2], the possible assignments are:
    Assignment 0: [0, 1, 2]
    Assignment 1: [1, 2, 0]
    Assignment 2: [2, 0, 1]
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple

from sentry_kafka_management.actions.brokers.parser import BrokerId, get_broker_zone

SLICE_SIZE = 3

Slice = list[BrokerId]

Assignment = list[BrokerId]


class TopicPlacement(NamedTuple):
    """
    Represents a topic and its partitions, each partition is given an assignment.
    """

    topic: str
    partitions: list[Assignment]


def build_slices(broker_id_mapping: dict[str, BrokerId]) -> list[Slice]:
    """
    Given a mapping of broker FQDNs to broker IDs, build a list of slices,
    where each slice is a list of broker IDs (one per zone).
    """
    slices: list[Slice] = []
    brokers_by_zone: dict[str, list[BrokerId]] = defaultdict(list)

    for broker_hostname, broker_id in broker_id_mapping.items():
        zone = get_broker_zone(broker_hostname)
        brokers_by_zone[zone].append(broker_id)

    # make result deterministic
    zones = sorted(brokers_by_zone.keys())
    for zone in zones:
        brokers_by_zone[zone].sort()

    # all zones should have the same number of brokers
    for zone in zones:
        if len(brokers_by_zone[zone]) != len(brokers_by_zone[zones[0]]):
            raise ValueError("All zones must have the same number of brokers")

    num_slices = len(brokers_by_zone[zones[0]])

    for slice in range(num_slices):
        slices.append([brokers_by_zone[zone][slice] for zone in zones])
    return slices


def compute_cluster_placement(
    broker_id_mapping: dict[str, BrokerId],
    topic_partitions: dict[str, int],
) -> list[TopicPlacement]:
    """
    Compute partition assignments for all topics in a cluster.

    The assignment is deterministic and computed from the ground up every time, it does not
    depend on the current cluster state.

    Args:
        broker_id_mapping: A mapping of broker FQDNs to broker IDs. The FQDNs contain zone
            information used to build slices. Broker IDs are used to build the assignments.
        topic_partitions: A mapping of topic names to their partition counts.

    Algorithm:
        1. Build slices from broker FQDNs (which contain zone information).
           Slices are built deterministically by sorting zones and broker IDs.
        2. For each topic, give an assignment to each partition round-robin, with a per-topic shift:
           topic i, partition j
           -> slice (i + j) % num_slices,
              assignment: ((i + j) // num_slices) % SLICE_SIZE

    For example, with 3 slices the assignments would be:
        Topic 0, Partition 0: slice 0, assignment 0 -> [0, 1, 2]
        Topic 0, Partition 1: slice 1, assignment 0 -> [3, 4, 5]
        Topic 0, Partition 2: slice 2, assignment 0 -> [6, 7, 8]
        Topic 0, Partition 3: slice 0, assignment 1 -> [1, 2, 0]
        Topic 0, Partition 4: slice 1, assignment 1 -> [4, 5, 3]
        ...
        Topic 1, Partition 0: slice 1, assignment 0 -> [3, 4, 5]
        Topic 1, Partition 1: slice 2, assignment 0 -> [6, 7, 8]
        Topic 1, Partition 2: slice 0, assignment 1 -> [1, 2, 0]
        Topic 1, Partition 3: slice 1, assignment 1 -> [4, 5, 3]
        Topic 1, Partition 4: slice 2, assignment 1 -> [7, 8, 6]
        ...

    Limitations:
        1. Topic-order sensitivity: a per-topic shift is derived from sorted topic names.
           Adding/removing/renaming a topic can change assignments for later topics.
        2. Topology sensitivity: when adding a new slice, recomputing produces a new assignment
           and partitions will move to different slices.
    """
    slices = build_slices(broker_id_mapping)
    topic_order = sorted(topic_partitions.keys())

    num_slices = len(slices)
    cluster_assignments: list[TopicPlacement] = []

    for topic_idx, topic_name in enumerate(topic_order):
        partition_count = topic_partitions[topic_name]
        assignments: list[Assignment] = []
        for partition_idx in range(partition_count):
            slice = (topic_idx + partition_idx) % num_slices
            rotation = ((topic_idx + partition_idx) // num_slices) % SLICE_SIZE
            assignments.append(slices[slice][rotation:] + slices[slice][:rotation])
        cluster_assignments.append(TopicPlacement(topic_name, assignments))

    return cluster_assignments
