"""
Topic placement algorithm for Kafka clusters.

This module computes static partition assignments for topics across broker slices.
The assignment is deterministic and computed from the ground up every time, and does not depend
on the current cluster state.

A topic placement is a list of assignments given to each partition of a topic.

A "slice" is a group of brokers, one per availability zone. Assigning a partition to a slice
guarantees its replicas span all availability zones.

For example, a cluster with 9 brokers (3 per zone) has 3 slices:
    Slice 0: [0, 1, 2]
    Slice 1: [3, 4, 5]
    Slice 2: [6, 7, 8]

Each slice has 3 available assignments, this is hardcoded to SLICE_SIZE as we have 3 availability
zones per Kafka cluster. Each assignment in a slice has the partition leader in a different AZ.

The algorithm:
1. Build slices from broker FQDNs (which contain zone information).
   Slices are built deterministically by sorting zones and broker IDs.
2. For each topic, give an assignment to each partition round-robin:
   slice i, partition j -> slice (i // SLICE_SIZE) % num_slices, assignment p % SLICE_SIZE

For example, with num_slices = 2 and N partitions, where N is the total partitions on the
cluster (not the topic, as we want to balance partition leaders across the cluster), the
assignments would be:
    Partition 0: slice 0, assignment 0
    Partition 1: slice 0, assignment 1
    Partition 2: slice 0, assignment 2
    Partition 3: slice 1, assignment 0
    ...

When the cluster expands (new slices added), recomputing produces a new
assignment. Some partitions naturally move to the new slices.
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple

from sentry_kafka_management.actions.brokers.parse import get_broker_zone

SLICE_SIZE = 3

Slice = list[int]

Assignment = list[int]


class TopicPlacement(NamedTuple):
    """
    Represents a topic and its partitions, each partition is given an assignment.
    """

    topic: str
    partitions: list[Assignment]


def _get_assignment(brokers: Slice, offset: int) -> list[int]:
    """
    Get a valid assignment for a given slice and offset.

    For example, if the slice is [0, 1, 2] and the assignments that could be
    returned are [0, 1, 2], [1, 2, 0], or [2, 0, 1].
    """
    offset = offset % len(brokers)
    return brokers[offset:] + brokers[:offset]


def build_slices(broker_id_mapping: dict[str, int]) -> list[Slice]:
    """
    Given a mapping of broker endpoints to broker IDs, build a list of slices,
    where each slice is a list of broker IDs (one per zone).
    """
    slices: list[Slice] = []
    brokers_by_zone: dict[str, list[int]] = defaultdict(list)

    for broker_str, broker_id in broker_id_mapping.items():
        zone = get_broker_zone(broker_str)
        brokers_by_zone[zone].append(broker_id)

    # make result deterministic
    zones = sorted(brokers_by_zone.keys())
    for zone in zones:
        brokers_by_zone[zone].sort()

    # all zones should have the same number of brokers
    if not all(len(brokers_by_zone[z]) == len(brokers_by_zone[zones[0]]) for z in zones):
        raise ValueError("All zones must have the same number of brokers")
    else:
        for slice_index in range(len(brokers_by_zone[zones[0]])):
            slices.append([brokers_by_zone[z][slice_index] for z in zones])

    return slices


def compute_cluster_placement(
    broker_id_mapping: dict[str, int],
    topic_partitions: dict[str, int],
) -> list[TopicPlacement]:
    """
    Compute partition assignments for all topics in a cluster.

    Each partition is assigned to a slice round-robin, and the assignment
    is rotated within the slice to distribute leaders evenly.

    Partition 0 topic 0: slice 0, offset 0
    Partition 0 topic 1: slice 1, offset 0
    Partition 0 topic 2: slice 2, offset 0
    partition 0 topic 3: slice 0, offset 1
    """
    slices = build_slices(broker_id_mapping)

    num_slices = len(slices)
    cluster_assignments: list[TopicPlacement] = []
    count = 0

    for topic, partition_count in sorted(topic_partitions.items()):
        assignments: list[Assignment] = []
        for _ in range(partition_count):
            slice = (count // SLICE_SIZE) % num_slices
            offset = count % SLICE_SIZE
            assignments.append(_get_assignment(slices[slice], offset))
            count += 1
        cluster_assignments.append(TopicPlacement(topic, assignments))

    return cluster_assignments
