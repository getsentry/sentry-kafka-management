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
   slice i, partition j -> slice: i % num_slices, assignment: (j // num_slices) % SLICE_SIZE

For example, with 3 slices and N partitions, where N is the total partitions on the
cluster (not the topic, as we want to balance partition leaders across the cluster),
the assignments would be:
    Partition 0: slice 0, assignment 0 -> [0, 1, 2]
    Partition 1: slice 1, assignment 0 -> [3, 4, 5]
    Partition 2: slice 2, assignment 0 -> [6, 7, 8]
    Partition 3: slice 0, assignment 1 -> [1, 2, 0]
    Partition 4: slice 1, assignment 1 -> [4, 5, 3]
    ...

Limitations:
1. Topic-order sensitivity: a per-topic shift is derived from sorted topic names.
   Adding/removing/renaming a topic can change assignments for later topics.
2. Topology sensitivity: when slices change (for example, brokers are added and
   new slices appear), recomputing produces a new assignment and some partitions
   naturally move to different slices.
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple

from sentry_kafka_management.actions.brokers.parser import get_broker_zone

SLICE_SIZE = 3

Slice = list[int]

Assignment = list[int]


class TopicPlacement(NamedTuple):
    """
    Represents a topic and its partitions, each partition is given an assignment.
    """

    topic: str
    partitions: list[Assignment]


def build_slices(broker_id_mapping: dict[str, int]) -> list[Slice]:
    """
    Given a mapping of broker endpoints to broker IDs, build a list of slices,
    where each slice is a list of broker IDs (one per zone).
    """
    slices: list[Slice] = []
    brokers_by_zone: dict[str, list[int]] = defaultdict(list)

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
    broker_id_mapping: dict[str, int],
    topic_partitions: dict[str, int],
) -> list[TopicPlacement]:
    """
    Compute partition assignments for all topics in a cluster.

    Each partition is assigned to a slice round-robin, and the assignment
    is rotated within the slice to distribute leaders evenly.

    Partition 0: slice 0, assignment 0 -> [0, 1, 2]
    Partition 1: slice 1, assignment 0 -> [3, 4, 5]
    Partition 2: slice 2, assignment 0 -> [6, 7, 8]
    Partition 3: slice 0, assignment 1 -> [1, 2, 0]
    Partition 4: slice 1, assignment 1 -> [4, 5, 3]
    ...

    Limitations:
    - The per-topic shift depends on sorted topic order. Adding/removing/renaming
      topics can shift assignments for lexicographically later topics.
    - Changing slice topology (for example, adding slices) can reassign partitions.
    """
    slices = build_slices(broker_id_mapping)

    num_slices = len(slices)
    cluster_assignments: list[TopicPlacement] = []
    shift = 0

    for topic, partition_count in sorted(topic_partitions.items()):
        assignments: list[Assignment] = [[] for _ in range(partition_count)]
        for i in range(partition_count):
            slice = (i + shift) % num_slices
            offset = ((i + shift) // num_slices) % SLICE_SIZE
            assignments[i] = slices[slice][offset:] + slices[slice][:offset]
        cluster_assignments.append(TopicPlacement(topic, assignments))
        shift += 1

    return cluster_assignments
