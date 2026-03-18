"""
Topic placement algorithm for Kafka clusters.

When we scale a Kafka cluster horizontally for instance, we will have to
move partitions from existing brokers to newly added brokers.

First, we introduce the concept of "slices" for this algorithm.

Partitions are assigned to "slices" of brokers, where each slice contains
one broker per availability zone. A partition being assigned to a slice
means that the partition will guaranteed to have its replicas across all
availability zones.

For example, if we have a cluster with 9 brokers, 3 per availability zone,
we will have 3 slices:
    Slice 0: [0, 1, 2]
    Slice 1: [3, 4, 5]
    Slice 2: [6, 7, 8]

Within each slice, leader assignment is rotated to distribute leadership
evenly across brokers:
    Slice [0, 1, 2] produces configs: [0,1,2], [1,2,0], [2,0,1]

Slices are built using broker FQDNs which contains zone information.

A "replica rotation config" is a list of broker IDs that represents the order
in which replicas for a partition are assigned to brokers. For example,
a replica rotation config [0, 1, 2] means that the replicas for a partition
will be assigned to the brokers with IDs 0, 1, 2.

The first broker ID in the rotation config is the broker where the leader of
the partition is assigned to. Thus, in order to have the leaders of all partitions
spread evenly across brokers, we need rotate the replica config before assigning.

The algorithm steps are:
1. Fetches broker IDs, topics and topic partitions from the cluster.
2. Builds the "slices" of broker IDs. Slice generation must be deterministic
   so that we can re-run the algorithm and get the same result.
3. Count the partitions per slice. If a partition has its replicas in a slice that
   matches one of our built slices, it is considered "placed". Otherwise, it is
   considered "unplaced".
4. Assign unplaced partitions to the least-common replica config.
5. Rebalance across slices. Compute the target count per slice as
   total_partitions // num_slices (with the remainder distributed to
   the first few slices). Then compute the delta for each slice
   (current - target). Slices with a positive delta are over-represented
   and donate partitions; slices with a negative delta are
   under-represented and receive them.

   For example, with 64 partitions across 3 slices, each slice targets
   22/21/21 partitions. If we add 2 slices (now 5 total), each slice
   targets 13/13/13/13/12 partitions. The 3 existing slices each need to
   give up 9/8/8 partitions to fill the 2 new slices. This makes horizontal
   scaling incremental, and we only move the minimum number of partitions
   needed to balance, rather than repartitioning everything.

Reference:
- https://www.notion.so/sentry/Ideal-Topic-Placement-Algorithm-3208b10e4b5d80889bd2f05195fea82d
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple


class TopicPartition(NamedTuple):
    topic: str
    partition_id: int


class ReplicaRotation(NamedTuple):
    slice_index: int
    rotation_index: int


Slice = list[int]  # list of broker IDs (one per zone)

ReplicaList = list[int]  # ordered broker IDs for a partition

TopicAssignments = dict[str, dict[int, ReplicaList]]  # topic_name -> {partition_id: replica_list}

SLICE_SIZE = 3


def build_slices(broker_id_mapping: dict[str, int]) -> list[Slice]:
    """
    Given a mapping of broker endpoints to broker IDs, build a list of slices,
    where each slice is a list of broker IDs (one per zone).
    """
    slices: list[Slice] = []
    by_zone: dict[str, list[int]] = defaultdict(list)

    for broker_str, broker_id in broker_id_mapping.items():
        try:
            zone = broker_str.split(":")[0].split(".")[1]
        except IndexError:
            raise ValueError(f"Invalid broker endpoint: {broker_str}")
        by_zone[zone].append(broker_id)

    zones = sorted(by_zone.keys())
    for zone in zones:
        by_zone[zone].sort()

    # all zones should have the same number of brokers
    if not all(len(by_zone[z]) == len(by_zone[zones[0]]) for z in zones):
        raise ValueError("All zones must have the same number of brokers")

    num_slices = len(by_zone[zones[0]])

    for slice_index in range(num_slices):
        slices.append([by_zone[z][slice_index] for z in zones])

    return slices


def _replica_configs(slice_brokers: Slice) -> list[ReplicaList]:
    """
    Given a slice, return a list of all replica configurations for the slice.
    """
    return [slice_brokers[i:] + slice_brokers[:i] for i in range(SLICE_SIZE)]


def _build_config_maps(
    slices: list[Slice],
) -> tuple[dict[ReplicaRotation, ReplicaList], dict[tuple[int, ...], ReplicaRotation]]:
    """
    Build lookup maps for replica configurations.
    """
    configs: dict[ReplicaRotation, ReplicaList] = {}
    config_lookup: dict[tuple[int, ...], ReplicaRotation] = {}
    for slice_index, slice_brokers in enumerate(slices):
        for config_index, config in enumerate(_replica_configs(slice_brokers)):
            config_id = ReplicaRotation(slice_index, config_index)
            configs[config_id] = config
            config_lookup[tuple(config)] = config_id
    return configs, config_lookup


def _classify_assignments(
    current_assignments: TopicAssignments,
    configs: dict[ReplicaRotation, ReplicaList],
    config_lookup: dict[tuple[int, ...], ReplicaRotation],
) -> tuple[dict[ReplicaRotation, int], dict[TopicPartition, ReplicaRotation], list[TopicPartition]]:
    """
    Split current assignments into placed and unplaced partitions.
    """
    config_counts: dict[ReplicaRotation, int] = {k: 0 for k in configs}
    placed: dict[TopicPartition, ReplicaRotation] = {}
    unplaced: list[TopicPartition] = []

    for topic, partitions in current_assignments.items():
        for partition, replicas in partitions.items():
            current_rotation = config_lookup.get(tuple(replicas))
            if current_rotation is not None:
                config_counts[current_rotation] += 1
                placed[TopicPartition(topic, partition)] = current_rotation
            else:
                unplaced.append(TopicPartition(topic, partition))

    return config_counts, placed, unplaced


def _rebalance_placed_partitions(
    slices: list[Slice],
    configs: dict[ReplicaRotation, ReplicaList],
    config_counts: dict[ReplicaRotation, int],
    placed_partitions: dict[TopicPartition, ReplicaRotation],
) -> tuple[dict[ReplicaRotation, int], dict[TopicPartition, ReplicaRotation]]:
    num_slices = len(slices)
    slice_counts = {slice_index: 0 for slice_index in range(num_slices)}
    for config_id, count in config_counts.items():
        slice_counts[config_id.slice_index] += count

    # calculate target counts for each slice
    base, remainder = divmod(len(placed_partitions), num_slices)
    target_counts = {
        slice_index: base + (1 if slice_index < remainder else 0)
        for slice_index in range(num_slices)
    }

    delta = {
        slice_index: slice_counts[slice_index] - target_counts[slice_index]
        for slice_index in range(num_slices)
    }

    if all(v == 0 for v in delta.values()):
        return config_counts, placed_partitions

    config_partitions: dict[ReplicaRotation, list[TopicPartition]] = {k: [] for k in configs}
    for topic_partition, config_key in placed_partitions.items():
        config_partitions[config_key].append(topic_partition)
    for key in config_partitions:
        config_partitions[key].sort()

    source_slices = [s for s in range(num_slices) if delta[s] > 0]
    target_slices = [s for s in range(num_slices) if delta[s] < 0]

    while any(delta[s] > 0 for s in source_slices):
        made_progress = False
        for source_slice in source_slices:
            if delta[source_slice] <= 0:
                continue
            target_slice = next((s for s in target_slices if delta[s] < 0), None)
            if target_slice is None:
                return config_counts, placed_partitions

            source_config = max(
                (k for k in configs if k.slice_index == source_slice),
                key=lambda k: config_counts[k],
            )

            topic_partition = config_partitions[source_config][0]

            target_config = min(
                (k for k in config_counts if k.slice_index == target_slice),
                key=lambda k: config_counts[k],
            )

            config_partitions[source_config].remove(topic_partition)
            config_partitions[target_config].append(topic_partition)
            placed_partitions[topic_partition] = target_config
            config_partitions[target_config].sort()
            config_counts[source_config] -= 1
            config_counts[target_config] += 1
            delta[source_slice] -= 1
            delta[target_slice] += 1
            made_progress = True
        if not made_progress:
            return config_counts, placed_partitions

    return config_counts, placed_partitions


def compute_cluster_placement(
    slices: list[Slice],
    current_assignments: TopicAssignments,
) -> TopicAssignments:
    """
    Compute partition placement for all topics in a cluster.
    """
    # build replica configurations for each slice
    configs, config_lookup = _build_config_maps(slices)

    # classify partitions into placed and unplaced partitions
    config_counts, placed_partitions, unplaced_partitions = _classify_assignments(
        current_assignments,
        configs,
        config_lookup,
    )

    # assign unplaced partitions to the least common config
    for topic_partition in unplaced_partitions:
        least_common = min(config_counts, key=lambda k: config_counts[k])
        config_counts[least_common] += 1
        placed_partitions[topic_partition] = least_common

    # now all partitions are placed, rebalance them across slices
    config_counts, placed_partitions = _rebalance_placed_partitions(
        slices,
        configs,
        config_counts,
        placed_partitions,
    )

    result: TopicAssignments = {}
    for topic_partition, config_key in placed_partitions.items():
        result.setdefault(topic_partition.topic, {})[topic_partition.partition_id] = configs[
            config_key
        ]

    return result
