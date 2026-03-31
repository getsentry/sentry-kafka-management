from collections import Counter

import pytest

from sentry_kafka_management.actions.brokers.parser import BrokerId
from sentry_kafka_management.actions.topics.placement import (
    SLICE_SIZE,
    build_slices,
    compute_cluster_placement,
)


def _make_broker_id_mapping(
    num_slices: int, zones: tuple[str, ...] = ("zone-a", "zone-b", "zone-c")
) -> dict[str, BrokerId]:
    """Build a broker_id_mapping with `num_slices` brokers per zone."""
    mapping: dict[str, BrokerId] = {}
    broker_id = 0
    for _ in range(num_slices):
        for zone in zones:
            fqdn = f"kafka-{broker_id}.{zone}.c.project.internal:9092"
            mapping[fqdn] = broker_id
            broker_id += 1
    return mapping


PLACEMENT_SCENARIOS = [
    pytest.param(
        1,
        {
            "topic-a": 64,
            "topic-b": 1,
            "topic-c": 32,
            "topic-d": 1,
            "topic-e": 64,
            "topic-f": 16,
        },
        id="1-slice-6-topics",
    ),
    pytest.param(
        2,
        {
            "topic-a": 16,
            "topic-b": 1,
            "topic-c": 32,
            "topic-d": 1,
            "topic-e": 64,
            "topic-f": 16,
        },
        id="2-slices-6-topics",
    ),
    pytest.param(
        3,
        {
            "topic-a": 64,
            "topic-b": 128,
            "topic-c": 32,
            "topic-d": 1,
            "topic-e": 64,
            "topic-f": 16,
        },
        id="3-slices-6-topics",
    ),
    pytest.param(
        4,
        {
            "topic-a": 32,
            "topic-b": 1,
            "topic-c": 64,
            "topic-d": 16,
            "topic-e": 64,
            "topic-f": 1,
        },
        id="4-slices-6-topics",
    ),
]


def test_parses_zone_from_broker_endpoint() -> None:
    broker_id_mapping = {
        "kafka-0.zone-a.c.project.internal:9092": 0,
        "kafka-1.zone-c.c.project.internal:9092": 1,
        "kafka-2.zone-a.c.project.internal:9092": 2,
        "kafka-3.zone-b.c.project.internal:9092": 3,
        "kafka-4.zone-b.c.project.internal:9092": 4,
        "kafka-5.zone-c.c.project.internal:9092": 5,
    }
    assert build_slices(broker_id_mapping) == [[0, 3, 1], [2, 4, 5]]


def test_invalid_broker_endpoint() -> None:
    with pytest.raises(ValueError):
        build_slices({"no-zone-here": 0, "kafka-1.zone-a.c.project.internal:9092": 1})


def test_uneven_zones() -> None:
    broker_id_mapping = {
        "kafka-0.zone-a.c.project.internal:9092": 0,
        "kafka-1.zone-a.c.project.internal:9092": 1,
        "kafka-2.zone-b.c.project.internal:9092": 2,
    }
    with pytest.raises(ValueError):
        build_slices(broker_id_mapping)


def test_slice_count_matches_brokers_per_zone() -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices=3)
    slices = build_slices(broker_id_mapping)
    assert len(slices) == 3
    for broker_slice in slices:
        assert len(broker_slice) == SLICE_SIZE


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_every_partition_has_full_replica_list(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    result = compute_cluster_placement(broker_id_mapping, topic_partitions)

    assert len(result) == len(topic_partitions)
    for placement, (expected_topic, expected_count) in zip(
        result, sorted(topic_partitions.items())
    ):
        assert placement.topic == expected_topic
        assert len(placement.partitions) == expected_count
        for assignment in placement.partitions:
            assert len(assignment) == SLICE_SIZE


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_replicas_always_within_same_slice(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    slices = build_slices(broker_id_mapping)
    slice_sets = [set(broker_slice) for broker_slice in slices]

    result = compute_cluster_placement(broker_id_mapping, topic_partitions)

    for placement in result:
        for assignment in placement.partitions:
            assert any(
                set(assignment).issubset(slice_set) for slice_set in slice_sets
            ), f"Replicas {assignment} not contained in any slice"


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_even_leader_distribution(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    result = compute_cluster_placement(broker_id_mapping, topic_partitions)

    leader_counts: Counter[int] = Counter()
    for placement in result:
        for assignment in placement.partitions:
            leader_counts[assignment[0]] += 1

    assert max(leader_counts.values()) - min(leader_counts.values()) <= 5


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_even_slice_distribution(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    """Partitions should be spread evenly across slices."""
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    slices = build_slices(broker_id_mapping)
    slice_sets = [set(broker_slice) for broker_slice in slices]

    result = compute_cluster_placement(broker_id_mapping, topic_partitions)

    slice_counts: Counter[int] = Counter()
    for placement in result:
        for assignment in placement.partitions:
            for slice_index, slice_set in enumerate(slice_sets):
                if assignment[0] in slice_set:
                    slice_counts[slice_index] += 1
                    break

    if slice_counts:
        assert max(slice_counts.values()) - min(slice_counts.values()) <= 3


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_no_duplicate_brokers_in_assignment(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    """Each partition's replica list should have no duplicate broker IDs."""
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    result = compute_cluster_placement(broker_id_mapping, topic_partitions)

    for placement in result:
        for assignment in placement.partitions:
            assert len(assignment) == len(set(assignment))


@pytest.mark.parametrize("num_slices, topic_partitions", PLACEMENT_SCENARIOS)
def test_deterministic(
    num_slices: int,
    topic_partitions: dict[str, int],
) -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices)
    result_1 = compute_cluster_placement(broker_id_mapping, topic_partitions)
    result_2 = compute_cluster_placement(broker_id_mapping, topic_partitions)
    assert result_1 == result_2


def test_empty_topics() -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices=3)
    result = compute_cluster_placement(broker_id_mapping, {})
    assert result == []


def test_round_robin_across_slices() -> None:
    broker_id_mapping = _make_broker_id_mapping(num_slices=3)
    slices = build_slices(broker_id_mapping)
    slice_sets = [set(broker_slice) for broker_slice in slices]
    expected_slices = [0, 1, 2, 0, 1, 2, 0, 1, 2]

    result = compute_cluster_placement(broker_id_mapping, {"topic-a": 4, "topic-b": 5})

    for partition_index, assignment in enumerate(result[0].partitions):
        expected_slice = expected_slices[partition_index]
        assert set(assignment).issubset(slice_sets[expected_slice])
