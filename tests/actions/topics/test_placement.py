from collections import Counter, defaultdict

from sentry_kafka_management.actions.topics.placement import (
    build_slices,
    compute_cluster_placement,
)

# 3 slices: A=[0,1,2], B=[3,4,5], C=[6,7,8]
SLICES_3 = [[0, 1, 2], [3, 4, 5], [6, 7, 8]]


class TestBuildSlices:
    def test_build_slices_parses_zone_from_broker_endpoint(self) -> None:
        broker_id_mapping = {
            "kafka-0.zone-a.c.project.internal:9092": 0,
            "kafka-1.zone-c.c.project.internal:9092": 1,
            "kafka-2.zone-a.c.project.internal:9092": 2,
            "kafka-3.zone-b.c.project.internal:9092": 3,
            "kafka-4.zone-b.c.project.internal:9092": 4,
            "kafka-5.zone-c.c.project.internal:9092": 5,
        }

        assert build_slices(broker_id_mapping) == [[0, 3, 1], [2, 4, 5]]


class TestComputeClusterPlacement:
    def test_all_partitions_get_full_replica_list(self) -> None:
        """Every partition should have a 3-broker replica list."""
        current = {"topic": {i: [SLICES_3[i % 3][0]] for i in range(12)}}
        result = compute_cluster_placement(SLICES_3, current)
        for _, replicas in result["topic"].items():
            assert len(replicas) == 3

    def test_replicas_within_slices(self) -> None:
        """All replicas for a partition must come from the same slice."""
        current = {"topic": {i: [SLICES_3[i % 3][0]] for i in range(64)}}
        result = compute_cluster_placement(SLICES_3, current)
        slice_sets = [set(slice_brokers) for slice_brokers in SLICES_3]
        for _, replicas in result["topic"].items():
            assert any(set(replicas).issubset(slice_set) for slice_set in slice_sets)

    def test_unplaced_partitions_go_to_least_common_config(self) -> None:
        """Unplaced partitions get assigned to least common config."""
        # 8 partitions, 1 slice [0,1,2]
        # P0=[0,1,2](A1), P1=[1,2,0](A2), P2=[2,0,1](A3), P3=[0,4,8](unplaced)
        # P4=[1,2,0](A2), P5=[0,1,2](A1), P6=[2,0,1](A3), P7=[0,1,2](A1)
        # Counts: A1=3, A2=2, A3=2, so P3 should get A2 or A3 (least common, first wins = A2)
        slices = [[0, 1, 2]]
        current = {
            "topic": {
                0: [0, 1, 2],
                1: [1, 2, 0],
                2: [2, 0, 1],
                3: [0, 4, 8],
                4: [1, 2, 0],
                5: [0, 1, 2],
                6: [2, 0, 1],
                7: [0, 1, 2],
            }
        }
        result = compute_cluster_placement(slices, current)
        assert result["topic"][3] in ([1, 2, 0], [2, 0, 1])

    def test_expansion_rebalance(self) -> None:
        """Adding a slice moves partitions from overloaded to underloaded."""
        # 2 slices A=[0,1,2], B=[3,4,5]
        # 2 topics, 4 partitions each
        current = {
            "topic-1": {0: [0, 1, 2], 1: [3, 4, 5], 2: [1, 2, 0], 3: [4, 5, 3]},
            "topic-2": {0: [2, 0, 1], 1: [5, 3, 4], 2: [0, 1, 2], 3: [3, 4, 5]},
        }
        # Add slice C=[6,7,8] -> becomes SLICES_3
        result = compute_cluster_placement(SLICES_3, current)

        # Count partitions per slice
        slice_sets = [set(s) for s in SLICES_3]
        slice_counts: dict[int, int] = defaultdict(int)
        for topic in result:
            for replicas in result[topic].values():
                for slice_index, slice_set in enumerate(slice_sets):
                    if replicas[0] in slice_set:
                        slice_counts[slice_index] += 1
                        break
        # A=3, B=3, C=2
        assert sorted(slice_counts.values()) == [2, 3, 3]

    def test_no_change_when_balanced(self) -> None:
        """If slices are already balanced, no partitions should move slices."""
        current = {
            "t": {
                0: [0, 1, 2],
                1: [1, 2, 0],
                2: [2, 0, 1],
                3: [3, 4, 5],
                4: [4, 5, 3],
                5: [5, 3, 4],
                6: [6, 7, 8],
                7: [7, 8, 6],
                8: [8, 6, 7],
            }
        }
        result = compute_cluster_placement(SLICES_3, current)
        # Every partition should keep its current config
        for partition in current["t"]:
            assert result["t"][partition] == current["t"][partition]

    def test_even_leader_distribution(self) -> None:
        """Leaders should be evenly distributed across brokers."""
        current = {"topic": {i: [SLICES_3[i % 3][0]] for i in range(63)}}
        result = compute_cluster_placement(SLICES_3, current)
        leader_counts: Counter[int] = Counter()
        for reps in result["topic"].values():
            leader_counts[reps[0]] += 1
        assert all(c == 7 for c in leader_counts.values()), f"Uneven leaders: {dict(leader_counts)}"
