from pathlib import Path

import pytest
import yaml

from sentry_kafka_management.actions.topics.placement import TopicPlacement
from sentry_kafka_management.scripts.topics.placement import (
    count_leader_distribution,
    get_default_partitions,
    get_topic_partitions,
    parse_topic_partitions,
)


@pytest.fixture
def shared_config(tmp_path: Path) -> Path:
    """Create a minimal shared config directory structure."""
    defaults_directory = tmp_path / "topics" / "defaults"
    defaults_directory.mkdir(parents=True)
    (defaults_directory / "defaults.yaml").write_text(
        yaml.dump({"partitions": 1, "replicationFactor": 3})
    )

    topics_directory = tmp_path / "topics"
    (topics_directory / "topic-a.yaml").write_text(yaml.dump({"partitions": 32}))
    (topics_directory / "topic-b.yaml").write_text(yaml.dump({"partitions": 16}))

    overrides_directory = tmp_path / "topics" / "regional_overrides" / "region-1"
    overrides_directory.mkdir(parents=True)

    (overrides_directory / "topic-a.yaml").write_text(
        yaml.dump({"cluster": "cluster-1", "partitions": 64})
    )
    (overrides_directory / "topic-b.yaml").write_text(yaml.dump({"cluster": "cluster-1"}))
    (overrides_directory / "topic-c.yaml").write_text(yaml.dump({"cluster": "cluster-1"}))
    (overrides_directory / "topic-d.yaml").write_text(
        yaml.dump({"cluster": "cluster-2", "partitions": 8})
    )

    return tmp_path


def test_get_default_partitions(shared_config: Path) -> None:
    assert get_default_partitions(shared_config) == 1


def test_get_topic_partitions_exists(shared_config: Path) -> None:
    assert get_topic_partitions(shared_config, "topic-a") == 32


def test_get_topic_partitions_missing(shared_config: Path) -> None:
    assert get_topic_partitions(shared_config, "some-topic") is None


def test_parse_topic_partitions_filters_by_cluster(shared_config: Path) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-1")
    assert "topic-d" not in result


def test_parse_topic_partitions_uses_override_partitions(shared_config: Path) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-1")
    assert result["topic-a"] == 64


def test_parse_topic_partitions_falls_back_to_top_level(shared_config: Path) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-1")
    assert result["topic-b"] == 16


def test_parse_topic_partitions_falls_back_to_defaults(shared_config: Path) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-1")
    assert result["topic-c"] == 1


def test_parse_topic_partitions_returns_all_matching_topics(
    shared_config: Path,
) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-1")
    assert sorted(result.keys()) == ["topic-a", "topic-b", "topic-c"]


def test_parse_topic_partitions_different_cluster(shared_config: Path) -> None:
    result = parse_topic_partitions(shared_config, "region-1", "cluster-2")
    assert result == {"topic-d": 8}


def test_parse_topic_partitions_empty_region(tmp_path: Path) -> None:
    defaults_directory = tmp_path / "topics" / "defaults"
    defaults_directory.mkdir(parents=True)
    (defaults_directory / "defaults.yaml").write_text(yaml.dump({"partitions": 1}))

    overrides_directory = tmp_path / "topics" / "regional_overrides" / "empty"
    overrides_directory.mkdir(parents=True)

    result = parse_topic_partitions(tmp_path, "empty", "cluster-1")
    assert result == {}


def test_count_leader_distribution(capsys: pytest.CaptureFixture[str]) -> None:
    result = [
        TopicPlacement("topic-a", [[0, 1, 2], [3, 4, 5], [0, 1, 2]]),
        TopicPlacement("topic-b", [[3, 4, 5], [0, 1, 2]]),
    ]
    count_leader_distribution(result)
    output = capsys.readouterr().out
    assert "Leader distribution: 3/2" in output
    assert "Replica distribution: 3/3/3/2/2/2" in output
