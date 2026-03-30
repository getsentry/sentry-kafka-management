import pytest

from sentry_kafka_management.actions.brokers.parser import (
    get_broker_id,
    get_broker_zone,
)


@pytest.mark.parametrize(
    "fqdn, expected_zone",
    [
        ("kafka-0.zone-a.c.project.internal:9092", "zone-a"),
        ("kafka-1.zone-b.c.project.internal:9092", "zone-b"),
        ("kafka-99.us-central1-c.c.project.internal:9092", "us-central1-c"),
    ],
)
def test_get_broker_zone(fqdn: str, expected_zone: str) -> None:
    assert get_broker_zone(fqdn) == expected_zone


@pytest.mark.parametrize(
    "fqdn",
    [
        "no-dots-here",
        "",
    ],
)
def test_get_broker_zone_invalid(fqdn: str) -> None:
    with pytest.raises(ValueError, match="Invalid broker FQDN"):
        get_broker_zone(fqdn)


@pytest.mark.parametrize(
    "fqdn, expected_id",
    [
        ("kafka-0.zone-a.c.project.internal:9092", 0),
        ("kafka-1.zone-b.c.project.internal:9092", 1),
        ("kafka-spans-42.us-central1-c.c.project.internal:9092", 42),
        ("kafka-0.us-east-1.c.project.internal:9092", 0),
    ],
)
def test_get_broker_id(fqdn: str, expected_id: int) -> None:
    assert get_broker_id(fqdn) == expected_id


@pytest.mark.parametrize(
    "fqdn",
    [
        "no-number.zone-a.c.project.internal:9092",
        "kafka.zone-a.c.project.internal:9092",
        "",
    ],
)
def test_get_broker_id_invalid(fqdn: str) -> None:
    with pytest.raises(ValueError, match="Invalid broker FQDN"):
        get_broker_id(fqdn)
