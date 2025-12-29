import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from sentry_kafka_management.actions.local.server_properties import (
    read_server_properties,
)


@pytest.mark.parametrize(
    "content,expected_result",
    [
        pytest.param(
            """# Kafka broker configuration
broker.id=0


log.dirs=/tmp/kafka-logs
# Comment 1
num.network.threads=8
num.io.threads=8

socket.send.buffer.bytes=102400

""",
            {
                "broker.id": "0",
                "log.dirs": "/tmp/kafka-logs",
                "num.network.threads": "8",
                "num.io.threads": "8",
                "socket.send.buffer.bytes": "102400",
            },
            id="basic",
        ),
        pytest.param(
            """broker.id = 0
  log.dirs=/tmp/kafka-logs
num.network.threads=8

# Comment line
num.io.threads=  16
""",
            {
                "broker.id": "0",
                "log.dirs": "/tmp/kafka-logs",
                "num.network.threads": "8",
                "num.io.threads": "16",
            },
            id="whitespace",
        ),
        pytest.param(
            """broker.id=0
advertised.listeners=PLAINTEXT://broker1:9092,SSL://broker1:9093,SASL_SSL://broker1:9094
listeners=PLAINTEXT://:9092,SSL://:9093
log.dirs=/var/lib/kafka/data-1,/var/lib/kafka/data-2,/var/lib/kafka/data-3
""",
            {
                "broker.id": "0",
                "advertised.listeners": (
                    "PLAINTEXT://broker1:9092,SSL://broker1:9093,SASL_SSL://broker1:9094"
                ),
                "listeners": "PLAINTEXT://:9092,SSL://:9093",
                "log.dirs": ("/var/lib/kafka/data-1,/var/lib/kafka/data-2,/var/lib/kafka/data-3"),
            },
            id="comma_separated_values",
        ),
        pytest.param(
            """broker.id=0
sasl.jaas.config=org.apache.kafka.login required username="admin" password="secret";
""",
            {
                "broker.id": "0",
                "sasl.jaas.config": (
                    'org.apache.kafka.login required username="admin" password="secret";'
                ),
            },
            id="equals_in_value",
        ),
    ],
)
def test_read_server_properties_valid_files(content: str, expected_result: dict[str, str]) -> None:
    """Test reading various valid server.properties file formats."""
    with NamedTemporaryFile(mode="w", suffix=".properties", delete=False) as f:
        f.write(content)
        f.flush()
        properties_file = Path(f.name)

    try:
        result = read_server_properties(properties_file)
        assert result == expected_result
    finally:
        properties_file.unlink()


def test_read_server_properties_file_not_found() -> None:
    """Test that FileNotFoundError is raised for non-existent files."""
    with pytest.raises(FileNotFoundError):
        read_server_properties(Path("/non/existent/file.properties"))


def test_read_server_properties_not_a_file() -> None:
    """Test that ValueError is raised when path is not a file."""
    with pytest.raises(ValueError):
        read_server_properties(Path("/tmp"))


def test_read_server_properties_permission_denied() -> None:
    """Test that PermissionError is raised when file cannot be read."""
    content = """broker.id=0"""
    with NamedTemporaryFile(mode="w", suffix=".properties", delete=False) as f:
        f.write(content)
        f.flush()
        properties_file = Path(f.name)

    try:
        os.chmod(properties_file, 0o000)
        with pytest.raises(PermissionError):
            read_server_properties(properties_file)
    finally:
        os.chmod(properties_file, 0o644)
        properties_file.unlink()
