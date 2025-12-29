import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from sentry_kafka_management.actions.local.server_properties import (
    read_server_properties,
)


def test_read_server_properties_basic() -> None:
    """Test reading a basic server.properties file with key-value pairs."""
    content = """# Kafka broker configuration
broker.id=0


log.dirs=/tmp/kafka-logs
# Comment 1
num.network.threads=8
num.io.threads=8

socket.send.buffer.bytes=102400

"""
    with NamedTemporaryFile(mode="w", suffix=".properties", delete=False) as f:
        f.write(content)
        f.flush()
        properties_file = Path(f.name)

    try:
        result = read_server_properties(properties_file)
        assert result == {
            "broker.id": "0",
            "log.dirs": "/tmp/kafka-logs",
            "num.network.threads": "8",
            "num.io.threads": "8",
            "socket.send.buffer.bytes": "102400",
        }
    finally:
        properties_file.unlink()


def test_read_server_properties_with_whitespace() -> None:
    """Test that whitespace around keys and values is properly stripped."""
    content = """broker.id = 0
  log.dirs=/tmp/kafka-logs
num.network.threads=8

# Comment line
num.io.threads=  16
"""
    with NamedTemporaryFile(mode="w", suffix=".properties", delete=False) as f:
        f.write(content)
        f.flush()
        properties_file = Path(f.name)

    try:
        result = read_server_properties(properties_file)
        assert result == {
            "broker.id": "0",
            "log.dirs": "/tmp/kafka-logs",
            "num.network.threads": "8",
            "num.io.threads": "16",
        }
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
