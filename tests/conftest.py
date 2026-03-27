import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest.mock import MagicMock

import pytest
import yaml


@pytest.fixture
def temp_config() -> Generator[Path, None, None]:
    """Create a temporary configuration file with two clusters."""
    config = [
        {
            "name": "cluster1",
            "brokers": ["broker1:9092", "broker2:9092"],
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": None,
            "sasl_username": None,
            "sasl_password": None,
            "topics": [
                {
                    "name": "topic1",
                    "partitions": 3,
                    "placement": {"rack": "rack1"},
                    "replication_factor": 2,
                    "settings": {"retention.ms": "86400000"},
                },
                {
                    "name": "topic2",
                    "partitions": 5,
                    "placement": {"rack": "rack2"},
                    "replication_factor": 3,
                    "settings": {"cleanup.policy": "delete"},
                },
            ],
        },
        {
            "name": "cluster2",
            "brokers": ["broker3:9092", "broker4:9092"],
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "user1",
            "sasl_password": "pass1",
            "topics": [
                {
                    "name": "topic3",
                    "partitions": 2,
                    "placement": {"zone": "zone1"},
                    "replication_factor": 2,
                    "settings": {"compression.type": "snappy"},
                }
            ],
        },
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config, f)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    temp_path.unlink()


@pytest.fixture
def mock_admin_client() -> MagicMock:
    """Create a mock AdminClient."""
    return MagicMock()


@pytest.fixture
def temp_record_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for emergency configs."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_properties_file() -> Generator[Path, None, None]:
    """Create a temporary server.properties file with a broker.id."""
    with TemporaryDirectory() as tmpdir:
        props_file = Path(tmpdir) / "server.properties"
        yield props_file


@pytest.fixture
def temp_sasl_credentials_file() -> Generator[Path, None, None]:
    """Create a temporary SASL credentials file."""
    with TemporaryDirectory() as tmpdir:
        sasl_file = Path(tmpdir) / "client.properties"
        yield sasl_file
