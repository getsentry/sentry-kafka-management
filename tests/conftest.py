import tempfile
import yaml
from pathlib import Path
import pytest


@pytest.fixture
def temp_clusters_config():
    """Create a temporary clusters configuration file with two clusters."""
    clusters_config = {
        "cluster1": {
            "brokers": ["broker1:9092", "broker2:9092"],
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": None,
            "sasl_username": None,
            "sasl_password": None
        },
        "cluster2": {
            "brokers": ["broker3:9092", "broker4:9092"],
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "user1",
            "sasl_password": "pass1"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(clusters_config, f)
        temp_path = Path(f.name)
    
    yield temp_path
    
    # Cleanup
    temp_path.unlink()


@pytest.fixture
def temp_topics_config():
    """Create a temporary topics configuration file with topics for two clusters."""
    topics_config = {
        "cluster1": {
            "topic1": {
                "partitions": 3,
                "placement": {"rack": "rack1"},
                "replication_factor": 2,
                "settings": {"retention.ms": "86400000"}
            },
            "topic2": {
                "partitions": 5,
                "placement": {"rack": "rack2"},
                "replication_factor": 3,
                "settings": {"cleanup.policy": "delete"}
            }
        },
        "cluster2": {
            "topic3": {
                "partitions": 2,
                "placement": {"zone": "zone1"},
                "replication_factor": 2,
                "settings": {"compression.type": "snappy"}
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(topics_config, f)
        temp_path = Path(f.name)
    
    yield temp_path
    
    # Cleanup
    temp_path.unlink()

