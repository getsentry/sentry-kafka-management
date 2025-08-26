from sentry_kafka_management.brokers import YamlKafkaConfig


def test_load_clusters_only(temp_clusters_config, temp_topics_config):
    """Test loading configuration with only clusters defined."""
    
    config = YamlKafkaConfig(temp_clusters_config, temp_topics_config)
    
    # Test clusters loading
    clusters = config.get_clusters()
    assert len(clusters) == 2
    assert "cluster1" in clusters
    assert "cluster2" in clusters
    
    # Verify cluster1 configuration
    cluster1 = clusters["cluster1"]
    assert isinstance(cluster1, dict)
    assert cluster1["brokers"] == ["broker1:9092", "broker2:9092"]
    assert cluster1["security_protocol"] == "PLAINTEXT"
    assert cluster1["sasl_mechanism"] is None
    assert cluster1["sasl_username"] is None
    assert cluster1["sasl_password"] is None
    
    # Verify cluster2 configuration
    cluster2 = clusters["cluster2"]
    assert isinstance(cluster2, dict)
    assert cluster2["brokers"] == ["broker3:9092", "broker4:9092"]
    assert cluster2["security_protocol"] == "SASL_SSL"
    assert cluster2["sasl_mechanism"] == "PLAIN"
    assert cluster2["sasl_username"] == "user1"
    assert cluster2["sasl_password"] == "pass1"


def test_load_clusters_and_topics(temp_clusters_config, temp_topics_config):
    """Test loading configuration with both clusters and topics defined."""
    config = YamlKafkaConfig(temp_clusters_config, temp_topics_config)
    
    # Test clusters loading
    clusters = config.get_clusters()
    assert len(clusters) == 2
    assert "cluster1" in clusters
    assert "cluster2" in clusters
    
    # Test topics loading for cluster1
    topics_cluster1 = config.get_topics_config("cluster1")
    assert len(topics_cluster1) == 2
    assert "topic1" in topics_cluster1
    assert "topic2" in topics_cluster1
    
    # Verify topic1 configuration
    topic1 = topics_cluster1["topic1"]
    assert isinstance(topic1, dict)
    assert topic1["partitions"] == 3
    assert topic1["placement"] == {"rack": "rack1"}
    assert topic1["replication_factor"] == 2
    assert topic1["settings"] == {"retention.ms": "86400000"}
    
    # Verify topic2 configuration
    topic2 = topics_cluster1["topic2"]
    assert isinstance(topic2, dict)
    assert topic2["partitions"] == 5
    assert topic2["placement"] == {"rack": "rack2"}
    assert topic2["replication_factor"] == 3
    assert topic2["settings"] == {"cleanup.policy": "delete"}
    
    # Test topics loading for cluster2
    topics_cluster2 = config.get_topics_config("cluster2")
    assert len(topics_cluster2) == 1
    assert "topic3" in topics_cluster2
    
    # Verify topic3 configuration
    topic3 = topics_cluster2["topic3"]
    assert isinstance(topic3, dict)
    assert topic3["partitions"] == 2
    assert topic3["placement"] == {"zone": "zone1"}
    assert topic3["replication_factor"] == 2
    assert topic3["settings"] == {"compression.type": "snappy"}
