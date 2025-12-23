# Shared configuration globals used by actions
KAFKA_TIMEOUT = 5

ALLOWED_CONFIGS = [
    "confluent.balancer.throttle.bytes.per.second",
    "follower.replication.throttled.rate",
    "follower.replication.throttled.replicas",
    "leader.replication.throttled.rate",
    "leader.replication.throttled.replicas",
]
