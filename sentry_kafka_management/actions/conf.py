# Shared configuration globals used by actions
KAFKA_TIMEOUT = 5

# Configs that are allowed to be updated on a broker with
# apply_configs and remove_dynamic_configs actions
ALLOWED_CONFIGS = [
    "follower.replication.throttled.rate",
    "follower.replication.throttled.replicas",
    "leader.replication.throttled.rate",
    "leader.replication.throttled.replicas",
]
