## Actions

`actions/` is a library of functions that interact with Kafka in some way.
Actions are separated into files based on what they interact with:
- `clusters.py` for cluster-level operations
- `topics.py` for topic-level operations
- `brokers/` for broker-level operations

These actions can be imported into our tools that interact with Kafka. Scripts can be created that run these actions in the `scripts/` directory.

### Local Actions
Most actions can be run from anywhere that can connect to Kafka via the Confluent admin client.

Some actions either read/write to a broker's filesystem, or run the built-in Kafka CLI commands, so must be run directly on a broker host. These actions are separated into the `local/` directory.
