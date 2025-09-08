# sentry-kafka-management
Libraries for managing our Kafka clusters/consumers, as well as Taskbroker.


### Usage
- `make install-dev` to install the development environment
- `make tests` to run the unit tests
- `make typecheck` to run Python type checking
- `make lint` to lint the code base and apply auto generated changes
- `make build` to build the wheels for the project and package the release

### Local kafka
- `devservices up` to spin up a local kafka node
- `docker exec kafka-kafka-1 kafka-topics --bootstrap-server localhost:9092 --topic test-topic --create` to create a test topic for the cluster
  - TODO: find a way to auto-create a test topic on the local kafka

### Docker

Build the image:

```bash
docker build -t sentry-kafka-management:local .
```

Show CLI help (router):

```bash
docker run --rm sentry-kafka-management:local --help
```

Run against a single clusters config via the unified CLI router (mount your config directory):

```bash
docker run --rm \
  -v "$PWD/devservices/kafka-conf:/config" \
  --network "devservices" \
  sentry-kafka-management:local \
  get-topics \
  -c /config/local-clusters.yml \
  -t /config/local-topics.yml \
  -n <cluster-name>
```
