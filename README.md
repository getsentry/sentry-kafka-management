# sentry-kafka-management
Libraries for managing our Kafka clusters/consumers, as well as Taskbroker.


### Usage
- `make install-dev` to install the development environment
- `make tests` to run the unit tests
- `make typecheck` to run Python type checking
- `make lint` to lint the code base and apply auto generated changes

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
  -v "$PWD/devservices:/config" \
  sentry-kafka-management:local \
  get-topics \
  -c /config/clusters.yml \
  -n <cluster-name>
```
