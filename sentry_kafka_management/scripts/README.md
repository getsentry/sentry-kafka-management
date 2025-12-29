## Scripts

`scripts/` contains CLI scripts made with Click which run various actions.

These scripts can be invoked from a container running the `sentry-kafka-management` image via the `kafka-scripts` CLI command. To add a new script to `kafka-scripts`, import it into the top-level `cli.py` and add it to the `COMMANDS` list.

### Local Scripts
If a script requires the use of local actions, that script is placed into the `local/` directory. See the Actions README for more info on local actions.
