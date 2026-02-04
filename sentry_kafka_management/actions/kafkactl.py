import os
import subprocess
from pathlib import Path
from typing import Any

import yaml

from sentry_kafka_management.brokers import KafkaConfig


def _setup_cluster_password(skm_conf: KafkaConfig) -> None:
    """
    In order to not hardcode Kafka passwords in the kafkactl config file,
    we can set the password as an env var on the pod in the form
    `CONTEXTS_{CLUSTER_NAME}_SASL_PASSWORD`.

    See https://github.com/deviceinsight/kafkactl/tree/main?tab=readme-ov-file#configuration-via-environment-variables # noqa: E501
    for more details.
    """
    for cluster, config in skm_conf.get_clusters().items():
        if config["sasl_password"]:
            if config["password_is_plaintext"]:
                password = config["sasl_password"]
            else:
                password = os.path.expandvars(config["sasl_password"])
            cluster_name = cluster.replace("-", "_").upper()
            os.environ[f"CONTEXTS_{cluster_name}_SASL_PASSWORD"] = password


def _maybe_create_conf(kafkactl_conf_path: Path, skm_conf: KafkaConfig) -> None:
    """
    If a kafkactl config file doesn't exist at `kafkactl_conf_path`, this
    generates one at that location using the cluster configs in `skm_conf`.
    """
    if not kafkactl_conf_path.exists():
        kafkactl_config: dict[str, Any] = {}
        for cluster, config in skm_conf.get_clusters().items():
            current_cluster: dict[str, Any] = {
                "brokers": config["brokers"],
                "requesttimeout": "10s",
            }
            if config["sasl_mechanism"]:
                current_cluster["sasl"] = {
                    "enabled": True,
                    "mechanism": config["sasl_mechanism"],
                    "username": config["sasl_username"],
                }
            kafkactl_config[cluster] = current_cluster
        with open(kafkactl_conf_path, "w") as f:
            yaml.dump({"contexts": kafkactl_config}, f)


def run_kafkactl(
    cli_args: list[str],
    skm_conf: KafkaConfig,
    kafkactl_exec: Path = Path("/usr/bin/kafkactl"),
    kafkactl_conf: Path = Path("~/.config/kafkactl/config.yml").expanduser(),
) -> str:
    """
    Runs the given kafkactl command, returning the stdout output.

    Args:
        cli_args: A list of args to pass to `kafkactl`.
        kafkactl_exec: The path to the `kafkactl` executable.
        kafkactl_conf: The path to the `kafkactl` config file.
            If one doesn't exist, it'll be generated at this location
            from the sentry-kafka-management config file.
        skm_conf: A sentry-kafka-management config object.
    Returns:
        The stdout from the kafkactl command result.
    """
    _maybe_create_conf(kafkactl_conf, skm_conf)
    _setup_cluster_password(skm_conf)

    try:
        result = subprocess.run(
            [kafkactl_exec.as_posix(), "-C", kafkactl_conf.as_posix()] + cli_args,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        error_msg = f"Command failed with exit code {e.returncode}"
        if e.stderr:
            error_msg += f"\nstderr: {e.stderr}"
        if e.stdout:
            error_msg += f"\nstdout: {e.stdout}"
        raise RuntimeError(error_msg) from e

    return result.stdout
