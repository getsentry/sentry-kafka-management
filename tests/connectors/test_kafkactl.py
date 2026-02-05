from pathlib import Path
from subprocess import CalledProcessError
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest
import yaml

from sentry_kafka_management.brokers import YamlKafkaConfig
from sentry_kafka_management.connectors.kafkactl import (
    _generate_password_env,
    _maybe_create_conf,
    run_kafkactl,
)
from tests.conftest import DummyKafkaConfig


def test_maybe_create_conf(temp_config: Path) -> None:
    with TemporaryDirectory() as tmpdir:
        kafkactl_conf = Path(tmpdir) / "kafkactl.yaml"
        skm_conf = YamlKafkaConfig(temp_config)
        _maybe_create_conf(kafkactl_conf, skm_conf)

        expected = {
            "contexts": {
                "cluster1": {
                    "brokers": ["broker1:9092", "broker2:9092"],
                    "requesttimeout": "10s",
                },
                "cluster2": {
                    "brokers": ["broker3:9092", "broker4:9092"],
                    "requesttimeout": "10s",
                    "sasl": {"enabled": True, "mechanism": "PLAIN", "username": "user1"},
                },
            }
        }
        with open(kafkactl_conf) as f:
            data = yaml.safe_load(f)
            assert data == expected


def test_generate_password_env(temp_config: Path) -> None:
    clusters = YamlKafkaConfig(temp_config)
    res = _generate_password_env(clusters)
    assert res == {"CONTEXTS_CLUSTER2_SASL_PASSWORD": "pass1"}


@patch("sentry_kafka_management.connectors.kafkactl.subprocess.run")
@patch(
    "sentry_kafka_management.connectors.kafkactl._generate_password_env",
    return_value={"CONTEXTS_CLUSTER2_SASL_PASSWORD": "pass1"},
)
@patch("sentry_kafka_management.connectors.kafkactl._maybe_create_conf")
def test_run_kafkactl_success(
    mock_conf: MagicMock,
    mock_password: MagicMock,
    mock_run: MagicMock,
) -> None:
    mock_run.return_value.stdout = "command_output"

    kafkactl_exec = "/usr/bin/kafkactl"
    kafkactl_conf = "/var/conf/kafkactl.yaml"
    cli_args = ["describe", "topic", "ingest-events"]
    output = run_kafkactl(
        cli_args,
        DummyKafkaConfig(),
        Path(kafkactl_exec),
        Path(kafkactl_conf),
    )
    mock_run.assert_called_once_with(
        [kafkactl_exec, "-C", kafkactl_conf] + cli_args,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
        env={"CONTEXTS_CLUSTER2_SASL_PASSWORD": "pass1"},
    )
    assert output == "command_output"


@patch(
    "sentry_kafka_management.connectors.kafkactl.subprocess.run",
    side_effect=CalledProcessError(returncode=2, cmd=["foo"]),
)
@patch("sentry_kafka_management.connectors.kafkactl._generate_password_env")
@patch("sentry_kafka_management.connectors.kafkactl._maybe_create_conf")
def test_run_kafkactl_failure(
    mock_conf: MagicMock,
    mock_password: MagicMock,
    mock_run: MagicMock,
) -> None:
    kafkactl_exec = "/usr/bin/kafkactl"
    kafkactl_conf = "/var/conf/kafkactl.yaml"
    cli_args = ["describe", "topic", "ingest-events"]
    with pytest.raises(RuntimeError):
        run_kafkactl(
            cli_args,
            DummyKafkaConfig(),
            Path(kafkactl_exec),
            Path(kafkactl_conf),
        )
