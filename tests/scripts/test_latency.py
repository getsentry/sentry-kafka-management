from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sentry_kafka_management.scripts.latency.consumer_latency import (
    consumer_latency,
)


@patch("sentry_kafka_management.scripts.latency.consumer_latency.time.sleep")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.run_latency_metrics_action")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.YamlKafkaConfig")
def test_consumer_latency_requires_statsd_options(
    mock_yaml_config: MagicMock,
    mock_metrics_backend: MagicMock,
    mock_run_latency_metrics: MagicMock,
    mock_sleep: MagicMock,
    temp_config: Path,
) -> None:
    runner = CliRunner()

    result = runner.invoke(consumer_latency, ["--config", str(temp_config)])

    assert result.exit_code != 0
    mock_yaml_config.assert_not_called()
    mock_metrics_backend.assert_not_called()
    mock_run_latency_metrics.assert_not_called()
    mock_sleep.assert_not_called()


@patch("sentry_kafka_management.scripts.latency.consumer_latency.time.sleep")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.run_latency_metrics_action")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.YamlKafkaConfig")
def test_consumer_latency_accepts_statsd_options(
    mock_yaml_config: MagicMock,
    mock_metrics_backend: MagicMock,
    mock_run_latency_metrics: MagicMock,
    mock_sleep: MagicMock,
    temp_config: Path,
) -> None:
    runner = CliRunner()
    mock_sleep.side_effect = KeyboardInterrupt

    result = runner.invoke(
        consumer_latency,
        [
            "--config",
            str(temp_config),
            "--statsd-host",
            "localhost",
            "--statsd-port",
            "8126",
        ],
    )

    assert result.exit_code != 0
    mock_yaml_config.assert_called_once_with(temp_config)
    mock_metrics_backend.assert_called_once_with("localhost", 8126)
    mock_run_latency_metrics.assert_called_once_with(
        mock_yaml_config.return_value, mock_metrics_backend.return_value
    )
    mock_sleep.assert_called_once_with(1.0)


@patch("sentry_kafka_management.scripts.latency.consumer_latency.time.sleep")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.run_latency_metrics_action")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.scripts.latency.consumer_latency.YamlKafkaConfig")
def test_consumer_latency_repeats_with_interval(
    mock_yaml_config: MagicMock,
    mock_metrics_backend: MagicMock,
    mock_run_latency_metrics: MagicMock,
    mock_sleep: MagicMock,
    temp_config: Path,
) -> None:
    runner = CliRunner()
    mock_sleep.side_effect = KeyboardInterrupt

    result = runner.invoke(
        consumer_latency,
        [
            "--config",
            str(temp_config),
            "--statsd-host",
            "localhost",
            "--statsd-port",
            "8126",
            "--interval",
            "0.25",
        ],
    )

    assert result.exit_code != 0
    mock_yaml_config.assert_called_once_with(temp_config)
    mock_metrics_backend.assert_called_once_with("localhost", 8126)
    mock_run_latency_metrics.assert_called_once_with(
        mock_yaml_config.return_value, mock_metrics_backend.return_value
    )
    mock_sleep.assert_called_once_with(0.25)
