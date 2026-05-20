from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from sentry_kafka_management.actions.latency.consumer_latency import (
    TopicConsumerLatency,
)
from sentry_kafka_management.scripts.latency.consumer_latency import consumer_latency


def test_consumer_latency_requires_statsd_options(temp_config: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(consumer_latency, ["--config", str(temp_config)])

    assert result.exit_code != 0
    assert "statsd-host" in result.output


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
@patch("time.sleep")
def test_consumer_latency_runs_collection_loop_and_emits_metrics(
    mock_sleep: MagicMock,
    mock_get_cluster_latency: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    metrics_backend = mock_metrics_backend_cls.return_value
    mock_get_cluster_latency.side_effect = lambda cluster_name, *_args, **_kwargs: [
        TopicConsumerLatency(
            cluster_name=cluster_name,
            group_id="group-a",
            topic_name="topic1",
            latency_ms=123.0,
            partition=0,
        )
    ]

    mock_sleep.side_effect = [None, KeyboardInterrupt]

    result = runner_invoke_with_statsd(temp_config, interval="0.25")

    assert result.exit_code != 0

    mock_metrics_backend_cls.assert_called_once_with("localhost", 8126)
    mock_sleep.assert_called_with(0.25)
    assert mock_sleep.call_count == 2

    assert mock_get_cluster_latency.call_count == 4
    called_clusters = {call.args[0] for call in mock_get_cluster_latency.call_args_list}
    assert called_clusters == {"cluster1", "cluster2"}
    assert metrics_backend.histogram.call_count == 4


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
@patch("time.sleep")
def test_consumer_latency_handles_iterations_with_no_scans(
    mock_sleep: MagicMock,
    mock_get_cluster_latency: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    metrics_backend = mock_metrics_backend_cls.return_value
    mock_get_cluster_latency.return_value = []
    mock_sleep.side_effect = KeyboardInterrupt

    result = runner_invoke_with_statsd(temp_config)

    assert result.exit_code != 0
    assert "No consumer latency collected" in result.output
    metrics_backend.histogram.assert_not_called()


def runner_invoke_with_statsd(temp_config: Path, interval: str | None = None) -> Result:
    args = [
        "--config",
        str(temp_config),
        "--statsd-host",
        "localhost",
        "--statsd-port",
        "8126",
    ]
    if interval is not None:
        args += ["--interval", interval]
    return CliRunner().invoke(consumer_latency, args)
