import signal
import threading
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from sentry_kafka_management.actions.latency.consumer_latency import (
    ConsumerLatencyResult,
    TopicConsumerLatency,
)
from sentry_kafka_management.scripts.latency.consumer_latency import consumer_latency


def test_consumer_latency_requires_statsd_options(temp_config: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(consumer_latency, ["--config", str(temp_config)])

    assert result.exit_code != 0
    assert "statsd-host" in result.output


def _stop_after(iterations: int) -> Callable[..., bool]:
    """Build a ``threading.Event.wait`` side effect that stops after N waits.

    Mirrors how a SIGINT/SIGTERM handler sets the shutdown event: the loop's
    interruptible sleep returns and the process exits cleanly.
    """

    def wait_side_effect(self: threading.Event, _timeout: float | None = None) -> bool:
        if wait_side_effect.calls >= iterations:  # type: ignore[attr-defined]
            self.set()
        wait_side_effect.calls += 1  # type: ignore[attr-defined]
        return self.is_set()

    wait_side_effect.calls = 1  # type: ignore[attr-defined]
    return wait_side_effect


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
@patch.object(threading.Event, "wait", autospec=True)
def test_consumer_latency_runs_collection_loop_and_emits_metrics(
    mock_wait: MagicMock,
    mock_get_cluster_latency: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    metrics_backend = mock_metrics_backend_cls.return_value
    mock_get_cluster_latency.side_effect = (
        lambda cluster_name, *_args, **_kwargs: ConsumerLatencyResult(
            scans=[
                TopicConsumerLatency(
                    cluster_name=cluster_name,
                    group_id="group-a",
                    topic_name="topic1",
                    latency_ms=123.0,
                    partition=0,
                )
            ],
        )
    )

    mock_wait.side_effect = _stop_after(2)

    result = runner_invoke_with_statsd(temp_config, interval="0.25")

    assert result.exit_code == 0

    mock_metrics_backend_cls.assert_called_once_with("localhost", 8126)
    assert mock_wait.call_args.args[1] == 0.25
    assert mock_wait.call_count == 2

    assert mock_get_cluster_latency.call_count == 4
    called_clusters = {call.args[0] for call in mock_get_cluster_latency.call_args_list}
    assert called_clusters == {"cluster1", "cluster2"}
    assert metrics_backend.histogram.call_count == 4


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch(
    "sentry_kafka_management.scripts.latency.consumer_latency.record_consumer_group_latency_action"
)
@patch.object(threading.Event, "wait", autospec=True)
def test_consumer_latency_passes_max_workers(
    mock_wait: MagicMock,
    mock_record: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    mock_record.return_value = ConsumerLatencyResult(scans=[])
    mock_wait.side_effect = _stop_after(1)

    result = CliRunner().invoke(
        consumer_latency,
        [
            "--config",
            str(temp_config),
            "--statsd-host",
            "localhost",
            "--statsd-port",
            "8126",
            "--max-workers",
            "32",
        ],
    )

    assert result.exit_code == 0
    mock_record.assert_called_once()
    assert mock_record.call_args.args[3] == 32


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch("sentry_kafka_management.actions.latency.consumer_latency.get_cluster_latency")
@patch.object(threading.Event, "wait", autospec=True)
def test_consumer_latency_handles_iterations_with_no_scans(
    mock_wait: MagicMock,
    mock_get_cluster_latency: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    metrics_backend = mock_metrics_backend_cls.return_value
    mock_get_cluster_latency.return_value = ConsumerLatencyResult(scans=[])
    mock_wait.side_effect = _stop_after(1)

    result = runner_invoke_with_statsd(temp_config)

    assert result.exit_code == 0
    assert "No consumer latency collected" in result.output
    metrics_backend.histogram.assert_not_called()


@patch("sentry_kafka_management.scripts.latency.consumer_latency.DatadogMetricsBackend")
@patch(
    "sentry_kafka_management.scripts.latency.consumer_latency.record_consumer_group_latency_action"
)
@patch("signal.signal")
def test_consumer_latency_stops_gracefully_on_signal(
    mock_signal: MagicMock,
    mock_record: MagicMock,
    mock_metrics_backend_cls: MagicMock,
    temp_config: Path,
) -> None:
    handlers: dict[int, Callable[[int, object], None]] = {}
    mock_signal.side_effect = lambda signum, handler: handlers.__setitem__(signum, handler)

    def record_then_signal(*_args: object, **_kwargs: object) -> ConsumerLatencyResult:
        # Simulate SIGTERM (e.g. a Kubernetes pod stop) landing mid-loop.
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return ConsumerLatencyResult(scans=[])

    mock_record.side_effect = record_then_signal

    result = runner_invoke_with_statsd(temp_config)

    assert result.exit_code == 0
    assert signal.SIGINT in handlers and signal.SIGTERM in handlers
    mock_record.assert_called_once()
    stop_event = mock_record.call_args.args[4]
    assert stop_event.is_set()
    assert "shutting down" in result.output
    assert "stopped" in result.output


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
