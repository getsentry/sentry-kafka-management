from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from sentry_kafka_management.actions.local.brokers import apply_desired_configs
from sentry_kafka_management.actions.local.kafka_cli import Config


@pytest.fixture
def mock_admin_client() -> MagicMock:
    """Create a mock AdminClient."""
    return MagicMock()


@pytest.fixture
def temp_record_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for emergency configs."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_properties_file() -> Generator[Path, None, None]:
    """Create a temporary server.properties file."""
    with TemporaryDirectory() as tmpdir:
        props_file = Path(tmpdir) / "server.properties"
        yield props_file


@patch("sentry_kafka_management.actions.local.brokers.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.brokers.apply_configs")
@patch("sentry_kafka_management.actions.local.brokers.get_active_broker_configs")
def test_apply_desired_configs_emergency_priority(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that emergency configs take priority over server.properties."""
    (temp_record_dir / "num.network.threads").write_text("1000")

    temp_properties_file.write_text("broker.id=1001\n" "num.network.threads=5\n")

    mock_get_configs.return_value = [
        Config(
            config_name="num.network.threads",
            is_sensitive=False,
            active_value="3",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="3",
        ),
    ]

    mock_apply_configs.return_value = (
        [{"broker_id": "1001", "config_name": "num.network.threads"}],
        [],
    )
    mock_remove_configs.return_value = ([], [])

    success, errors = apply_desired_configs(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 1
    assert len(errors) == 0


@patch("sentry_kafka_management.actions.local.brokers.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.brokers.apply_configs")
@patch("sentry_kafka_management.actions.local.brokers.get_active_broker_configs")
def test_apply_desired_configs_apply_from_properties(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that configs from server.properties are applied when they differ."""
    temp_properties_file.write_text(
        "broker.id=1001\n" "num.io.threads=10\n" "background.threads=20\n"
    )

    mock_get_configs.return_value = [
        Config(
            config_name="num.io.threads",
            is_sensitive=False,
            active_value="8",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="8",
        ),
        Config(
            config_name="background.threads",
            is_sensitive=False,
            active_value="10",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="10",
        ),
    ]

    mock_apply_configs.return_value = (
        [
            {"broker_id": "1001", "config_name": "num.io.threads"},
            {"broker_id": "1001", "config_name": "background.threads"},
        ],
        [],
    )
    mock_remove_configs.return_value = ([], [])

    success, errors = apply_desired_configs(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 2
    assert len(errors) == 0


@patch("sentry_kafka_management.actions.local.brokers.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.brokers.apply_configs")
@patch("sentry_kafka_management.actions.local.brokers.get_active_broker_configs")
def test_apply_desired_configs_remove_dynamic_when_static_matches(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that dynamic configs are removed when static value matches desired."""
    temp_properties_file.write_text("broker.id=1001\n" "message.max.bytes=50000000\n")

    mock_get_configs.return_value = [
        Config(
            config_name="message.max.bytes",
            is_sensitive=False,
            active_value="60000000",
            dynamic_value="60000000",
            dynamic_default_value=None,
            static_value="50000000",
            default_value="1048588",
        ),
    ]

    mock_apply_configs.return_value = ([], [])
    mock_remove_configs.return_value = (
        [{"broker_id": "1001", "config_name": "message.max.bytes"}],
        [],
    )

    success, errors = apply_desired_configs(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 1
    assert len(errors) == 0
    mock_remove_configs.assert_called_once()
