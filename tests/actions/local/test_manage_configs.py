from pathlib import Path
from unittest.mock import MagicMock, patch

from sentry_kafka_management.actions.brokers import ConfigChange
from sentry_kafka_management.actions.local.kafka_cli import Config
from sentry_kafka_management.actions.local.manage_configs import update_config_state


def broker_id_config() -> Config:
    """Create a Config object for broker.id that can be used in tests."""
    return Config(
        config_name="broker.id",
        is_sensitive=False,
        active_value="1001",
        dynamic_value=None,
        dynamic_default_value=None,
        static_value="1001",
        default_value="-1",
    )


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_emergency_priority(
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
        broker_id_config(),
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
        [
            ConfigChange(
                broker_id="1001",
                config_name="num.network.threads",
                old_value="3",
                new_value="1000",
            ).to_success()
        ],
        [],
    )
    mock_remove_configs.return_value = ([], [])

    success, errors = update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 1
    assert len(errors) == 0

    mock_apply_configs.assert_called_once_with(
        mock_admin_client,
        {"num.network.threads": "1000"},
        ["1001"],
        dry_run=True,
    )


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_apply_from_properties(
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
        broker_id_config(),
        Config(
            config_name="num.io.threads",
            is_sensitive=False,
            active_value="8",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="8",
        ),
    ]

    mock_apply_configs.return_value = (
        [
            ConfigChange(
                broker_id="1001",
                config_name="num.io.threads",
                old_value="8",
                new_value="10",
            ).to_success(),
            ConfigChange(
                broker_id="1001",
                config_name="background.threads",
                old_value=None,
                new_value="20",
            ).to_success(),
        ],
        [],
    )
    mock_remove_configs.return_value = ([], [])

    success, errors = update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 2
    assert len(errors) == 0

    mock_apply_configs.assert_called_once_with(
        mock_admin_client,
        {"num.io.threads": "10", "background.threads": "20"},
        ["1001"],
        dry_run=True,
    )


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_remove_dynamic_when_static_matches(
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
        broker_id_config(),
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
        [
            ConfigChange(
                broker_id="1001",
                config_name="message.max.bytes",
                old_value="60000000",
                new_value=None,
            ).to_success()
        ],
        [],
    )

    success, errors = update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 1
    assert len(errors) == 0
    mock_apply_configs.assert_not_called()
    mock_remove_configs.assert_called_once_with(
        mock_admin_client,
        ["message.max.bytes"],
        ["1001"],
        dry_run=True,
    )


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_keep_emergency_config(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that dynamic configs are not removed when they're emergency configs,
    even if the active value matches the static value."""
    (temp_record_dir / "num.network.threads").write_text("1000")

    temp_properties_file.write_text("broker.id=1001\n" "num.network.threads=1000\n")

    mock_get_configs.return_value = [
        broker_id_config(),
        Config(
            config_name="num.network.threads",
            is_sensitive=False,
            active_value="1000",
            dynamic_value="1000",
            dynamic_default_value=None,
            static_value="1000",
            default_value="3",
        ),
    ]

    mock_apply_configs.return_value = (
        [
            ConfigChange(
                broker_id="1001",
                config_name="num.network.threads",
                old_value="1000",
                new_value="1000",
            ).to_success()
        ],
        [],
    )
    mock_remove_configs.return_value = ([], [])

    success, errors = update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 1
    assert len(errors) == 0

    mock_apply_configs.assert_called_once_with(
        mock_admin_client,
        {"num.network.threads": "1000"},
        ["1001"],
        dry_run=True,
    )

    mock_remove_configs.assert_not_called()


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_no_op_when_active_matches_desired(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that no changes are made when active value already matches desired value."""
    temp_properties_file.write_text("broker.id=1001\n" "num.io.threads=8\n")

    mock_get_configs.return_value = [
        broker_id_config(),
        Config(
            config_name="num.io.threads",
            is_sensitive=False,
            active_value="8",  # Already matches desired
            dynamic_value=None,
            dynamic_default_value=None,
            static_value="8",
            default_value="8",
        ),
        Config(
            config_name="background.threads",
            is_sensitive=False,
            active_value="20",
            dynamic_value="20",
            dynamic_default_value=None,
            static_value=None,
            default_value="10",
        ),
    ]

    mock_apply_configs.return_value = ([], [])
    mock_remove_configs.return_value = ([], [])

    success, errors = update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        dry_run=True,
    )

    assert len(success) == 0
    assert len(errors) == 0

    mock_apply_configs.assert_not_called()
    mock_remove_configs.assert_not_called()


@patch("sentry_kafka_management.actions.local.manage_configs.remove_dynamic_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.apply_configs")
@patch("sentry_kafka_management.actions.local.manage_configs.get_active_broker_configs")
def test_update_config_state_passes_sasl_credentials_file(
    mock_get_configs: MagicMock,
    mock_apply_configs: MagicMock,
    mock_remove_configs: MagicMock,
    mock_admin_client: MagicMock,
    temp_record_dir: Path,
    temp_properties_file: Path,
) -> None:
    """Test that sasl_credentials_file is correctly passed to get_active_broker_configs."""
    temp_properties_file.write_text("broker.id=1001\n")

    # Create a temporary SASL credentials file
    sasl_file = temp_properties_file.parent / "client.properties"
    sasl_file.write_text("security.protocol=PLAINTEXT\n")

    mock_get_configs.return_value = [broker_id_config()]
    mock_apply_configs.return_value = ([], [])
    mock_remove_configs.return_value = ([], [])

    update_config_state(
        mock_admin_client,
        temp_record_dir,
        temp_properties_file,
        sasl_credentials_file=sasl_file,
        dry_run=True,
    )

    # Verify get_active_broker_configs was called with the sasl_credentials_file
    mock_get_configs.assert_called_once_with(1001, sasl_credentials_file=sasl_file)
