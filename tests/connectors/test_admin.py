import os
from unittest.mock import MagicMock, call, patch

from sentry_kafka_management.brokers import ClusterConfig
from sentry_kafka_management.connectors.admin import get_admin_client


@patch("sentry_kafka_management.connectors.admin.AdminClient")
def test_get_admin_client_env_var(mock_client: MagicMock) -> None:
    test_conf_env_var = ClusterConfig(
        {
            "brokers": ["123.456.789:9092", "987.654.321:9092"],
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "SASL_PLAINTEXT",
            "sasl_username": "test_username",
            "sasl_password": "${TEST_ENV_VAR}",
            "password_is_plaintext": False,
        }
    )
    try:
        os.environ["TEST_ENV_VAR"] = "test123"
        get_admin_client(test_conf_env_var)
    finally:
        os.environ.pop("TEST_ENV_VAR")
    expected = {
        "bootstrap.servers": "123.456.789:9092,987.654.321:9092",
        "security.protocol": "PLAINTEXT",
        "sasl.mechanism": "SASL_PLAINTEXT",
        "sasl.username": "test_username",
        "sasl.password": "test123",
    }
    assert mock_client.call_args == call(expected)


@patch("sentry_kafka_management.connectors.admin.AdminClient")
def test_get_admin_client_plaintext(mock_client: MagicMock) -> None:
    test_conf_env_var = ClusterConfig(
        {
            "brokers": ["123.456.789:9092", "987.654.321:9092"],
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "SASL_PLAINTEXT",
            "sasl_username": "test_username",
            "sasl_password": "test123",
            "password_is_plaintext": True,
        }
    )
    get_admin_client(test_conf_env_var)
    expected = {
        "bootstrap.servers": "123.456.789:9092,987.654.321:9092",
        "security.protocol": "PLAINTEXT",
        "sasl.mechanism": "SASL_PLAINTEXT",
        "sasl.username": "test_username",
        "sasl.password": "test123",
    }
    assert mock_client.call_args == call(expected)
