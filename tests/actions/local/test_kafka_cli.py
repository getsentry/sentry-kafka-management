from unittest.mock import MagicMock, patch

import pytest

from sentry_kafka_management.actions.local.kafka_cli import (
    Config,
    _parse_line,
    _parse_output,
    _run_kafka_configs_describe,
    _str_to_bool,
    _str_to_dict,
    get_active_broker_configs,
)


def test_str_to_bool() -> None:
    assert _str_to_bool("true")
    assert _str_to_bool("True")
    assert not _str_to_bool("false")
    assert not _str_to_bool("False")
    with pytest.raises(ValueError):
        _str_to_bool("foo")


def test_str_to_dict() -> None:
    assert _str_to_dict("{DEFAULT_CONFIG:fake=bar, STATIC_BROKER_CONFIG:fake=123}") == {
        "DEFAULT_CONFIG": "bar",
        "STATIC_BROKER_CONFIG": "123",
    }
    assert _str_to_dict("{DEFAULT_CONFIG:bar=bin:baz}") == {"DEFAULT_CONFIG": "bin:baz"}
    assert _str_to_dict("{DEFAULT_CONFIG:authorizer.class.name=}") == {"DEFAULT_CONFIG": ""}
    assert _str_to_dict("{DEFAULT_CONFIG:foo=, DYNAMIC_BROKER_CONFIG:foo=val}") == {
        "DEFAULT_CONFIG": "",
        "DYNAMIC_BROKER_CONFIG": "val",
    }
    with pytest.raises(ValueError):
        _str_to_dict("foo:bar")
    with pytest.raises(ValueError):
        _str_to_dict("test123")


@patch("sentry_kafka_management.actions.local.kafka_cli.subprocess.run")
def test_run_kafka_configs_describe(mock_run: MagicMock) -> None:
    mock_run_output = MagicMock()
    mock_run_output.check_returncode.return_value = 0
    mock_run_output.stdout = "All configs for broker 1 are:\n    foo:bar"
    mock_run.return_value = mock_run_output
    assert _run_kafka_configs_describe(1, "localhost:9092") == ["foo:bar"]


@pytest.mark.parametrize(
    "line, expected, negative_test",
    [
        pytest.param(
            (
                "offsets.topic.num.partitions=1 sensitive=false synonyms="
                "{STATIC_BROKER_CONFIG:offsets.topic.num.partitions=1, "
                "DEFAULT_CONFIG:offsets.topic.num.partitions=50}"
            ),
            Config(
                config_name="offsets.topic.num.partitions",
                active_value="1",
                is_sensitive=False,
                dynamic_value=None,
                dynamic_default_value=None,
                static_value="1",
                default_value="50",
            ),
            False,
            id="regular_line",
        ),
        pytest.param(
            "offsets.topic.num.partitions=1 sensitive=false synonyms={}",
            Config(
                config_name="offsets.topic.num.partitions",
                active_value="1",
                is_sensitive=False,
                dynamic_value=None,
                dynamic_default_value=None,
                static_value=None,
                default_value=None,
            ),
            False,
            id="empty_synonyms",
        ),
        pytest.param(
            (
                "authorizer.class.name= sensitive=false synonyms="
                "{DEFAULT_CONFIG:authorizer.class.name=}"
            ),
            Config(
                config_name="authorizer.class.name",
                active_value="",
                is_sensitive=False,
                dynamic_value=None,
                dynamic_default_value=None,
                static_value=None,
                default_value="",
            ),
            False,
            id="empty_value_line",
        ),
        pytest.param(
            (
                "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,"
                "SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL sensitive=false "
                "synonyms={DEFAULT_CONFIG:listener.security.protocol.map=PLAINTEXT:PLAINTEXT,"
                "SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL}"
            ),
            Config(
                config_name="listener.security.protocol.map",
                active_value=(
                    "PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL"
                ),
                is_sensitive=False,
                dynamic_value=None,
                dynamic_default_value=None,
                static_value=None,
                default_value=(
                    "PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL"
                ),
            ),
            False,
            id="caps_in_value",
        ),
        pytest.param(
            "offsets.topic.num.partitions=1",
            None,
            True,
            id="malformed_line",
        ),
        pytest.param(
            (
                "offsets.topic.num.partitions=1 sensitive=false synonyms="
                "{FOO:offsets.topic.num.partitions=1,"
                "BAR:offsets.topic.num.partitions=50}"
            ),
            None,
            True,
            id="wrong_keys",
        ),
    ],
)
def test_parse_line(line: str, expected: Config, negative_test: bool) -> None:
    """
    Tests the line parsing logic.

    Params:
        line: The line to test
        expected: The expected result
        negative_test: If True, expect a ValueError from _parse_line()
    """
    if not negative_test:
        assert _parse_line(line) == expected
    else:
        with pytest.raises(ValueError):
            _parse_line(line)


def test_parse_output() -> None:
    input = [
        "metrics.sample.window.ms=30000 sensitive=false synonyms={DEFAULT_CONFIG:metrics.sample.window.ms=30000}",  # noqa: E501
        "min.insync.replicas=1 sensitive=false synonyms={DEFAULT_CONFIG:min.insync.replicas=1}",
    ]
    expected = [
        Config(
            config_name="metrics.sample.window.ms",
            is_sensitive=False,
            active_value="30000",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="30000",
        ),
        Config(
            config_name="min.insync.replicas",
            is_sensitive=False,
            active_value="1",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="1",
        ),
    ]
    assert _parse_output(input) == expected


@patch(
    "sentry_kafka_management.actions.local.kafka_cli._run_kafka_configs_describe",
    return_value=[
        "metrics.sample.window.ms=30000 sensitive=false synonyms={DEFAULT_CONFIG:metrics.sample.window.ms=30000}",  # noqa: E501
        "min.insync.replicas=1 sensitive=false synonyms={DEFAULT_CONFIG:min.insync.replicas=1}",
    ],
)
def test_get_active_broker_configs(mock_cli: MagicMock) -> None:
    expected = [
        Config(
            config_name="metrics.sample.window.ms",
            is_sensitive=False,
            active_value="30000",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="30000",
        ),
        Config(
            config_name="min.insync.replicas",
            is_sensitive=False,
            active_value="1",
            dynamic_value=None,
            dynamic_default_value=None,
            static_value=None,
            default_value="1",
        ),
    ]
    assert get_active_broker_configs(1) == expected
