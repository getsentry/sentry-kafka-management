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
    assert _str_to_dict("{STATIC_BROKER_CONFIG:fake=bar, DEFAULT_CONFIG:fake=123}") == {
        "STATIC_BROKER_CONFIG": "bar",
        "DEFAULT_CONFIG": "123",
    }
    assert _str_to_dict("{DYNAMIC_BROKER_CONFIG:bar=bin:baz}") == {
        "DYNAMIC_BROKER_CONFIG": "bin:baz"
    }
    assert _str_to_dict("{DEFAULT_CONFIG:authorizer.class.name=}") == {"DEFAULT_CONFIG": ""}
    assert _str_to_dict(
        "{STATIC_BROKER_CONFIG:listener.security.protocol.map="
        "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT, "
        "DEFAULT_CONFIG:listener.security.protocol.map="
        "PLAINTEXT:PLAINTEXT,SSL:SSL}"
    ) == {
        "STATIC_BROKER_CONFIG": "PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
        "DEFAULT_CONFIG": "PLAINTEXT:PLAINTEXT,SSL:SSL",
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


def test_parse_line() -> None:
    regular_line = (
        "offsets.topic.num.partitions=1 sensitive=false synonyms="
        "{STATIC_BROKER_CONFIG:offsets.topic.num.partitions=1, "
        "DEFAULT_CONFIG:offsets.topic.num.partitions=50}"
    )
    expected = Config(
        config_name="offsets.topic.num.partitions",
        active_value="1",
        is_sensitive=False,
        dynamic_value=None,
        dynamic_default_value=None,
        static_value="1",
        default_value="50",
    )
    assert _parse_line(regular_line) == expected

    empty_synonyms = "offsets.topic.num.partitions=1 sensitive=false synonyms={}"
    expected = Config(
        config_name="offsets.topic.num.partitions",
        active_value="1",
        is_sensitive=False,
        dynamic_value=None,
        dynamic_default_value=None,
        static_value=None,
        default_value=None,
    )
    assert _parse_line(empty_synonyms) == expected

    empty_value_line = (
        "authorizer.class.name= sensitive=false synonyms=" "{DEFAULT_CONFIG:authorizer.class.name=}"
    )
    expected = Config(
        config_name="authorizer.class.name",
        active_value="",
        is_sensitive=False,
        dynamic_value=None,
        dynamic_default_value=None,
        static_value=None,
        default_value="",
    )
    assert _parse_line(empty_value_line) == expected

    malformed_line = "offsets.topic.num.partitions=1"
    with pytest.raises(ValueError):
        _parse_line(malformed_line)
    wrong_keys = (
        "offsets.topic.num.partitions=1 sensitive=false synonyms="
        "{FOO:offsets.topic.num.partitions=1,"
        "BAR:offsets.topic.num.partitions=50}"
    )
    with pytest.raises(ValueError):
        _parse_line(wrong_keys)


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
