import logging
import re
import subprocess
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Sequence


class ConfigTypes(StrEnum):
    """
    Enum of allowed config types returned by `kafka-configs --describe`.
    """

    DEFAULT_CONFIG = "DEFAULT_CONFIG"
    STATIC_BROKER_CONFIG = "STATIC_BROKER_CONFIG"
    # Used when a dynamic config is set on a single broker
    DYNAMIC_BROKER_CONFIG = "DYNAMIC_BROKER_CONFIG"
    # Used when a dynamic config is set as a cluster-level default
    DYNAMIC_DEFAULT_BROKER_CONFIG = "DYNAMIC_DEFAULT_BROKER_CONFIG"

    @staticmethod
    def values() -> list[str]:
        """
        Get all enum values.
        """
        return list(ConfigTypes._value2member_map_.keys())


@dataclass
class Config:
    """
    Represents a Config object parsed from the output of the
    `kafka-configs` CLI.

    Fields:
        config_name: Name of the config
        is_sensitive: Whether or not the config is sensitive
                    (and therefore shouldn't have its value displayed)
        active_value: The current active value of the config.
                    Value hierarchy is: dynamic > static > default.
        dynamic_value: Optional. The current dynamic value for the config (if one exists).
        dynamic_default_value: Optional. The current cluster-wide dynamic value for the config
                               (if one exists).
        static_value: Optional. The current static value for the config (if one exists).
        default_value: Optional. The current default value for the config (if one exists).
    """

    config_name: str
    is_sensitive: bool
    active_value: str
    dynamic_value: str | None
    dynamic_default_value: str | None
    static_value: str | None
    default_value: str | None


def _str_to_bool(boolstr: str) -> bool:
    """
    Converts the string literal `"true"` or `"false"` to
    its boolean value.
    Raises an error if boolstr isn't a string representation of a bool.
    """
    if boolstr.lower() == "true":
        return True
    elif boolstr.lower() == "false":
        return False
    else:
        raise ValueError(f"{boolstr} is not a string representation of a bool.")


def _str_to_dict(dictstr: str) -> dict[str, str]:
    """
    Converts a string representation of a dict into a dict.
    Differs from `json.loads()` by expecting the string to be in the format:
    `"{CONFIG_TYPE:config_name=value, CONFIG_TYPE:config_name=value, ...}"`
    """
    # if dictstr is empty, return that
    if dictstr == "{}":
        return {}
    if not dictstr.startswith("{") or not dictstr.endswith("}"):
        raise ValueError(
            'Expected string representation of dict in the format `"{key:value, key:value, ...}"`, '
            f"instead got {dictstr}"
        )
    stripped = dictstr.strip("{}")

    # Regex that:
    # - extracts the config name: `({config_types_pattern})`
    # - gets rid of the config name after the colon: `:[^=]+=`
    # - captures the value until we hit a comma, optional whitespace,
    #   and a config name: `((?:(?!,\s*{config_types_pattern}:).)*)`
    # This will break if we ever have one of ConfigTypes as the value of a config,
    # if that happens something has gone terribly wrong
    config_types_pattern = "(?:" + "|".join(ConfigTypes.values()) + ")"
    regex = re.compile(rf"({config_types_pattern}):[^=]+=((?:(?!,\s*{config_types_pattern}:).)*)")

    res = {key: value.strip() for key, value in regex.findall(stripped)}
    if not all([conf in ConfigTypes.values() for conf in res.keys()]):
        raise ValueError(
            f'Extracted invalid keys from config synonyms in dict "{dictstr}", '
            f"expected one of {ConfigTypes.values()} but got {res.keys()}"
        )
    if not res:
        raise ValueError(f"Could not extract config synonyms from string '{dictstr}'")
    return res


def _run_kafka_configs_describe(
    broker_id: int,
    bootstrap_server: str,
    sasl_credentials_file: Path | None = None,
) -> list[str]:
    """
    Runs `kafka-configs --entity-type brokers --describe --all` against
    the given broker and returns the output as a list of lines.
    Removes the first line as that doesn't contain a config.

    Params:
        broker_id: Configs retrieved will be filtered to just this broker
        bootstrap_server: Host/port of Kafka's external listener
    """
    # We aren't running with `shell=True`, but more validation doesn't hurt
    if type(broker_id) is not int:
        raise ValueError(f"{broker_id} is not a valid value for broker ID.")

    command = [
        "kafka-configs",
        "--bootstrap-server",
        bootstrap_server,
        "--entity-type",
        "brokers",
        "--entity-name",
        str(broker_id),
        "--describe",
        "--all",
    ]
    if sasl_credentials_file is not None:
        command.extend(["--command-config", str(sasl_credentials_file)])
    res = subprocess.run(command, capture_output=True, text=True)
    try:
        res.check_returncode()
    except subprocess.CalledProcessError as e:
        logging.error(e.stdout)
        logging.error(e.stderr)
        raise e
    lines = res.stdout.split("\n")
    if lines[0].strip() != f"All configs for broker {broker_id} are:":
        raise ValueError(f"Got unexpected output from kafka-configs:\n{lines}")
    return [line.strip() for line in lines[1:] if line]


def _parse_line(line: str) -> Config:
    """
    Parses a single line from the output of `_run_kafka_configs` into a Config object.
    """
    items = line.split(" ", 2)
    # validate the line has the expected number of items in it
    if len(items) != 3:
        raise ValueError(f"Config line had an unexpected number of items: {items}")
    # extract config name and current value (limit to 1 as value can contain equals sign)
    [name, value] = items[0].split("=", 1)
    # extract if config is sensitive
    is_sensitive = _str_to_bool(items[1].split("=")[1])
    # extract dynamic/static/default values, if they exist
    synonyms = _str_to_dict(items[2].split("=", 1)[1])
    return Config(
        config_name=name,
        active_value=value,
        is_sensitive=is_sensitive,
        dynamic_value=synonyms.get(ConfigTypes.DYNAMIC_BROKER_CONFIG),
        dynamic_default_value=synonyms.get(ConfigTypes.DYNAMIC_DEFAULT_BROKER_CONFIG),
        static_value=synonyms.get(ConfigTypes.STATIC_BROKER_CONFIG),
        default_value=synonyms.get(ConfigTypes.DEFAULT_CONFIG),
    )


def _parse_output(lines: Sequence[str]) -> list[Config]:
    """
    Takes the output from `_run_kafka_configs` and parses it into a list of Config objects.
    """
    res = []
    for line in lines:
        res.append(_parse_line(line))
    return res


def get_active_broker_configs(
    broker_id: int,
    bootstrap_servers: str = "localhost:9092",
    sasl_credentials_file: Path | None = None,
) -> list[Config]:
    """
    Runs `kafka-configs --entity-type brokers --describe --all` against
    the given broker (default localhost), and parses its output to get a list of all configs
    (and their active values on the broker).

    This differs from the `describe_broker_configs()` action as it tells you
    both the active value for a config, but also the underlying
    dynamic/static/default values for that config. This means we can get
    the static value that Kafka has stored for a config even if that config has
    a dynamic value set.
    """
    kafka_configs_lines = _run_kafka_configs_describe(
        broker_id, bootstrap_servers, sasl_credentials_file
    )
    configs = _parse_output(kafka_configs_lines)
    return configs
