from pathlib import Path


def record_config(config_name: str, config_value: str, record_dir: Path) -> None:
    """
    Takes a mapping of config names to config values.
    Records each of these in the dir specified by `record_dir`,
    creating files with names equal to the config names, each containing the config's value.
    Will overwrite any existing files.

    Args:
        config_name: Name of the config.
        config_value: Value of the config.
        record_dir: Directory to record configs in.
    """
    assert record_dir.is_dir(), "record_dir must be a directory."
    with open(record_dir / config_name, "w") as f:
        f.write(config_value)


def read_record_dir(record_dir: Path) -> dict[str, str]:
    """
    Reads all config values from a dir recorded in the format written by `record_config()`.

    Args:
        record_dir: Directory to read config records from.
    """
    assert record_dir.is_dir(), "record_dir must be a directory."
    configs: dict[str, str] = {}
    records = record_dir.iterdir()
    for record_file in records:
        assert record_file.is_file(), (
            "Expected all records in record_dir to be files,",
            f"instead found {record_file.as_posix()}.",
        )
        try:
            with open(record_file, "r") as f:
                configs[record_file.name] = f.read()
        except FileNotFoundError:
            pass
    return configs


def cleanup_config_record(record_dir: Path, config_name: str) -> None:
    """
    Deletes the given config record within the given directory.
    Used to cleanup config record files saved by `record_config()`.

    Args:
        record_dir: Directory to delete config records from.
        config_name: Name of the config to delete
    """
    record = record_dir / config_name
    try:
        record.unlink()
    except FileNotFoundError:
        pass
