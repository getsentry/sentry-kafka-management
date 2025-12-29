from pathlib import Path
from tempfile import TemporaryDirectory

from sentry_kafka_management.actions.brokers_local.filesystem import (
    cleanup_config_record,
    read_record_dir,
    record_config,
)


def test_record_config() -> None:
    with TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        record_config(
            "num.network.threads",
            "4",
            dir_path,
        )
        with open(dir_path / "num.network.threads") as f:
            value = f.read()
            assert value == "4"
        # ensure we overwrite the file properly
        record_config(
            "num.network.threads",
            "5",
            dir_path,
        )
        with open(dir_path / "num.network.threads") as f:
            value = f.read()
            assert value == "5"


def test_read_record_dir() -> None:
    confs = ["num.network.threads", "num.io.threads"]
    with TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        for conf in confs:
            record_config(
                conf,
                "4",
                dir_path,
            )

        result = read_record_dir(dir_path)
        assert result == {"num.network.threads": "4", "num.io.threads": "4"}
        assert len(list(dir_path.iterdir())) == 2


def test_cleanup_config_record() -> None:
    confs = ["num.network.threads", "num.io.threads"]
    with TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        for conf in confs:
            record_config(
                conf,
                "4",
                dir_path,
            )
        for conf in confs:
            cleanup_config_record(dir_path, conf)
        assert len(list(dir_path.iterdir())) == 0
