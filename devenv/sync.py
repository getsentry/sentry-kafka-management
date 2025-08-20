from devenv import constants
from devenv.lib import config, uv

import subprocess


def main(context: dict[str, str]) -> int:
    reporoot = context["reporoot"]
    cfg = config.get_repo(reporoot)

    uv.install(
        cfg["uv"]["version"],
        cfg["uv"][constants.SYSTEM_MACHINE],
        cfg["uv"][f"{constants.SYSTEM_MACHINE}_sha256"],
        reporoot,
    )

    # reporoot/.venv is the default venv location
    print("syncing .venv ...")
    subprocess.run(("uv", "sync", "--frozen", "--quiet"))

    return 0
