from __future__ import annotations

import argparse
from importlib import import_module
from typing import Mapping, Sequence

FUNCTIONS: Mapping[str, str] = {
    # function name -> module path (script) to execute
    "get-topics": "sentry_kafka_management.scripts.topics",
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Router CLI for sentry-kafka-management. "
        "Provide a function, remaining args are delegated to that script.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "function",
        nargs="?",
        choices=sorted(FUNCTIONS.keys()),
        help="Function to execute (e.g., get-topics)",
    )

    # Parse only the function; leave the rest for the target script
    args, remainder = parser.parse_known_args(list(argv) if argv is not None else None)

    if args.function is None:
        parser.print_help()
        return 2

    module_path = FUNCTIONS[args.function]
    module = import_module(module_path)
    target_main = getattr(module, "main", None)
    if target_main is None:
        raise SystemExit(f"Target module '{module_path}' has no 'main' callable")

    result = target_main(remainder)
    if isinstance(result, int):
        return result
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
