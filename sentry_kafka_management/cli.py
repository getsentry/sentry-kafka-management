#!/usr/bin/env python3

from __future__ import annotations

import argparse
from typing import Callable, Mapping, Sequence

from sentry_kafka_management.scripts.brokers import describe_broker_configs
from sentry_kafka_management.scripts.topics import list_topics

FUNCTIONS: Mapping[str, Callable[[Sequence[str] | None], int]] = {
    "get-topics": list_topics,
    "describe-broker-configs": describe_broker_configs,
}


def main(argv: Sequence[str] | None = None) -> int:
    # Build dynamic epilog that shows available functions with their docstrings
    def _functions_epilog() -> str:
        lines: list[str] = ["Available functions:"]
        for name in sorted(FUNCTIONS.keys()):
            func = FUNCTIONS[name]
            doc = (func.__doc__ or "").strip()
            summary = doc.splitlines()[0] if doc else ""
            if summary:
                lines.append(f"  {name}: {summary}")
            else:
                lines.append(f"  {name}")
        return "\n".join(lines)

    parser = argparse.ArgumentParser(
        description="Router CLI for sentry-kafka-management. "
        "Provide a function, remaining args are delegated to that script.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_functions_epilog(),
    )
    parser.add_argument(
        "function",
        nargs="?",
        choices=sorted(FUNCTIONS.keys()),
    )

    # Parse only the function; leave the rest for the target script
    args, remainder = parser.parse_known_args(list(argv) if argv is not None else None)
    if args.function is None:
        parser.print_help()
        return 2

    target_main = FUNCTIONS[args.function]
    result = target_main(remainder)
    if isinstance(result, int):
        return result
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
