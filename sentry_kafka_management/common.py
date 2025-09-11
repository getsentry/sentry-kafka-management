import argparse
from argparse import ArgumentParser
from pathlib import Path


def kafka_script_parser(description: str | None, epilog: str | None) -> ArgumentParser:
    """
    Creates an argparse object with common flags for kafka scripts already provided.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help="Path to the YAML configuration file",
    )
    return parser
