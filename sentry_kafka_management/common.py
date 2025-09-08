import argparse
from argparse import ArgumentParser


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
        "-c", "--cluster-config", required=True, help="Path to the cluster YAML configuration file"
    )
    parser.add_argument(
        "-t", "--topic-config", required=True, help="Path to the topics' YAML configuration file"
    )
    return parser
