from pathlib import Path


def read_server_properties(properties_file: Path) -> dict[str, str]:
    """
    Reads a Kafka server.properties file and returns a dictionary of config names to values.

    Parses the standard Kafka server.properties format:
    - Config lines are in the format: key=value
    - Empty lines and lines starting with '#' are ignored
    - Whitespace around keys and values is stripped

    Args:
        properties_file: Path to the server.properties file
    """
    if not properties_file.exists():
        raise FileNotFoundError(f"Properties file not found: {properties_file}")

    if not properties_file.is_file():
        raise ValueError(f"Path is not a file: {properties_file}")

    configs: dict[str, str] = {}

    with open(properties_file, "r") as f:
        for line_num, line in enumerate(f, start=1):
            # Strip whitespace from the line
            line = line.strip()

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            # Parse key=value pairs
            if "=" in line:
                # Split on first '=' to handle values that contain '='
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()

                if key:  # Only add if key is non-empty
                    configs[key] = value
            else:
                # Skip malformed lines (or could raise an error if strict parsing is needed)
                print(f"Warning: Skipping malformed line {line_num}: {line}")

    return configs
