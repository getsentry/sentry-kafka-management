from pathlib import Path


def read_server_properties(properties_file: Path) -> dict[str, str]:
    """
    Reads a Kafka server.properties file and returns a dictionary of config names to values.

    Parses the standard Kafka server.properties format:
    - Config lines are in the format: key=value
    - Empty lines and comments (lines starting with '#' or '!') are ignored
    - Whitespace around keys and values is stripped
    - Line continuation with backslash (\\) is supported

    Args:
        properties_file: Path to the server.properties file
    """
    if not properties_file.exists():
        raise FileNotFoundError(f"Properties file not found: {properties_file}")

    if not properties_file.is_file():
        raise ValueError(f"Path is not a file: {properties_file}")

    configs: dict[str, str] = {}

    with open(properties_file, "r") as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].rstrip("\n\r")

        while line.endswith("\\") and i + 1 < len(lines):
            next_line = lines[i + 1].rstrip("\n\r")
            line = line[:-1] + next_line
            i += 1

        line = line.strip()

        if not line or line.startswith("#") or line.startswith("!"):
            i += 1
            continue

        if "=" in line:
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key:
                configs[key] = value
        else:
            # Don't print the full line as it may contain sensitive values
            print(f"Warning: Skipping malformed line {i + 1} (expected format: key=value)")

        i += 1

    return configs
