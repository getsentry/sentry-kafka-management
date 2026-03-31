import re

BrokerId = int


def get_broker_zone(fqdn: str) -> str:
    """
    Get the zone from a broker FQDN.
    FQDN follows the GCP Zonal DNS format: INSTANCE_NAME.ZONE.c.PROJECT_ID.internal
    """
    try:
        return fqdn.split(":")[0].split(".")[1]
    except Exception:
        raise ValueError(f"Invalid broker FQDN: {fqdn}") from None


def get_broker_id(fqdn: str) -> BrokerId:
    """
    Get the broker ID from a broker FQDN.
    FQDN follows the GCP Zonal DNS format: INSTANCE_NAME-<id>.ZONE.c.PROJECT_ID.internal
    """
    match = re.match(r"^[^.]+-(\d+)\.", fqdn)
    if not match:
        raise ValueError(f"Invalid broker FQDN: {fqdn}")
    return int(match.group(1))
