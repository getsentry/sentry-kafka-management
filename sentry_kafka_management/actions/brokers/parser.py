def get_broker_zone(fqdn: str) -> str:
    """
    Get the zone from a broker FQDN.
    FQDN follows the GCP Zonal DNS format: INSTANCE_NAME.ZONE.c.PROJECT_ID.internal
    """
    try:
        return fqdn.split(":")[0].split(".")[1]
    except Exception:
        raise ValueError(f"Invalid broker FQDN: {fqdn}") from None


def get_broker_id(fqdn: str) -> int:
    """
    Get the broker ID from a broker FQDN.
    FQDN follows the GCP Zonal DNS format: INSTANCE_NAME-<id>.ZONE.c.PROJECT_ID.internal
    """
    try:
        broker_id = int(fqdn.split(":")[0].split(".")[0].split("-")[-1])
        if not isinstance(broker_id, int) or broker_id < 0:
            raise ValueError(f"Invalid broker ID: {broker_id}")
        return broker_id
    except Exception:
        raise ValueError(f"Invalid broker FQDN: {fqdn}") from None
