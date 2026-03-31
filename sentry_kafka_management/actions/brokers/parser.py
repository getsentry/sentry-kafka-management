def get_broker_zone(fqdn: str) -> str:
    """
    Get the zone from a broker FQDN.
    FQDN follows the GCP Zonal DNS format: INSTANCE_NAME.ZONE.c.PROJECT_ID.internal
    """
    try:
        return fqdn.split(":")[0].split(".")[1]
    except Exception:
        raise ValueError(f"Invalid broker FQDN: {fqdn}") from None
