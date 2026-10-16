"""Lookups shared by the tasks that read FOLIO Inventory."""

from folioclient import FolioClient


def lookup_by_local_id(local_id: str, folio_client: FolioClient) -> dict | None:
    """Looks up an existing FOLIO instance by an external local identifier.

    Queries FOLIO's own "hrid" field, since that's the property FOLIO exposes
    for this purpose; the value being searched for is our generic local_id,
    which may be an HRID or another institution's local system identifier.
    Returns None when no Instance has it.
    """
    hrid = local_id.strip()
    instances = folio_client.folio_get(
        "/inventory/instances", key="instances", query=f'hrid=="{hrid}"'
    )
    if len(instances) > 1:
        raise ValueError(
            f"Expected one FOLIO Instance for {hrid}, found {len(instances)}"
        )
    return instances[0] if instances else None
