"""Lookups shared by the tasks that read FOLIO Inventory."""

import re

from folioclient import FolioClient


def marc_base_uri(institution: str) -> str:
    """
    Base for the URIs marc2bibframe mints, which have the form
    {base}{001}#Work. FOLIO keeps the Instance HRID in the 001, so the URIs
    are stable across runs and, kept as derivedFrom, let a rerun find the
    resources already saved
    """
    return f"urn:folio:{institution}:"


def cql_quote(value: str) -> str:
    """
    Escapes a value for a double-quoted CQL term: the backslash and quote,
    and the *, ? and ^ that CQL would otherwise read as masking characters
    """
    return re.sub(r'([\\"*?^])', r"\\\1", value)


def lookup_by_local_id(local_id: str, folio_client: FolioClient) -> dict | None:
    """Looks up an existing FOLIO instance by an external local identifier.

    Queries FOLIO's own "hrid" field, since that's the property FOLIO exposes
    for this purpose; the value being searched for is our generic local_id,
    which may be an HRID or another institution's local system identifier.
    Returns None when no Instance has it.
    """
    hrid = local_id.strip()
    instances = folio_client.folio_get(
        "/inventory/instances", key="instances", query=f'hrid=="{cql_quote(hrid)}"'
    )
    if len(instances) > 1:
        raise ValueError(
            f"Expected one FOLIO Instance for {hrid}, found {len(instances)}"
        )
    return instances[0] if instances else None
