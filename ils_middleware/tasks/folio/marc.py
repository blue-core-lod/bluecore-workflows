"""Retrieves a FOLIO Instance's MARC record from Source Record Storage and
transforms it to BIBFRAME with marc2bibframe, the alternative to mapping the
Inventory JSON in bibframe.py."""

import json
import logging

import pymarc
from airflow.sdk import task
from bluecore_models.utils.marc import replace_dlc_assigner
from marc_bibframe import marcxml_to_graph

from ils_middleware.tasks.folio.bibframe import folio_client
from ils_middleware.tasks.folio.inventory import marc_base_uri

logger = logging.getLogger(__name__)


def marc_json_to_xml(marc_json: dict) -> str:
    """Converts a MARC-in-JSON record, as SRS stores it, to MARC XML"""
    (record,) = pymarc.parse_json_to_array(json.dumps(marc_json))
    return pymarc.record_to_xml(record, namespace=True).decode("utf-8")


@task
def retrieve_marc_record(folio_instance: dict, institution: str) -> str:
    """
    Retrieves the MARC record behind a FOLIO Instance from Source Record
    Storage and returns it as MARC XML
    """
    hrid = folio_instance.get("hrid")
    source = folio_instance.get("source")
    if source != "MARC":
        raise ValueError(f"{hrid} has no MARC record in FOLIO, its source is {source}")
    srs_record = folio_client(institution).folio_get(
        f"/source-storage/records/{folio_instance['id']}/formatted",
        query_params={"idType": "INSTANCE"},
    )
    logger.info(f"Retrieved MARC record for {hrid}")
    return marc_json_to_xml(srs_record["parsedRecord"]["content"])


@task
def marc_to_bibframe(marc_xml: str, institution: str) -> str:
    """
    Transforms MARC XML to BIBFRAME with marc2bibframe, applies Blue Core's
    assigner policy as the marc_to_bibframe DAG does, and returns RDF/XML
    """
    graph = marcxml_to_graph(marc_xml, baseuri=marc_base_uri(institution))
    replace_dlc_assigner(graph)
    return graph.serialize(format="xml")
