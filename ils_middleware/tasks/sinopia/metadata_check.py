"""Retrieves related AdminMetadata resource info for downstream tasks."""

import json
import logging

import rdflib
import requests  # type: ignore

from typing import Optional

logger = logging.getLogger(__name__)


def _query_for_ils_info(graph_jsonld: str, uri: str) -> dict:
    graph = rdflib.Graph()
    graph.parse(data=graph_jsonld, format="json-ld")
    output = {}

    # In localAdminMetadata the identifier is modeled with a blank
    # node and is represented as ?ident_bnode in query below
    ils_info_query = f"""PREFIX sinopia: <http://sinopia.io/vocabulary/>
    PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

    SELECT ?ils ?identifier
    WHERE {{
        <{uri}> bf:identifier ?ident_bnode .
        ?ident_bnode rdf:value ?identifier .
        ?ident_bnode bf:source ?source .
        ?source rdfs:label ?ils .
    }}
    """
    for row in graph.query(ils_info_query):
        output[str(row[0])] = str(row[1])  # type: ignore
    return output


def _get_retrieve_metadata_resource(uri: str) -> Optional[dict]:
    """Retrieves AdminMetadata resource and extracts any ILS identifiers"""
    metadata_result = requests.get(uri)
    if metadata_result.status_code > 399:
        msg = f"{uri} retrieval failed {metadata_result.status_code}\n{metadata_result.text}"
        logging.error(msg)
        return None

    resource = metadata_result.json()

    # Ignore and return if not using the pcc:sinopia:localAdminMetadata template
    if not resource.get("templateId").startswith("pcc:sinopia:localAdminMetadata"):
        return None
    return _query_for_ils_info(json.dumps(resource.get("data")), uri)


def _retrieve_all_metadata(bf_admin_metadata_all: list) -> Optional[list]:
    ils_info = []
    for metadata_uri in bf_admin_metadata_all:
        metadata = _get_retrieve_metadata_resource(metadata_uri)
        if metadata is None or len(metadata) < 1 or metadata in ils_info:
            continue
        ils_info.append(metadata)

    if len(ils_info) > 0:
        return ils_info

    return None


def _retrieve_all_resource_refs(resources: list) -> dict:
    retrieved_resources = {}
    for resource_uri in resources:
        result = requests.get(f"{resource_uri}/relationships")
        if result.status_code > 399:
            msg = f"Failed to retrieve {resource_uri}: {result.status_code}\n{result.text}"
            logging.error(msg)
            continue

        metadata_uris = result.json().get("sinopiaHasLocalAdminMetadataInferredRefs")
        ils_info = _retrieve_all_metadata(metadata_uris)
        if ils_info:
            retrieved_resources[resource_uri] = ils_info

    return retrieved_resources


def existing_metadata_check(*args, **kwargs):
    """Queries Sinopia API for related resources of an instance."""
    task_instance = kwargs["task_instance"]
    resource_uris = task_instance.xcom_pull(
        key="resources", task_ids="api-message-parse"
    )

    resource_refs = _retrieve_all_resource_refs(resource_uris)
    new_resources = []
    overlay_resources = []
    for resource_uri in resource_uris:
        if resource_uri in resource_refs:
            overlay_resources.append(
                {"resource_uri": resource_uri, "catkey": resource_refs[resource_uri]}
            )
        else:
            new_resources.append(resource_uri)

    task_instance.xcom_push(key="new_resources", value=new_resources)
    task_instance.xcom_push(key="overlay_resources", value=overlay_resources)
