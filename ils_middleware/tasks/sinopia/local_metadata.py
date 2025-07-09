"""Adds Sinopia localAdminMetadata record."""

import json
import datetime
import logging
import typing
import uuid

import rdflib
import requests  # type: ignore

from airflow.models import Variable

logger = logging.getLogger(__name__)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
BFLC = rdflib.Namespace("http://id.loc.gov/ontologies/bflc/")
SINOPIA = rdflib.Namespace("http://sinopia.io/vocabulary/")


def _add_local_system_id(
    graph: rdflib.Graph, identifier: str, ils: str
) -> rdflib.BNode:
    """Adds Local System ID to the localSystemMetadata"""
    id_blank_node = rdflib.BNode()
    graph.add((id_blank_node, rdflib.RDF.type, BF.Local))
    graph.add((id_blank_node, rdflib.RDF.value, rdflib.Literal(identifier)))
    source_blank_node = rdflib.BNode()
    graph.add((id_blank_node, BF.source, source_blank_node))
    graph.add((source_blank_node, rdflib.RDF.type, BF.Source))
    graph.add((source_blank_node, rdflib.RDFS.label, rdflib.Literal(ils)))
    return id_blank_node


def _pull_identifiers(tasks, resource_uri, instance) -> typing.Optional[str]:
    resource_uuid = resource_uri.split("/")[-1]
    for task_id in tasks:
        value = instance.xcom_pull(key=resource_uuid, task_ids=task_id)
        if value:
            return value
    return None


def _retrieve_ils_identifiers(ils_tasks, resource_uri, instance) -> dict:
    ils_info = {}
    for ils, tasks in ils_tasks.items():
        identifier = _pull_identifiers(tasks, resource_uri, instance)
        if identifier:
            ils_info[ils] = identifier
    return ils_info


def create_admin_metadata(**kwargs) -> str:
    """Creates a Sinopia Local Admin Metadata graph and returns as JSON-LD."""
    admin_metadata_uri = kwargs.get("admin_metadata_uri", "")
    instance_uri = kwargs.get("instance_uri")
    ils_identifiers = kwargs.get("ils_identifiers", {})
    cataloger_id = kwargs.get("cataloger_id")

    graph = rdflib.Graph()
    local_admin_metadata = rdflib.URIRef(admin_metadata_uri)
    graph.add((local_admin_metadata, rdflib.RDF.type, SINOPIA.LocalAdminMetadata))
    graph.add(
        (
            local_admin_metadata,
            SINOPIA.hasResourceTemplate,
            rdflib.Literal("pcc:sinopia:localAdminMetadata"),
        )
    )
    graph.add(
        (
            local_admin_metadata,
            SINOPIA.localAdminMetadataFor,
            rdflib.URIRef(instance_uri),  # type: ignore
        )
    )
    if cataloger_id:
        graph.add(
            (local_admin_metadata, BFLC.catalogerId, rdflib.Literal(cataloger_id))
        )
    for ils, identifier in ils_identifiers.items():
        if identifier:
            ident_bnode = _add_local_system_id(graph, identifier, ils)
            graph.add((local_admin_metadata, BF.identifier, ident_bnode))
    export_date = datetime.datetime.utcnow().isoformat()
    graph.add((local_admin_metadata, SINOPIA.exportDate, rdflib.Literal(export_date)))
    return graph.serialize(format="json-ld")


def new_local_admin_metadata(*args, **kwargs):
    "Add Identifier to Sinopia localAdminMetadata."
    task_instance = kwargs["task_instance"]
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    jwt = kwargs.get("jwt")
    ils_tasks = kwargs.get("ils_tasks")

    user = Variable.get("sinopia_user")
    kwargs["cataloger_id"] = user
    sinopia_api_uri = Variable.get("sinopia_api_uri")

    for resource_uri in resources:
        resource_uuid = resource_uri.split("/")[-1]
        message = task_instance.xcom_pull(
            key=resource_uuid, task_ids="api-message-parse"
        )
        resource = message.get("resource")
        group = resource.get("group")
        logger.debug(resource)

        editGroups = resource.get("editGroups", [])

        admin_metadata_uri = f"{sinopia_api_uri}/{uuid.uuid4()}"

        ils_identifiers = _retrieve_ils_identifiers(
            ils_tasks, resource_uri, task_instance
        )

        local_metadata_rdf = create_admin_metadata(
            instance_uri=resource_uri,
            admin_metadata_uri=admin_metadata_uri,
            ils_identifiers=ils_identifiers,
        )

        local_metadata_rdf = json.loads(local_metadata_rdf)

        headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}

        sinopia_doc = {
            "data": local_metadata_rdf,
            "user": user,
            "group": group,
            "editGroups": editGroups,
            "templateId": "pcc:sinopia:localAdminMetadata",
            "types": [
                str(SINOPIA.LocalAdminMetadata),
            ],
            "bfAdminMetadataRefs": [],
            "bfItemRefs": [],
            "bfInstanceRefs": [
                resource_uri,
            ],
            "bfWorkRefs": [],
            "sinopiaLocalAdminMetadataForRefs": [
                resource_uri,
            ],
        }

        logger.debug(sinopia_doc)

        new_admin_result = requests.post(
            admin_metadata_uri,
            json=sinopia_doc,
            headers=headers,
        )

        if new_admin_result.status_code > 399:
            msg = f"Failed to add localAdminMetadata, {new_admin_result.status_code}\n{new_admin_result.text}"
            logger.error(msg)
            raise Exception(msg)

        logger.debug(f"Results of new_admin_result {new_admin_result.text}")
        task_instance.xcom_push(key=resource_uuid, value=admin_metadata_uri)
