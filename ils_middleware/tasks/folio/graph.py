import json
import logging

import httpx
import rdflib
from bluecore_models.utils.graph import CONTEXT_URL, load_jsonld

logger = logging.getLogger(__name__)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")


def normalize_context(doc):
    """Swap Blue Core's '<bluecore>/api/context.jsonld' for the bibframe-json
    context URL, which load_jsonld reads without fetching. Mirrors
    bluecore_api.app.utils.jsonld.normalize_context, which only covers inbound data."""
    if isinstance(doc, dict) and str(doc.get("@context")).endswith(
        "/api/context.jsonld"
    ):
        return {**doc, "@context": CONTEXT_URL}
    return doc


def _build_graph(json_ld: dict | list, instance_uri: str) -> tuple:
    """Builds RDF Graph from BF Instance's RDF and retrieves
    and parses RDF from Work"""
    graph = load_jsonld(normalize_context(json_ld))

    work_uri = graph.value(subject=rdflib.URIRef(instance_uri), predicate=BF.instanceOf)

    if work_uri is None:
        raise ValueError(f"Instance {instance_uri} missing bf:instanceOf")

    # Retrieve JSON-LD from Work RDF
    work_result = httpx.get(
        f"{work_uri}?expand=true",
        headers={"Accept": "application/ld+json"},
        follow_redirects=True,
    )

    if work_result.status_code > 399:
        raise ValueError(f"Error retrieving {work_uri}")

    graph += load_jsonld(normalize_context(json.loads(work_result.text)))
    logger.debug(f"Graph triples {len(graph)}")
    return graph, str(work_uri)


def construct_graph(**kwargs):
    task_instance = kwargs["task_instance"]

    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    for instance_uri in resources:
        instance_uuid = instance_uri.split("/")[-1]
        resource = task_instance.xcom_pull(
            key=instance_uuid, task_ids="api-message-parse"
        ).get("resource")
        graph, work_uri = _build_graph(resource, instance_uri)
        task_instance.xcom_push(
            key=instance_uuid,
            value={"graph": graph.serialize(format="json-ld"), "work_uri": work_uri},
        )
    return "constructed_graphs"
