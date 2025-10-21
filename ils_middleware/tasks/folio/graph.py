import json
import logging

import rdflib
import requests  # type: ignore

logger = logging.getLogger(__name__)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")


def _build_graph(json_ld: list, instance_uri: str) -> tuple:
    """Builds RDF Graph from BF Instance's RDF and retrieves
    and parses RDF from Work"""
    graph = rdflib.Graph()
    graph.parse(data=json.dumps(json_ld), format="json-ld")

    work_uri = graph.value(subject=rdflib.URIRef(instance_uri), predicate=BF.instanceOf)

    if work_uri is None:
        raise ValueError(f"Instance {instance_uri} missing bf:instanceOf")

    # Retrieve JSON-LD from Work RDF
    work_result = requests.get(str(work_uri))

    if work_result.status_code > 399:
        raise ValueError(f"Error retrieving {work_uri}")

    graph.parse(data=json.dumps(work_result.json()["data"]), format="json-ld")
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

        graph, work_uri = _build_graph(resource.get("data"), instance_uri)
        task_instance.xcom_push(
            key=instance_uuid,
            value={"graph": graph.serialize(format="json-ld"), "work_uri": work_uri},
        )
    return "constructed_graphs"
