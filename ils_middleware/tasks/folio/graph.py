import json
import logging

import rdflib
import requests  # type: ignore

logger = logging.getLogger(__name__)


def _build_graph(json_ld: list, work_uri: str) -> rdflib.Graph:
    """Builds RDF Graph from BF Instance's RDF and retrieves
    and parses RDF from Work"""
    graph = rdflib.Graph()
    graph.parse(data=json.dumps(json_ld), format="json-ld")

    # Retrieve JSON-LD from Work RDF
    work_result = requests.get(work_uri)

    if work_result.status_code > 399:
        raise ValueError(f"Error retrieving {work_uri}")

    graph.parse(data=json.dumps(work_result.json()["data"]), format="json-ld")
    logger.debug(f"Graph triples {len(graph)}")
    return graph


def construct_graph(**kwargs):
    task_instance = kwargs["task_instance"]

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_uuid = instance_uri.split("/")[-1]
        resource = task_instance.xcom_pull(
            key=instance_uuid, task_ids="sqs-message-parse"
        ).get("resource")
        work_refs = resource.get("bfWorkRefs")
        if len(work_refs) < 1:
            raise ValueError(f"Missing BF Work URI for BF Instance {instance_uri}")
        work_uri = work_refs[
            0
        ]  # Grabs the first Work URI, may need a way to specify a work
        graph = _build_graph(resource.get("data"), work_uri)
        task_instance.xcom_push(
            key=instance_uuid,
            value={"graph": graph.serialize(format="json-ld"), "work_uri": work_uri},
        )
    return "constructed_graphs"
