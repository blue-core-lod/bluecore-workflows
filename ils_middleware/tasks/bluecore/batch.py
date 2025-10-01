import json
import logging
import os
import pathlib
import rdflib
from typing import Any, Dict, List

from bluecore_models.utils.graph import (
    BF,
    generate_entity_graph,
    generate_other_resources,
    init_graph,
    handle_external_subject,
)

BLUECORE_URL = os.environ.get("BLUECORE_URL", "https://bcld.info/")

logger = logging.getLogger(__name__)


def _add_other_resources(**kwargs):
    """
    Adds Other Resources from the entity graph to the resource list
    """
    file_graph: rdflib.Graph = kwargs["file_graph"]
    entity_graph_dict: Dict[str, Any] = kwargs["entity_graph"]
    entity: str = kwargs["entity"]
    resources: List[Dict[str, Any]] = kwargs["resources"]
    entity_graph_str = json.dumps(entity_graph_dict)
    entity_graph: rdflib.Graph = rdflib.Graph().parse(
        data=entity_graph_str, format="json-ld"
    )
    entity = rdflib.URIRef(entity)
    other_resources: List[Dict[str, str]] = generate_other_resources(
        file_graph, entity_graph
    )
    for resource in other_resources:
        # This data for other_resources is not framed.
        new_resource: Dict[str, Any] = json.loads(resource["graph"])
        resources.append(
            {
                "class": "OtherResource",
                "uri": resource["uri"],
                "resource": new_resource,
                "bibframe_resource_uri": str(entity),
            }
        )


def delete_upload(upload: str):
    """
    Deletes upload file
    """
    upload_path = pathlib.Path(upload)
    upload_path.unlink()
    parent_dir = upload_path.parent
    if parent_dir.exists() and not any(parent_dir.iterdir()):
        parent_dir.rmdir()


def is_zip(file_name: str) -> bool:
    """Determines if file is a zip file"""
    if file_name.endswith(".zip") or file_name.endswith(".gz"):
        return True
    return False


def parse_file_to_graph(file_str: str) -> List[Dict[str, Any]]:
    file_path = pathlib.Path(file_str)
    if not file_path.exists():
        raise ValueError(f"{file_path} does not exist or Airflow cannot read")
    resources: List[Dict[str, Any]] = []
    file_graph = init_graph()
    file_graph.parse(data=file_path.read_text(), format="json-ld")
    works: dict = {}
    for work in file_graph.subjects(predicate=rdflib.RDF.type, object=BF.Work):
        if isinstance(work, rdflib.BNode):
            logger.info(f"Work {work} is a blank node, not processing")
            continue
        work_graph = generate_entity_graph(file_graph, work)
        updated_payload: Dict[str, Any] = handle_external_subject(
            data=work_graph.serialize(format="json-ld"),
            type="works",
            bluecore_base_url=BLUECORE_URL,
        )
        works[str(work)] = updated_payload["uri"]
        resources.append(
            {
                "class": "Work",
                "uri": updated_payload["uri"],
                "uuid": updated_payload["uuid"],
                "resource": updated_payload["data"],
            }
        )
        _add_other_resources(
            resources=resources,
            file_graph=file_graph,
            entity_graph=updated_payload["data"],
            entity=updated_payload["uri"],
        )
    for instance in file_graph.subjects(predicate=rdflib.RDF.type, object=BF.Instance):
        if isinstance(instance, rdflib.BNode):
            logger.info(f"Instance {instance} is a blank node, not processing")
            continue
        instance_graph = generate_entity_graph(file_graph, instance)
        updated_payload = handle_external_subject(
            data=instance_graph.serialize(format="json-ld"),
            type="instances",
            bluecore_base_url=BLUECORE_URL,
        )
        instance_payload: Dict[str, Any] = {
            "class": "Instance",
            "uri": updated_payload["uri"],
            "uuid": updated_payload["uuid"],
            "resource": updated_payload["data"],
        }
        instance_of_work = instance_graph.value(
            subject=instance, predicate=BF.instanceOf
        )
        if instance_of_work is not None and str(instance_of_work) in works:
            instance_payload["work_uri"] = works.get(str(instance_of_work))
        resources.append(instance_payload)
        _add_other_resources(
            resources=resources,
            file_graph=file_graph,
            entity_graph=updated_payload["data"],
            entity=updated_payload["uri"],
        )
    return resources
