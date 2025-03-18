import logging
import pathlib

import rdflib

from bluecore.utils.graph import (
    BF,
    generate_entity_graph,
    generate_other_resources,
    init_graph,
)

logger = logging.getLogger(__name__)


def _add_other_resources(**kwargs):
    """
    Adds Other Resources from the entity graph to the resource list
    """
    file_graph: rdflib.Graph = kwargs["file_graph"]
    entity_graph: rdflib.Graph = kwargs["entity_graph"]
    entity: rdflib.URIRef = kwargs["entity"]
    resources: list = kwargs["resources"]
    other_resources = generate_other_resources(file_graph, entity_graph)
    for resource in other_resources:
        resources.append(
            {
                "class": "OtherResource",
                "uri": resource["uri"],
                "resource": resource["graph"],
                "bibframe_resource_uri": str(entity),
            }
        )


def is_zip(file_name: str) -> bool:
    """Determines if file is a zip file"""
    if file_name.endswith(".zip") or file_name.endswith(".gz"):
        return True
    return False


def parse_file_to_graph(file_str: str) -> list:
    file_path = pathlib.Path(file_str)
    if not file_path.exists():
        raise ValueError(f"{file_path} does not exist or Airflow cannot read")
    resources = []
    file_graph = init_graph()
    file_graph.parse(data=file_path.read_text(), format="json-ld")
    works = set()
    for work in file_graph.subjects(predicate=rdflib.RDF.type, object=BF.Work):
        if isinstance(work, rdflib.BNode):
            logger.info(f"Work {work} is a blank node, not processing")
            continue
        work_graph = generate_entity_graph(file_graph, work)
        works.add(str(work))
        resources.append(
            {
                "class": "Work",
                "uri": str(work),
                "resource": work_graph.serialize(format="json-ld"),
            }
        )
        _add_other_resources(
            resources=resources,
            file_graph=file_graph,
            entity_graph=work_graph,
            entity=work,
        )
    for instance in file_graph.subjects(predicate=rdflib.RDF.type, object=BF.Instance):
        if isinstance(instance, rdflib.BNode):
            logger.info(f"Instance {instance} is a blank node, not processing")
            continue
        instance_graph = generate_entity_graph(file_graph, instance)
        instance_payload = {
            "class": "Instance",
            "uri": str(instance),
            "resource": instance_graph.serialize(format="json-ld"),
        }
        instance_of_work = instance_graph.value(
            subject=instance, predicate=BF.instanceOf
        )
        if instance_of_work is not None and str(instance_of_work) in works:
            instance_payload["work_uri"] = str(instance_of_work)
        resources.append(instance_payload)
        _add_other_resources(
            resources=resources,
            file_graph=file_graph,
            entity_graph=instance_graph,
            entity=instance,
        )
    return resources
