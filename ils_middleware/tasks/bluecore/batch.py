import logging
import pathlib

import rdflib

from bluecore.utils.graph import BF, generate_entity_graph, init_graph

logger = logging.getLogger(__name__)

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
    file_graph.parse(data=file_path.read_text(),
                     format='json-ld')
    for work in file_graph.subjects(predicate=rdflib.RDF.type,
                                    object=BF.Work):
        if isinstance(work, rdflib.BNode):
            logger.info(f"Work {work} is a blank node, not processing")
            continue
        work_graph = generate_entity_graph(file_graph, work)
        resources.append({"class": "Work", "uri": str(work), "resource": work_graph.serialize(format='json-ld')})
    for instance in file_graph.subjects(predicate=rdflib.RDF.type,
                                        object=BF.Instance):
        if isinstance(instance, rdflib.BNode):
            logger.info(f"Instance {instance} is a blank node, not processing")
            continue
        instance_graph = generate_entity_graph(file_graph, instance)
        resources.append({"class": "Instance", "uri": str(instance), "resource": instance_graph.serialize(format='json-ld')})
    return resources