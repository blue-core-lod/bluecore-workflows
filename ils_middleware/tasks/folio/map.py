"""Maps Sinopia RDF to FOLIO Records."""

import logging

import rdflib

import ils_middleware.tasks.folio.mappings.bf_instance as bf_instance_map
import ils_middleware.tasks.folio.mappings.bf_work as bf_work_map

logger = logging.getLogger(__name__)

BF_TO_FOLIO_MAP = {
    "contributor.primary.Person": {
        "template": bf_work_map.primary_contributor,
        "uri": "work",
        "class": "bf:Person",
    },
    "editions": {"template": bf_work_map.editions, "uri": "work"},
    "instance_format": {
        "template": bf_instance_map.instance_format_id,
        "uri": "instance",
    },
    "identifiers.isbn": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:Isbn",
    },
    "identifiers.oclc": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:OclcNumber",
    },
    "identifiers.lccn": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:Lccn",
    },
    "identifiers.doi": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:Doi",
    },
    "identifiers.issn": {
        "template": bf_instance_map.identifier,
        "uri": "instance",
        "class": "bf:Issn",
    },
    "instance_type": {"template": bf_work_map.instance_type_id, "uri": "work"},
    "language": {"template": bf_work_map.language, "uri": "work"},
    "modeOfIssuanceId": {
        "template": bf_instance_map.mode_of_issuance,
        "uri": "instance",
    },
    "notes": {"template": bf_instance_map.note, "uri": "instance"},
    "physical_description": {
        "template": bf_instance_map.physical_description,
        "uri": "instance",
    },
    "publication": {"template": bf_instance_map.publication, "uri": "instance"},
    "subjects": {"template": bf_work_map.subject, "uri": "work"},
    "genre": {"template": bf_work_map.genre, "uri": "work"},
    "title": {
        "template": bf_instance_map.title,
        "uri": "instance",
        "class": "bf:Title",
    },
}

FOLIO_FIELDS = BF_TO_FOLIO_MAP.keys()


def _task_id(task_groups: str) -> str:
    task_id = "bf-graph"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"
    return task_id


def _build_and_query_graph(**kwargs) -> list:
    bf_class = kwargs["bf_class"]
    instance_uri = kwargs["instance_uri"]
    task_instance = kwargs["task_instance"]
    template = kwargs["template"]
    uri_type = kwargs["uri_type"]
    task_groups = ".".join(kwargs["task_groups_ids"])

    query_params = {}
    task_id = _task_id(task_groups)
    instance_uuid = instance_uri.split("/")[-1]

    if uri_type.startswith("bf_work"):
        query_params[uri_type] = task_instance.xcom_pull(
            key=instance_uuid, task_ids=task_id
        ).get("work_uri")
    else:
        query_params[uri_type] = instance_uri
    if bf_class:
        query_params["bf_class"] = bf_class

    query = template.format(**query_params)
    graph = rdflib.Graph()

    json_ld = task_instance.xcom_pull(key=instance_uuid, task_ids=task_id).get("graph")
    graph.parse(data=json_ld, format="json-ld")
    logging.info(f"SPARQL:\n{query}")
    return [row for row in graph.query(query)]


def map_to_folio(**kwargs):
    task_instance = kwargs["task_instance"]
    folio_field = kwargs.get("folio_field")
    bf_class = BF_TO_FOLIO_MAP[folio_field].get("class")
    template = BF_TO_FOLIO_MAP[folio_field].get("template")
    uri_type = f"bf_{BF_TO_FOLIO_MAP[folio_field].get('uri')}"

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")
    for instance_uri in resources:
        values = _build_and_query_graph(
            uri_type=uri_type,
            template=template,
            instance_uri=instance_uri,
            bf_class=bf_class,
            **kwargs,
        )
        instance_uuid = instance_uri.split("/")[-1]
        task_instance.xcom_push(key=instance_uuid, value=values)
    return "mapping_complete"
