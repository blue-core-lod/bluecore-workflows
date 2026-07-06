"""Maps Sinopia RDF to FOLIO Records."""

import datetime
import logging

import rdflib

import ils_middleware.tasks.folio.mappings.bf_instance as bf_instance_map
import ils_middleware.tasks.folio.mappings.bf_work as bf_work_map

logger = logging.getLogger(__name__)

# Order matters: fields that accumulate into the same FOLIO array (e.g. subjects/genre,
# identifiers, editions) must be ordered so the first writer populates the list before
# subsequent entries append to it via record.get(..., []).
BF_TO_FOLIO_MAP = {
    "cataloged_date": {
        "template": bf_instance_map.cataloged_date,
        "uri": "instance",
    },
    "classifications.ddc": {
        "template": bf_work_map.classification,
        "uri": "work",
        "class": "bf:ClassificationDdc",
    },
    "classifications.lcc": {
        "template": bf_work_map.classification,
        "uri": "work",
        "class": "bf:ClassificationLcc",
    },
    "classifications.nlm": {
        "template": bf_work_map.classification,
        "uri": "work",
        "class": "bf:ClassificationNlm",
    },
    "alternative_titles.abbreviated": {
        "template": bf_instance_map.alternative_title,
        "uri": "instance",
        "class": "bf:AbbreviatedTitle",
    },
    "alternative_titles.parallel": {
        "template": bf_instance_map.alternative_title,
        "uri": "instance",
        "class": "bf:ParallelTitle",
    },
    "alternative_titles.variant": {
        "template": bf_instance_map.alternative_title,
        "uri": "instance",
        "class": "bf:VariantTitle",
    },
    "alternative_titles.abbreviated.work": {
        "template": bf_work_map.alternative_title,
        "uri": "work",
        "class": "bf:AbbreviatedTitle",
    },
    "alternative_titles.parallel.work": {
        "template": bf_work_map.alternative_title,
        "uri": "work",
        "class": "bf:ParallelTitle",
    },
    "alternative_titles.variant.work": {
        "template": bf_work_map.alternative_title,
        "uri": "work",
        "class": "bf:VariantTitle",
    },
    "contributor.Person": {
        "template": bf_work_map.contributor,
        "uri": "work",
        "class": "bf:Person",
    },
    "contributor.primary.Person": {
        "template": bf_work_map.primary_contributor,
        "uri": "work",
        "class": "bf:Person",
    },
    "editions": {"template": bf_instance_map.editions, "uri": "instance"},
    "electronic_access": {
        "template": bf_instance_map.electronic_locator,
        "uri": "instance",
    },
    "editions.work": {"template": bf_work_map.editions, "uri": "work"},
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
    "identifiers.local": {
        "template": bf_instance_map.local_identifier,
        "uri": "instance",
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
    "publication_frequency": {
        "template": bf_instance_map.publication_frequency,
        "uri": "instance",
    },
    "publication_range": {
        "template": bf_instance_map.publication_range,
        "uri": "instance",
    },
    "series.controlled": {
        "template": bf_work_map.series_controlled,
        "uri": "work",
    },
    "series.uncontrolled": {
        "template": bf_work_map.series_uncontrolled,
        "uri": "work",
    },
    "subjects": {"template": bf_work_map.subject, "uri": "work"},
    "genre": {"template": bf_work_map.genre, "uri": "work"},
    "nature_of_content": {"template": bf_work_map.genre, "uri": "work"},
    "title": {
        "template": bf_instance_map.title,
        "uri": "instance",
        "class": "bf:Title",
    },
}

# Fields mapped to the FOLIO holdings record instead of the instance record.
BF_TO_FOLIO_HOLDINGS_MAP = {
    "call_number.shelf_mark": {
        "template": bf_work_map.call_number,
        "uri": "work",
        "class": "bf:shelfMark",
    },
    "call_number.ddc": {
        "template": bf_work_map.call_number,
        "uri": "work",
        "class": "bf:shelfMarkDdc",
    },
    "call_number.udc": {
        "template": bf_work_map.call_number,
        "uri": "work",
        "class": "bf:shelfMarkUdc",
    },
    "call_number.lcc": {
        "template": bf_work_map.call_number,
        "uri": "work",
        "class": "bf:shelfMarkLcc",
    },
    "call_number.nlm": {
        "template": bf_work_map.call_number,
        "uri": "work",
        "class": "bf:shelfMarkNlm",
    },
}

FOLIO_FIELDS = BF_TO_FOLIO_MAP.keys()
HOLDINGS_FOLIO_FIELDS = BF_TO_FOLIO_HOLDINGS_MAP.keys()
ALL_FOLIO_FIELDS = list(FOLIO_FIELDS) + list(HOLDINGS_FOLIO_FIELDS)


def _task_id(task_groups: str) -> str:
    task_id = "bf-graph"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"
    return task_id


def _retrieve_values(query_row: list | tuple) -> list:
    output = []
    for field in query_row:
        if field:
            if isinstance(field, rdflib.URIRef):
                output.append(str(field))
                continue
            value = field.value
            if isinstance(value, datetime.datetime):
                value = value.date().isoformat()
            elif isinstance(value, datetime.date):
                value = value.isoformat()
            output.append(value)
        else:
            output.append(None)  # type: ignore
    return output


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
    values = []
    for row in graph.query(query):
        values.append(_retrieve_values(row))  # type: ignore
    return values


def map_to_folio(**kwargs):
    task_instance = kwargs["task_instance"]
    folio_field = kwargs.get("folio_field")
    field_map = (
        BF_TO_FOLIO_MAP.get(folio_field) or BF_TO_FOLIO_HOLDINGS_MAP[folio_field]
    )
    bf_class = field_map.get("class")
    template = field_map.get("template")
    uri_type = f"bf_{field_map.get('uri')}"

    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")
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
