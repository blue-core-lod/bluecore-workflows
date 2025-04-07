"""Converts PYMARC JSON to Symphony JSON"""

import json
import logging

logger = logging.getLogger(__name__)


def _get_subfields(subfields: dict) -> list:
    output = []
    for subfield, value in subfields.items():
        output.append({"code": subfield, "data": value})
    return output


def _get_variable_field(value, new_field):
    if "ind1" in value:
        new_field["inds"] = "".join([value["ind1"], value["ind2"]])
    new_field["subfields"] = []
    for row in value["subfields"]:
        new_field["subfields"].extend(_get_subfields(row))
    return new_field


def _get_fields(field):
    new_field = {}
    for tag, value in field.items():
        new_field["tag"] = tag
        if isinstance(value, str):
            new_field["subfields"] = [{"code": "_", "data": value}]
        else:
            new_field = _get_variable_field(value, new_field)
    return new_field


def to_symphony_json(**kwargs):
    """Converst pymarc MARC json to Symphony JSON varient."""
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_uuid = instance_uri.split("/")[-1]
        marc_raw_json = task_instance.xcom_pull(
            key=instance_uuid, task_ids="process_symphony.marc_json_to_s3"
        )
        pymarc_json = json.loads(marc_raw_json)
        record = {"standard": "MARC21", "type": "BIB", "fields": []}
        record["leader"] = pymarc_json.get("leader")
        for field in pymarc_json["fields"]:
            record["fields"].append(_get_fields(field))

        task_instance.xcom_push(key=instance_uuid, value=record)
