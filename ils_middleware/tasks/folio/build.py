"""Module builds FOLIO Inventory Instance records based on values extracted from upstream tasks
SPARQL queries run on BF Instance and Work RDF graphs from Sinopia."""

import datetime
import logging

from airflow.models.connection import Connection

from folioclient import FolioClient
from folio_uuid import FOLIONamespaces, FolioUUID

from ils_middleware.tasks.folio.map import FOLIO_FIELDS

logger = logging.getLogger(__name__)


def _default_transform(**kwargs) -> tuple:
    folio_field = kwargs["folio_field"]
    values = kwargs.get("values", [])
    logger.debug(f"field: {folio_field} values: {values} type: {type(values)}")
    return folio_field, values


def _contributors(**kwargs) -> tuple:
    folio_client = kwargs["folio_client"]
    values = kwargs["values"]
    record = kwargs["record"]
    is_primary = kwargs.get("primary", False)
    contrib_name_type = kwargs.get("contrib_name", "Personal name")

    lookup_contrib_id = {}
    for row in folio_client.contributor_types:
        lookup_contrib_id[row["name"].casefold()] = row["id"]

    lookup_contrib_name_id = {}
    for row in folio_client.contrib_name_types:
        lookup_contrib_name_id[row["name"].casefold()] = row["id"]

    contributors = record.get("contributors", [])
    for row in values:
        contributor = {
            "contributorNameTypeId": lookup_contrib_name_id[
                contrib_name_type.casefold()
            ],
            "contributorTypeId": lookup_contrib_id[row[1].casefold()],
            "contributorTypeText": row[1],
            "name": row[0],
            "primary": is_primary,
        }
        contributors.append(contributor)

    return "contributors", contributors


def _primary_contributor(**kwargs) -> tuple:
    return _contributors(primary=True, contrib_name="Personal name", **kwargs)


def _folio_id(resource_uri: str, okapi_url: str) -> str:
    folio_id = FolioUUID(okapi_url, FOLIONamespaces.instances, resource_uri)
    return str(folio_id)


def _electronic_access(**kwargs) -> dict:
    folio_client = kwargs["folio_client"]
    sinopia_url = kwargs["instance_uri"]

    output = {}
    relationship_id = None
    for row in folio_client.folio_get(
        "/electronic-access-relationships", key="electronicAccessRelationships"
    ):
        if row["name"] == "Resource":
            relationship_id = row["id"]
            break

    if relationship_id is not None:
        output = {"uri": sinopia_url, "relationshipId": relationship_id}

    return output


def _identifiers(**kwargs) -> tuple:
    folio_client = kwargs["folio_client"]
    folio_field = kwargs["folio_field"]
    identifier_name = None
    if folio_field.endswith("isbn"):
        identifier_name = "ISBN"
    if folio_field.endswith("oclc"):
        identifier_name = "OCLC"
    if folio_field.endswith("lccn"):
        identifier_name = "LCCN"
    if folio_field.endswith("doi"):
        identifier_name = "DOI"
    if folio_field.endswith("issn"):
        identifier_name = "ISSN"

    values = kwargs["values"]

    lookup_ident_ids = {}
    for row in folio_client.identifier_types:
        lookup_ident_ids[row["name"]] = row["id"]

    identifiers = kwargs["record"].get("identifiers", [])
    for row in values:
        identifiers.append(
            {"identifierTypeId": lookup_ident_ids[identifier_name], "value": row[0]}
        )

    return "identifiers", identifiers


def _instance_format_ids(**kwargs) -> tuple:
    folio_client = kwargs["folio_client"]
    values = kwargs["values"]
    format_ids = []
    lookup_id = {}
    for row in folio_client.instance_formats:
        lookup_id[row["name"]] = row["id"]

    for row in values:
        name = f"{row[0]} -- {row[1]}"
        uuid = lookup_id.get(name)
        if uuid:
            format_ids.append(uuid)

    return "instanceFormatIds", format_ids


def _instance_type_id(**kwargs) -> tuple:
    folio_client = kwargs["folio_client"]
    values = kwargs["values"]

    # Only use first value and lowercase
    name = values[0][0].lower()

    ident = None

    for row in folio_client.instance_types:
        if row["name"] == name:
            ident = row["id"]
            break

    if ident is None:
        raise ValueError(f"instanceTypeId for {name} not found")
    return "instanceTypeId", ident


def _language(**kwargs) -> tuple:
    values = kwargs["values"]

    language_codes = []
    for row in values:
        code = row[0].split("/")[-1]
        language_codes.append(code)

    return "languages", language_codes


def _mode_of_issuance_id(**kwargs) -> tuple:
    folio_client = kwargs["folio_client"]
    values = kwargs["values"]

    mode_id = None
    name = values[0][0]

    for row in folio_client.modes_of_issuance:
        if row["name"] == name:
            mode_id = row["id"]
            break

    return "modeOfIssuanceId", mode_id


def _notes(**kwargs) -> tuple:
    values = kwargs["values"]
    folio_client = kwargs["folio_client"]
    note_id = None
    # For now assign every note as a FOLIO "General note"
    for row in folio_client.instance_note_types:
        if row["name"].startswith("General note"):
            note_id = row["id"]
            break
    notes = []
    for row in values:
        notes.append({"instanceNoteId": note_id, "note": row[0], "staffOnly": False})

    return "notes", notes


def _physical_descriptions(**kwargs) -> tuple:
    values = kwargs["values"]
    output = []

    for row in values:
        desc = row[0]  # Extent
        if row[1]:  # Diminisons
            desc = f"{desc}, {row[1]}"
        output.append(desc)

    return "physicalDescriptions", output


def _publication(**kwargs) -> tuple:
    values = kwargs["values"]
    publications = []
    for row in values:
        publication = {"role": "Publication"}
        if row[0]:  # Publisher Name
            publication["publisher"] = row[0]
        if row[1]:
            publication["dateOfPublication"] = row[1]
        if row[2]:
            publication["place"] = row[2]
        publications.append(publication)
    return "publication", publications


def _subjects(**kwargs) -> tuple:
    values = kwargs["values"]
    subjects = []
    for row in values:
        subjects.append(row[0])

    return "subjects", subjects


def _title(**kwargs) -> tuple:
    values = kwargs["values"]

    for row in values:
        title = row[0]
        if row[1]:  # subtitle
            title = f"{title} : {row[1]}"
        if row[2]:  # partNumber"
            title = f"{title}. {row[2]}"
        if row[3]:  # partName
            title = f"{title}, {row[3]}"
    return "title", title


def _user_folio_id(okapi_url: str, folio_user: str) -> str:
    folio_uuid = FolioUUID(okapi_url, FOLIONamespaces.users, folio_user)
    return str(folio_uuid)


transforms = {
    "identifiers.isbn": _identifiers,
    "identifiers.oclc": _identifiers,
    "identifiers.doi": _identifiers,
    "identifiers.lccn": _identifiers,
    "instance_format": _instance_format_ids,
    "instance_type": _instance_type_id,
    "language": _language,
    "modeOfIssuanceId": _mode_of_issuance_id,
    "notes": _notes,
    "physical_description": _physical_descriptions,
    "contributor.primary.Person": _primary_contributor,
    "publication": _publication,
    "subjects": _subjects,
    "title": _title,
}


def _create_update_metadata(**kwargs) -> dict:
    folio_client = kwargs["folio_client"]

    current_timestamp = datetime.datetime.utcnow().isoformat()
    user_uuid = _user_folio_id(folio_client.okapi_url, folio_client.username)
    metadata = kwargs.get("metadata", {})
    if len(metadata) < 1:
        metadata = {
            "createdDate": current_timestamp,
            "createdByUserId": user_uuid,
        }
    else:
        metadata["updatedDate"] = current_timestamp
        metadata["updatedByUserId"] = user_uuid
    return metadata


def _task_ids(task_groups: str, folio_field: str) -> str:
    task_id = f"{folio_field}_task"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"
    return task_id


def _inventory_record(**kwargs) -> dict:
    instance_uri = kwargs["instance_uri"]
    task_instance = kwargs["task_instance"]
    task_groups = ".".join(kwargs["task_groups_ids"])
    folio_client = kwargs["folio_client"]

    record = {
        "id": _folio_id(instance_uri, folio_client.okapi_url),
        "metadata": _create_update_metadata(**kwargs),
        "source": "SINOPIA",
        "electronicAccess": [_electronic_access(**kwargs)],
    }
    instance_uuid = instance_uri.split("/")[-1]

    for folio_field in FOLIO_FIELDS:
        post_processing = transforms.get(folio_field, _default_transform)
        task_id = _task_ids(task_groups, folio_field)
        raw_values = task_instance.xcom_pull(key=instance_uuid, task_ids=task_id)
        if raw_values:
            record_field, values = post_processing(
                values=raw_values,
                okapi_url=folio_client.okapi_url,
                folio_field=folio_field,
                folio_user=folio_client.username,
                folio_client=folio_client,
                record=record,
            )

            record[record_field] = values
        logger.debug(f"{raw_values} values for {instance_uri}'s {task_id}")
    return record


def build_records(**kwargs):
    """ """
    task_instance = kwargs["task_instance"]
    connection_id = kwargs["folio_connection_id"]

    connection = Connection.get_connection_from_secrets(connection_id)

    folio_client = FolioClient(
        connection.host,
        connection.extra_dejson["tenant"],
        connection.login,
        connection.password,
    )

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for resource_uri in resources:
        inventory_rec = _inventory_record(
            instance_uri=resource_uri,
            folio_client=folio_client,
            **kwargs,
        )
        resource_uuid = resource_uri.split("/")[-1]
        task_instance.xcom_push(key=resource_uuid, value=inventory_rec)
    return "build-complete"
