"""Modules add new or existing inventory instance JSON records to FOLIO the
inventory-storage batch endpoint with upsert query parameter set to true.

See https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/instance-sync.html
"""

import logging

import httpx

from airflow.models.connection import Connection
from folioclient import FolioClient

logger = logging.getLogger(__name__)


def _check_for_existance(records: list, folio_client: FolioClient) -> tuple:
    new_records, existing_records = [], []
    with folio_client.get_folio_http_client() as httpx_client:
        for record in records:
            existing_result = httpx_client.get(
                f"{folio_client.okapi_url}/inventory/instances/{record['id']}",
                headers=folio_client.okapi_headers,
            )
            if existing_result.status_code == 404:
                new_records.append(record)
                continue
            existing_record = existing_result.json()
            record["hrid"] = existing_record["hrid"]
            record["_version"] = existing_record["_version"]
            existing_records.append(record)

    return new_records, existing_records


def _push_to_xcom(records: list, task_instance):
    for record in records:
        logger.debug(record)
        uri = record["electronicAccess"][0]["uri"]
        uuid = uri.split("/")[-1]
        task_instance.xcom_push(key=uuid, value=record["id"])


def _post_to_okapi(**kwargs):
    task_instance = kwargs["task_instance"]
    records = kwargs["records"]
    folio_client = kwargs["folio_client"]
    endpoint = kwargs.get("endpoint", "/inventory/instances")

    for record in records:
        try:
            result = folio_client.folio_post(endpoint, payload=record)
            logger.info(f"Record result {result}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Error POST record {record['id']} {e}")

    _push_to_xcom(records, task_instance)


def _put_to_okapi(**kwargs):
    task_instance = kwargs["task_instance"]
    records = kwargs["records"]
    folio_client = kwargs["folio_client"]
    for record in records:
        try:
            folio_client.folio_put(
                f"/inventory/instances/{record['id']}", payload=record
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"Error PUT record {record} {e}")
    _push_to_xcom(records, task_instance)


def post_folio_records(**kwargs):
    """Creates new records in FOLIO"""
    task_instance = kwargs["task_instance"]
    connection_id = kwargs["folio_connection_id"]

    task_groups = ".".join(kwargs["task_groups_ids"])
    connection = Connection.get_connection_from_secrets(connection_id)

    task_id = "build-folio"

    folio_client = FolioClient(
        connection.host,
        connection.extra_dejson["tenant"],
        connection.login,
        connection.password,
    )

    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    inventory_records = []
    for instance_uri in resources:
        instance_uuid = instance_uri.split("/")[-1]
        inventory_records.append(
            task_instance.xcom_pull(key=instance_uuid, task_ids=task_id)
        )

    new_records, existing_records = _check_for_existance(
        inventory_records, folio_client
    )
    _post_to_okapi(records=new_records, folio_client=folio_client, **kwargs)
    _put_to_okapi(records=existing_records, folio_client=folio_client, **kwargs)
