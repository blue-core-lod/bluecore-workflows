"""Modules add new or existing inventory instance JSON records to FOLIO the
inventory-storage batch endpoint with upsert query parameter set to true.

See https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/instance-sync.html
"""

import logging

import httpx
from airflow.sdk import Connection
from folioclient import FolioClient

from ils_middleware.tasks.sinopia.email import send_local_id_not_found_email

logger = logging.getLogger(__name__)


def _lookup_by_local_id(
    local_id: str, folio_client: FolioClient, httpx_client
) -> dict | None:
    """Looks up an existing FOLIO instance by an external local identifier.

    Queries FOLIO's own "hrid" field, since that's the property FOLIO exposes
    for this purpose; the value being searched for is our generic local_id,
    which may be an HRID or another institution's local system identifier.
    """
    result = httpx_client.get(
        f"{folio_client.okapi_url}/inventory/instances",
        headers=folio_client.okapi_headers,
        params={"query": f'hrid=="{local_id}"'},
    )
    if result.status_code != 200:
        return None
    instances = result.json().get("instances", [])
    return instances[0] if instances else None


def _check_for_existance(
    records: list, folio_client: FolioClient, local_ids: dict | None = None
) -> tuple:
    new_records, existing_records, unmatched_local_ids = [], [], []
    local_ids = local_ids or {}
    with folio_client.get_folio_http_client() as httpx_client:
        for record in records:
            local_id = local_ids.get(record.get("sourceUri", "").split("/")[-1])
            if local_id:
                existing_record = _lookup_by_local_id(
                    local_id, folio_client, httpx_client
                )
                if existing_record is None:
                    logger.warning(
                        f"No existing FOLIO instance found for local identifier {local_id}; "
                        f"skipping {record.get('sourceUri')}"
                    )
                    unmatched_local_ids.append({"record": record, "local_id": local_id})
                    continue
                record["id"] = existing_record["id"]
            else:
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

    return new_records, existing_records, unmatched_local_ids


def _notify_local_id_not_found(unmatched_local_ids: list, task_instance) -> None:
    for unmatched in unmatched_local_ids:
        instance_uuid = unmatched["record"].get("sourceUri", "").split("/")[-1]
        message = task_instance.xcom_pull(
            key=instance_uuid, task_ids="api-message-parse"
        )
        if isinstance(message, dict):
            send_local_id_not_found_email(message, unmatched["local_id"])


def _push_to_xcom(records: list, task_instance):
    for record in records:
        logger.debug(record)
        uri = record["sourceUri"]
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
    connection = Connection.get(connection_id)

    task_id = "build-folio"

    folio_client = FolioClient(
        connection.host,
        connection.extra_dejson["tenant"],
        connection.login,
        connection.password,
    )

    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"

    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    inventory_records = []
    local_ids = {}
    for instance_uri in resources:
        instance_uuid = instance_uri.split("/")[-1]
        inventory_records.append(
            task_instance.xcom_pull(key=instance_uuid, task_ids=task_id)
        )
        message = task_instance.xcom_pull(
            key=instance_uuid, task_ids="api-message-parse"
        )
        if isinstance(message, dict) and message.get("local_id"):
            local_ids[instance_uuid] = message["local_id"]

    new_records, existing_records, unmatched_local_ids = _check_for_existance(
        inventory_records, folio_client, local_ids=local_ids
    )
    _post_to_okapi(records=new_records, folio_client=folio_client, **kwargs)
    _put_to_okapi(records=existing_records, folio_client=folio_client, **kwargs)
    _notify_local_id_not_found(unmatched_local_ids, task_instance)
