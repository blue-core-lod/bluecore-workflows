"""Modules add new or existing inventory instance JSON records to FOLIO the
inventory-storage batch endpoint with upsert query parameter set to true.

See https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/instance-sync.html
"""

import logging

import httpx
from airflow.sdk import Connection
from folioclient import FolioClient

from ils_middleware.tasks.folio.inventory import lookup_by_local_id
from ils_middleware.tasks.sinopia.email import send_local_id_not_found_email

logger = logging.getLogger(__name__)


def _check_for_existance(
    records: list, folio_client: FolioClient, local_ids: dict | None = None
) -> tuple:
    new_records, existing_records, unmatched_local_ids = [], [], []
    local_ids = local_ids or {}
    with folio_client.get_folio_http_client() as httpx_client:
        for record in records:
            local_id = local_ids.get(record.get("sourceUri", "").split("/")[-1])
            if local_id:
                existing_record = lookup_by_local_id(local_id, folio_client)
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


def _push_to_xcom(records: list, task_instance) -> list:
    """Pushes the FOLIO id and HRID of each record, keyed by instance uuid,
    and returns the uuids"""
    uuids = []
    for record in records:
        logger.debug(record)
        uri = record["sourceUri"]
        uuid = uri.split("/")[-1]
        task_instance.xcom_push(key=uuid, value=record["id"])
        task_instance.xcom_push(
            key=f"{uuid}-export",
            value={"folio_id": record["id"], "hrid": record.get("hrid")},
        )
        uuids.append(uuid)
    return uuids


def _lookup_hrid(record_id: str, folio_client: FolioClient) -> str | None:
    """Retrieves the HRID FOLIO assigned to a new instance"""
    try:
        instance = folio_client.folio_get(f"/inventory/instances/{record_id}")
    except httpx.HTTPError as e:
        logger.warning(f"Error retrieving HRID for {record_id} {e}")
        return None
    if isinstance(instance, dict):
        return instance.get("hrid")
    return None


def _post_to_okapi(**kwargs) -> list:
    task_instance = kwargs["task_instance"]
    records = kwargs["records"]
    folio_client = kwargs["folio_client"]
    endpoint = kwargs.get("endpoint", "/inventory/instances")

    posted = []
    for record in records:
        try:
            result = folio_client.folio_post(endpoint, payload=record)
            logger.info(f"Record result {result}")
        except httpx.HTTPStatusError as e:
            logger.error(f"Error POST record {record['id']} {e}")
            continue
        hrid = result.get("hrid") if isinstance(result, dict) else None
        record["hrid"] = hrid or _lookup_hrid(record["id"], folio_client)
        if record["hrid"] is None:
            logger.warning(f"No HRID found for new record {record['id']}")
        posted.append(record)

    return _push_to_xcom(posted, task_instance)


def _put_to_okapi(**kwargs) -> list:
    task_instance = kwargs["task_instance"]
    records = kwargs["records"]
    folio_client = kwargs["folio_client"]
    updated = []
    for record in records:
        try:
            folio_client.folio_put(
                f"/inventory/instances/{record['id']}", payload=record
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"Error PUT record {record} {e}")
            continue
        updated.append(record)
    return _push_to_xcom(updated, task_instance)


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
    exported = _post_to_okapi(records=new_records, folio_client=folio_client, **kwargs)
    exported += _put_to_okapi(
        records=existing_records, folio_client=folio_client, **kwargs
    )
    task_instance.xcom_push(key="exported", value=exported)
    _notify_local_id_not_found(unmatched_local_ids, task_instance)
