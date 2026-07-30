import logging
from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task

from ils_middleware.dags.alma import institutions
from ils_middleware.tasks.keycloak import get_user_profile

logger = logging.getLogger(__name__)


def _check_available_institutions(group: str) -> bool:
    """
    Checks if group available for export
    """
    all_institutions = institutions + ["stanford", "cornell"]
    return group.casefold() in all_institutions


def _build_payload(params: dict) -> dict:
    """
    Builds the DAG trigger payload from incoming API call params. local_id is
    always present and is None unless the export is overlaying an existing
    catalog record by local identifier (e.g. an HRID or other institution's
    local system identifier).
    """
    return {
        "resource": params["resource"],
        "local_id": params.get("local_id"),
        "user": params["user"],
    }


def _trigger_dags(**kwargs):
    payload: dict = kwargs["payload"]

    for group in payload["user"]["groups"]:
        if _check_available_institutions(group):
            logger.info(f"Trigger DAG for {group}")
            # Assumes the DAG name is the same as the group
            TriggerDagRunOperator(
                task_id=f"{group}-dag-run",
                trigger_dag_id=group,
                conf={"message": payload},
            ).execute(kwargs)


@dag(
    start_date=datetime(2024, 1, 15),
    catchup=False,
)
def monitor_institutions_exports():
    """
    ### Triggers Institutional DAGs based on incoming API call
    """

    @task
    def incoming_api_call():
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("No parameters found in context")
        return _build_payload(params)

    @task
    def retrieve_user(payload: dict) -> dict:
        user_uid = payload.get("user", "")
        payload["user"] = get_user_profile(user_uid)
        return payload

    @task
    def trigger_institutional_dags(**kwargs):
        _trigger_dags(**kwargs)

    payload = incoming_api_call()
    payload_with_user = retrieve_user(payload)

    trigger_institutional_dags(payload=payload_with_user)


monitor_export_api_messages = monitor_institutions_exports()
