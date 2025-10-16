import logging

from datetime import datetime
from typing import Union

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import get_current_context

from ils_middleware.dags.alma import institutions
from ils_middleware.tasks.keycloak import get_user_groups

logger = logging.getLogger(__name__)


def _check_available_institutions(group: str) -> bool:
    """
    Checks if group available for export
    """
    all_institutions = institutions + ["stanford", "cornell"]
    return group.casefold() in all_institutions


def _get_user_group(user_id: str) -> str:
    groups = get_user_groups(user_id)

    error: Union[str, None] = None

    match len(groups):
        case 0:
            error = f"{user_id} not in any groups"

        case 1:
            error = None

        case _:
            error = f"{user_id} can only be in one group for export"

    if error:
        raise ValueError(error)

    group = groups[0]
    if not _check_available_institutions(group):
        raise ValueError(f"Group {group} not available for export")

    return group


def _trigger_dags(**kwargs):
    payload: dict = kwargs["payload"]
    group: str = payload["group"]

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
        return {
            "resource": params["resource"],
            "user": params["user"],
        }

    @task
    def retrieve_user_group(payload: dict) -> dict:
        username = payload.get("user", "")
        group = _get_user_group(username)
        payload["group"] = group
        return payload

    @task
    def trigger_institutional_dags(**kwargs):
        _trigger_dags(**kwargs)

    payload = incoming_api_call()
    payload_with_user_group = retrieve_user_group(payload)

    trigger_institutional_dags(payload=payload_with_user_group)


monitor_export_api_messages = monitor_institutions_exports()
