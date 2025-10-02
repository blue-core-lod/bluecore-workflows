import logging

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import get_current_context

from ils_middleware.dags.alma import institutions

logger = logging.getLogger(__name__)


def _check_available_institutions(payload: dict) -> dict:
    """
    Parses incoming payload to check if group exists
    """
    all_institutions = institutions + ["stanford", "cornell"]
    if payload["group"].casefold() not in all_institutions:
        raise ValueError(f"{payload['group']} isn't available for export")
    return payload


def _trigger_dags(**kwargs):
    payload: dict = kwargs["payload"]
    logger.info(f"Trigger DAG for {payload['group']}")
    # Assumes the DAG name is the same as the group
    group = payload["group"].casefold()
    TriggerDagRunOperator(
        task_id=f"{group}-dag-run",
        trigger_dag_id=group,
        conf={"message": payload},
    ).execute(kwargs)


@dag(
    start_date=datetime(2024, 1, 15),
    schedule=timedelta(minutes=5),
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
            "group": params["group"],
            "user": params["user"],
        }

    @task
    def parse_messages(messages: dict) -> dict:
        return _check_available_institutions(messages)

    @task
    def trigger_institutional_dags(**kwargs):
        _trigger_dags(**kwargs)

    payload = incoming_api_call()
    checked_messages = parse_messages(payload)

    trigger_institutional_dags(payload=checked_messages)


monitor_export_api_messages = monitor_institutions_exports()
