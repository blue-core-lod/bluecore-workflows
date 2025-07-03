import json
import logging

import httpx

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import get_current_context

from ils_middleware.dags.alma import institutions

logger = logging.getLogger(__name__)


def _parse_institutional_msgs(messages: list) -> list:
    institutional_messages = []
    all_institutions = institutions + ["stanford", "cornell"]
    for message in messages:
        if message["group"].casefold() in all_institutions:
            institutional_messages.append(message)
    return institutional_messages


def _trigger_dags(**kwargs):
    messages = kwargs.get("messages")
    for message in messages:
        logger.info(f"Trigger DAG for {message['group']}")
        # Assumes the DAG name is the same as the group
        group = message["group"].casefold()
        TriggerDagRunOperator(
            task_id=f"{group}-dag-run",
            trigger_dag_id=group,
            conf={"message": message},
        ).execute(kwargs)


@dag(
    start_date=datetime(2024, 1, 15),
    schedule=timedelta(minutes=5),
    catchup=False,
)
def monitor_institutions_messages():
    """
    ### Triggers Institutional DAGs based on incoming API call
    """

    @task
    def incoming_api_call(*args, **kwargs):
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("No parameters found in context")
        return {
            "messages": [
                {
                    "resource": params["resource"],
                    "group": params["group"],
                    "user": params["user"],
                }
            ]
        }

    @task
    def parse_messages(messages: list) -> list:
        return _parse_institutional_msgs(messages)

    @task
    def trigger_institutional_dags(**kwargs):
        _trigger_dags(**kwargs)

    messages = incoming_api_call()
    group_messages = parse_messages(messages)

    trigger_institutional_dags(messages=group_messages)


monitor_export_messages = monitor_institutions_messages()
