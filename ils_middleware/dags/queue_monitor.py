import json
import logging

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from ils_middleware.dags.alma import institutions
from ils_middleware.tasks.amazon.sqs import SubscribeOperator

logger = logging.getLogger(__name__)


def _parse_institutional_msgs(task_instance) -> list:
    messages = task_instance.xcom_pull(task_ids="sqs-sensor", key="messages")
    institutional_messages = []
    all_institutions = institutions + ["stanford", "cornell"]
    for row in messages:
        message = json.loads(row["Body"])
        if message["group"] in all_institutions:
            institutional_messages.append(message)
    return institutional_messages


def _trigger_dags(**kwargs):
    messages = kwargs.get("messages")
    for message in messages:
        logger.info(f"Trigger DAG for {message['group']}")
        # Assumes the DAG name is the same as the group
        TriggerDagRunOperator(
            task_id=f"{message['group']}-dag-run",
            trigger_dag_id=f"{message['group']}",
            conf={"message": message},
        ).execute(kwargs)


@dag(
    start_date=datetime(2024, 1, 15),
    schedule=timedelta(minutes=5),
    catchup=False,
)
def monitor_institutions_messages():
    """
    ### Monitors SQS Queue and Triggers Institutional DAGs
    """

    listen_sqs = SubscribeOperator(queue="all-institutions")

    @task
    def parse_messages(**kwargs) -> list:
        ti = kwargs["ti"]
        return _parse_institutional_msgs(ti)

    @task
    def trigger_institutional_dags(**kwargs):
        _trigger_dags(**kwargs)

    group_messages = parse_messages()

    listen_sqs >> group_messages

    trigger_institutional_dags(messages=group_messages)


monitor_respond_messages = monitor_institutions_messages()
