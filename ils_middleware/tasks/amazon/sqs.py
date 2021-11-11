"""Custom Operator using AWS SQSSensor."""
import logging
import json

import requests  # type: ignore

from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor

logger = logging.getLogger(__name__)


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> SQSSensor:
    """Subscribe to a topic to filter SQS messages."""
    queue_name = kwargs.get("queue", "")
    aws_sqs_url = Variable.get("sqs_url")
    return SQSSensor(
        aws_conn_id="aws_sqs_connection",
        sqs_queue=f"{aws_sqs_url}{queue_name}",
        task_id="sqs-sensor",
        dag=kwargs.get("dag"),
        max_messages=1,
    )


def get_resource(resource_uri: str) -> dict:
    """Retrieves the Resource"""
    result = requests.get(resource_uri)
    if result.status_code < 400:
        return result.json()
    return {}


def parse_messages(**kwargs) -> str:
    """Parses SQS Message Body into constituent part."""
    task_instance = kwargs["task_instance"]
    raw_sqs_message = task_instance.xcom_pull(key="messages", task_ids=["sqs-sensor"])[
        0
    ]
    message_body = json.loads(raw_sqs_message[0].get("Body"))
    resource_uri = message_body["resource"]["uri"]
    task_instance.xcom_push(key="email", value=message_body["user"]["email"])
    task_instance.xcom_push(key="resource_uri", value=resource_uri)
    task_instance.xcom_push(key="resource", value=get_resource(resource_uri))
    return "completed_parse"
