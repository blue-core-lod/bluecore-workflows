"""Custom Operator using AWS SQSSensor."""

import logging

import requests  # type: ignore

from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

logger = logging.getLogger(__name__)


# Should return aws_sqs_sensor operator
# https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/contrib/sensors/aws_sqs_sensor/index.html
def SubscribeOperator(**kwargs) -> SqsSensor:
    """Subscribe to a topic to filter SQS messages."""
    queue_name = kwargs.get("queue", "")
    aws_sqs_url = Variable.get("sqs_url")
    return SqsSensor(
        aws_conn_id="aws_sqs_connection",
        sqs_queue=f"{aws_sqs_url}{queue_name}",
        task_id="sqs-sensor",
        dag=kwargs.get("dag"),
        max_messages=10,
        timeout=240,
    )


def get_resource(resource_uri: str) -> dict:
    """Retrieves the Resource"""
    result = requests.get(resource_uri)
    if result.status_code < 400:
        return result.json()
    return {}


def parse_messages(**kwargs) -> str:
    """Parses and checks for existing resource in message."""
    task_instance = kwargs["task_instance"]
    message = task_instance.xcom_pull(task_ids="get-message-from-context")

    resources = []
    resources_with_errors = []

    resource_uri = message["resource"]["uri"]
    resource_uuid = resource_uri.split("/")[-1]
    try:
        task_instance.xcom_push(
            key=resource_uuid,
            value={
                "email": message["user"]["email"],
                "group": message["group"],
                "target": message["target"],
                "resource_uri": resource_uri,
                "resource": get_resource(resource_uri),
            },
        )
        resources.append(resource_uri)
    except KeyError:
        resources_with_errors.append(resource_uri)

    task_instance.xcom_push(key="resources", value=resources)
    task_instance.xcom_push(key="bad_resources", value=resources_with_errors)
    return "completed_parse"
