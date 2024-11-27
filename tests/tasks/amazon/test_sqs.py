"""Test AWS SQS Operators and functions."""

import json

import pytest
import requests  # type: ignore

from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from ils_middleware.tasks.amazon.sqs import (
    SubscribeOperator,
    parse_messages,
    get_resource,
)

from tasks import test_task_instance, mock_message, mock_task_instance  # noqa: F401


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 16)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "sqs_url":
            return "http://aws.com/12345/"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_subscribe_operator_missing_kwargs(test_dag, mock_variable):
    """Test missing kwargs for SubscribeOperator."""

    task = SubscribeOperator(dag=test_dag)
    assert task is not None
    assert task.sqs_queue == "http://aws.com/12345/"
    assert task.aws_conn_id == "aws_sqs_connection"


def test_subscribe_operator(test_dag, mock_variable):
    """Test with typical kwargs for SubscribeOperator."""
    task = SubscribeOperator(queue="stanford-ils", dag=test_dag)
    assert task.sqs_queue.startswith("http://aws.com/12345/stanford-ils")
    assert task.aws_conn_id == "aws_sqs_connection"


def mock_resource(uri):
    return {
        "user": "jpnelson",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "templateId": "ld4p:RT:bf2:Monograph:Instance:Un-nested",
        "types": ["http://id.loc.gov/ontologies/bibframe/Instance"],
        "bfAdminMetadataRefs": [
            "https://api.development.sinopia.io/resource/7f775ec2-4fe8-48a6-9cb4-5b218f9960f1",
            "https://api.development.sinopia.io/resource/bc9e9939-45b3-4122-9b6d-d800c130c576",
        ],
        "bfItemRefs": [],
        "bfInstanceRefs": [],
        "bfWorkRefs": [
            "https://api.development.sinopia.io/resource/6497a461-42dc-42bf-b433-5e47c73f7e89"
        ],
        "id": "7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "uri": uri,
        "timestamp": "2021-10-29T20:30:58.821Z",
    }


@pytest.fixture
def mock_resources():
    return [
        "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
        "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
    ]


@pytest.fixture
def mock_get_resource(monkeypatch):
    resources = []

    def mock_get(uri, *args, **kwargs):
        resources.append(mock_resource(uri))
        response = requests.models.Response()
        response.status_code = 200
        return response

    def mock_json(*args, **kwargs):
        return resources.pop()

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests.models.Response, "json", mock_json)


def test_parse_messages(
    mock_task_instance,  # noqa: F811
    mock_get_resource,
    mock_resources,  # noqa: F811
):
    """Test parse_messages function."""
    task_instance = test_task_instance()
    result = parse_messages(task_instance=task_instance)
    assert result == "completed_parse"

    resource = mock_resources[0]
    uuid = resource.split("/")[-1]
    assert task_instance.xcom_pull(key=uuid).get("resource") == mock_resource(resource)


@pytest.fixture
def mock_task_instance_bad_resource(mocker, mock_resources):
    def mock_xcom_pull(**kwargs):
        if kwargs.get("task_ids", "") == "get-message-from-context":
            return json.loads(mock_message()[2]["Body"])
        return messages[kwargs["key"]]

    def mock_xcom_push(**kwargs):
        messages[kwargs["key"]] = kwargs["value"]

    messages = {}
    mock_test_instance = mocker.MagicMock()
    mock_test_instance.xcom_pull = mock_xcom_pull
    mock_test_instance.xcom_push = mock_xcom_push
    return mock_test_instance


def test_parse_messages_bad_resources(
    mocker,
    mock_get_resource,
    mock_task_instance_bad_resource,
    mock_resources,  # noqa: F811
):

    parse_messages(task_instance=mock_task_instance_bad_resource)

    assert len(mock_task_instance_bad_resource.xcom_pull(key="bad_resources")) == 1


def test_get_resource(mock_get_resource):
    resource = get_resource("http://sinopia.io/resource/456abc")
    assert resource == mock_resource("http://sinopia.io/resource/456abc")


@pytest.fixture
def mock_failed_get_resource(monkeypatch):
    def mock_get(*args, **kwargs):
        response = requests.models.Response()
        response.status_code = 404
        return response

    monkeypatch.setattr(requests, "get", mock_get)


def test_failed_get_resource(mock_failed_get_resource):
    uri = "http://sinopia.io/resource/456abc"
    resource = get_resource(uri)
    assert resource == {}
