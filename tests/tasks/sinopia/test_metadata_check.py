"""Tests metadata checks"""

import datetime

import pytest
import requests  # type: ignore

from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.metadata_check import (
    existing_metadata_check,
    _get_retrieve_metadata_resource,
    _retrieve_all_metadata,
)


def test_task():
    start_date = datetime.datetime(2021, 10, 28)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


task_instance = TaskInstance(test_task())
mock_push_store: dict = {}

admin_metadata = [
    {
        "@id": "https://api.sinopia.io/resource/1234abcde",
        "http://id.loc.gov/ontologies/bibframe/identifier": [{"@id": "_:b1"}],
        "http://sinopia.io/vocabulary/exportDate": [
            {"@value": "2021-10-28T22:19:06.176422"}
        ],
    },
    {
        "@id": "_:b1",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#value": ["13704749"],
        "http://id.loc.gov/ontologies/bibframe/source": [{"@id": "_:b2"}],
    },
    {
        "@id": "_:b2",
        "http://www.w3.org/2000/01/rdf-schema#label": [{"@value": "SIRSI"}],
    },
]

sinopia_api = {
    # Happy Path
    "http://example.com/rdf/0000-1111-2222-3333/relationships": {
        "sinopiaHasLocalAdminMetadataInferredRefs": [
            "https://api.sinopia.io/resource/1234abcde"
        ]
    },
    "http://example.com/rdf/4444-5555-6666-7777/relationships": {
        "sinopiaHasLocalAdminMetadataInferredRefs": [
            "https://api.sinopia.io/resource/1234abcde"
        ]
    },
    "https://api.sinopia.io/resource/1234abcde": {
        "templateId": "pcc:sinopia:localAdminMetadata",
        "data": admin_metadata,
    },
    # Not a localAdmin record
    "https://api.sinopia.io/resource/ku333aa555/relationships": {
        "sinopiaHasLocalAdminMetadataInferredRefs": [
            "https://api.sinopia.io/resource/753878c"
        ]
    },
    "https://api.sinopia.io/resource/753878c": {"templateId": "pcc:bf2:AdminMetadata"},
    # No bfAdminMetadata
    "https://api.sinopia.io/resource/oprt5531/relationships": {
        "sinopiaHasLocalAdminMetadataInferredRefs": []
    },
    # Missing AdminMetadata URI
    "https://s.io/11ec/relationships": {
        "sinopiaHasLocalAdminMetadataInferredRefs": ["https://s.io/3818"]
    },
}


@pytest.fixture
def mock_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        new_result = mocker.stub(name="get")
        if args[0] in sinopia_api:
            new_result.status_code = 200
            new_result.json = lambda: sinopia_api.get(args[0])
        else:
            new_result.status_code = 401
            new_result.text = "Not Found"
        return new_result

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def mock_datetime(monkeypatch):
    datetime_mock = MagicMock(wrap=datetime.datetime)
    datetime_mock.fromisoformat.return_value = datetime.datetime(2021, 10, 28, 22, 29)

    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key == "resources":
            return [
                "http://example.com/rdf/0000-1111-2222-3333",
                "http://example.com/rdf/4444-5555-6666-7777",
                "https://api.sinopia.io/resource/oprt5531",
            ]
        else:
            return mock_push_store[key]

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = value
        return None

    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)
    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_check_one_metadata_record(mock_requests, mock_datetime, mock_task_instance):
    existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/gh1234",
        ils_tasks={"overlay": "post_ils_overlay"},
    )
    overlay_resources = task_instance.xcom_pull(key="overlay_resources")
    assert len(overlay_resources) == 2


def test_no_admin_metadata_records(mock_requests, mock_datetime, mock_task_instance):
    existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/oprt5531",
        ils_tasks={"new": "post_ils_new"},
    )
    new_resources = task_instance.xcom_pull(key="new_resources")
    assert len(new_resources) == 1


def test_no_local_metadata_records(mock_requests, mock_datetime, mock_task_instance):
    existing_metadata_check(
        task_instance=task_instance,
        resource_uri="https://api.sinopia.io/resource/ku333aa555",
        ils_tasks={"new": "post_ils_new"},
    )
    new_resources = task_instance.xcom_pull(key="new_resources")
    assert len(new_resources) == 1


def test_resource_not_found(mock_requests):
    result = _get_retrieve_metadata_resource("http://sinopia.io/bad")

    assert result is None


def test_not_local_admin_templatet(mock_requests):
    result = _get_retrieve_metadata_resource("https://api.sinopia.io/resource/753878c")

    assert result is None


def test_dups_in_retrieve_all_metadata(mock_requests):
    result = _retrieve_all_metadata(
        [
            "https://api.sinopia.io/resource/1234abcde",
            "https://api.sinopia.io/resource/1234abcde",
        ]
    )

    assert result == [{"SIRSI": "13704749"}]
