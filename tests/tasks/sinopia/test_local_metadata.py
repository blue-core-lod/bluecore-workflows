"""Test Sinopia LocalAdminMetadata"""

import uuid

import pytest
import rdflib
import requests  # type: ignore

from airflow.models import Variable
from pytest_mock import MockerFixture

from ils_middleware.tasks.sinopia.local_metadata import (
    create_admin_metadata,
    new_local_admin_metadata,
)

from tasks import test_task_instance, mock_task_instance  # noqa: F401


@pytest.fixture
def mock_requests_post(monkeypatch, mocker: MockerFixture):
    mock_request = mocker.stub(name="mock_post")
    mock_request.text = "https://sinopia.io/resource/4546abcd890"
    mock_request.status_code = 201

    def mock_post(*args, **kwargs):
        return mock_request

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def mock_airflow_variables(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0].startswith("sinopia_user"):
            return "ils_middleware"
        if args[0].startswith("sinopia_api_uri"):
            return "https://api.development.sinopia.io/resource"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid4(*args, **kwargs):
        return "1a3cebda-34b9-4e15-bc79-f6a5f915ce76"

    monkeypatch.setattr(uuid, "uuid4", mock_uuid4)


def test_new_local_admin_metadata(
    mock_requests_post,
    mock_airflow_variables,
    mock_uuid,
    mock_task_instance,  # noqa: F811
):
    new_local_admin_metadata(
        task_instance=test_task_instance(),
        ils_tasks={"SIRSI": ["post_new_symphony"]},
        jwt="abcd1234efg",
    )

    assert (
        test_task_instance().xcom_pull(
            key="0000-1111-2222-3333"
        )
        == "https://api.development.sinopia.io/resource/1a3cebda-34b9-4e15-bc79-f6a5f915ce76"
    )

    assert (
        test_task_instance().xcom_pull(
            key="4444-5555-6666-7777"
        )
        == "https://api.development.sinopia.io/resource/1a3cebda-34b9-4e15-bc79-f6a5f915ce76"
    )


def test_create_admin_metadata():
    admin_metadata = rdflib.Graph()
    admin_metadata_str = create_admin_metadata(
        instance_uri="https://api.sinopia.io/resource/12345",
        cataloger_id="ils_middleware",
        ils_identifiers={"SIRSI": "234566"},
    )
    admin_metadata.parse(data=admin_metadata_str, format="json-ld")
    assert len(admin_metadata) == 11
    for row in admin_metadata.query(
        """SELECT ?ident WHERE {
       ?id rdf:type <http://id.loc.gov/ontologies/bibframe/Local> .
       ?id rdf:value ?ident .
    }"""
    ):
        assert str(row[0]) == "234566"
