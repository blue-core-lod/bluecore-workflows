import json

import pytest
from airflow.models.taskinstance import TaskInstance
from pytest_mock import MockerFixture
from tasks import mock_task_instance, test_task_instance  # noqa: F401

from ils_middleware.tasks.folio.graph import _build_graph, construct_graph

mock_instance_doc = {
    "https://api.stage.sinopia.io/resource/8a2dda53-d3bc-485a-9154-635823045b4f": {
        "user": "kbeckett@stanford.edu",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "data": [],
        "id": "8a2dda53-d3bc-485a-9154-635823045b4f",
        "bfWorkRefs": [],
        "templateId": "ld4p:RT:bf2:Monograph:Instance:Un-nested",
        "types": ["http://id.loc.gov/ontologies/bibframe/Instance"],
    }
}

mock_work_jsonld = json.dumps(
    [
        {
            "@id": "https://api.development.sinopia.io/resource/6497a461-42dc-42bf-b433-5e47c73f7e89",
            "@type": ["http://id.loc.gov/ontologies/bibframe/Work"],
            "http://id.loc.gov/ontologies/bibframe/title": [{"@id": "_:b49"}],
        },
        {
            "@id": "_:b49",
            "@type": ["http://id.loc.gov/ontologies/bibframe/Title"],
            "http://id.loc.gov/ontologies/bibframe/mainTitle": [
                {
                    "@language": "eng",
                    "@value": "The California wildlife habitat garden",
                }
            ],
            "http://id.loc.gov/ontologies/bibframe/subtitle": [
                {
                    "@language": "eng",
                    "@value": "how to attract bees, butterflies, birds, and other animals",
                }
            ],
        },
    ]
)

instance_uri = (
    "https://api.stage.sinopia.io/resource/8a2dda53-d3bc-485a-9154-635823045b4f"
)
work_uri = "https://api.sinopia.io/resources/not-found"


@pytest.fixture
def mock_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        url = args[0]
        if url.startswith(work_uri):
            get_response.status_code = 401
        else:
            get_response.status_code = 200
            get_response.text = mock_work_jsonld
        return get_response

    monkeypatch.setattr("ils_middleware.tasks.folio.graph.httpx.get", mock_get)


def test_construct_graph(mock_requests, mock_task_instance):  # noqa: F811
    """Tests construct_graph"""

    construct_graph(
        task_instance=test_task_instance(),
    )

    assert "graph" in test_task_instance().xcom_pull(key="0000-1111-2222-3333")

    assert (
        test_task_instance().xcom_pull(key="0000-1111-2222-3333").get("work_uri")
        == "https://api.development.sinopia.io/resource/6497a461-42dc-42bf-b433-5e47c73f7e89"
    )


def test_missing_instance_of_build_graph(mock_requests):
    instance_uri = "https://dev.bcld.info/instance/da30d80a-9aad-48ed-b4a4-687e380d422b"

    with pytest.raises(ValueError, match="missing bf:instanceOf"):
        _build_graph([], instance_uri)


@pytest.fixture
def mock_bad_work_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key.startswith("resources"):
            return [
                instance_uri,
            ]
        return {"resource": mock_instance_doc[instance_uri]}

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_missing_work_build_graph(mock_requests, mock_bad_work_task_instance):
    instance_uri = "https://dev.bcld.info/instance/da30d80a-9aad-48ed-b4a4-687e380d422b"
    instance_jsonld = [
        {
            "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
            "@id": instance_uri,
            "http://id.loc.gov/ontologies/bibframe/instanceOf": [{"@id": work_uri}],
        }
    ]
    with pytest.raises(ValueError, match=f"Error retrieving {work_uri}"):
        _build_graph(instance_jsonld, instance_uri)


def test_build_graph_with_bluecore_api_context(monkeypatch, mocker: MockerFixture):
    bc_instance = "http://localhost/instances/d576df38-2e54-4ec6-a584-6831b2775f4b"
    bc_work = "http://localhost/works/338c9d94-416a-4b2b-b12f-d25cbbdaa997"
    api_context = "http://localhost/api/context.jsonld"

    def mock_get(*args, **kwargs):
        response = mocker.stub(name="get_result")
        response.status_code = 200
        response.text = json.dumps(
            {"@context": api_context, "@id": bc_work, "@type": "Work"}
        )
        return response

    monkeypatch.setattr("ils_middleware.tasks.folio.graph.httpx.get", mock_get)

    instance = {
        "@context": api_context,
        "@id": bc_instance,
        "@type": "Instance",
        "instanceOf": bc_work,
    }
    graph, work = _build_graph(instance, bc_instance)

    assert work == bc_work
    assert len(graph) == 3
