import pytest
import requests  # type: ignore

from pytest_mock import MockerFixture
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.folio.graph import construct_graph, _build_graph
from tasks import test_task_instance, mock_task_instance  # noqa: F401

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

mock_work = {
    "data": [
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
}

instance_uri = (
    "https://api.stage.sinopia.io/resource/8a2dda53-d3bc-485a-9154-635823045b4f"
)
work_uri = "https://api.sinopia.io/resources/not-found"


@pytest.fixture
def mock_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        if args[0] == work_uri:
            get_response.status_code = 401
        else:
            get_response.status_code = 200
            get_response.json = lambda: mock_work
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


def test_construct_graph(mock_requests, mock_task_instance):  # noqa: F811
    """Tests construct_graph"""

    construct_graph(
        task_instance=test_task_instance(),
    )

    assert "graph" in test_task_instance().xcom_pull(key="0000-1111-2222-3333").keys()

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
