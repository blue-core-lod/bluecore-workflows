import pytest
import json
import requests  # type: ignore
from datetime import datetime

from pytest_mock import MockerFixture

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

CATKEY = "320011"
MARC_JSON = {
    "leader": "11222999   adf",
    "fields": [{"tag": "245"}],
    "SIRSI": [
        CATKEY,
    ],
}
MARC_JSON_NO_CAT_KEY = {"leader": "11222999   adf", "fields": [{"tag": "245"}]}


def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


def test_task_instance():
    return TaskInstance(test_task())


def test_alma_api_key():
    return ["12ab34c56789101112131415161718192021"]


def test_import_profile_id():
    return ["33008879050003681"]


def test_uri_region():
    return ["https://api-na.hosted.exlibrisgroup.com"]


def test_xml_response():
    return [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?">'
        "<bib>"
        "<mms_id>9978021305103681</mms_id>"
        "</bib>",
    ]


def test_mms_id():
    return ["9978021305103681"]


mock_resources = {
    "0000-1111-2222-3333": {
        "user": "jpnelson",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "data": [
            {
                "@id": "_:b29",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Title"],
                "http://id.loc.gov/ontologies/bibframe/mainTitle": [
                    {"@value": "Great force", "@language": "eng"}
                ],
            },
            {
                "@id": "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
                "http://sinopia.io/vocabulary/hasResourceTemplate": [
                    {"@value": "pcc:bf2:Monograph:Instance"}
                ],
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
                "http://id.loc.gov/ontologies/bibframe/title": [{"@id": "_:b29"}],
                "http://id.loc.gov/ontologies/bibframe/date": [
                    {"@language": "eng", "@value": "2012"}
                ],
            },
        ],
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
        "uri": "https://api.development.sinopia.io/resource/7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "timestamp": "2021-10-29T20:30:58.821Z",
    },
    "4444-5555-6666-7777": {
        "user": "jpnelson",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "data": [
            {
                "@id": "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
                "http://id.loc.gov/ontologies/bibframe/date": [
                    {"@language": "eng", "@value": "2012"}
                ],
            },
        ],
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
        "uri": "https://api.development.sinopia.io/resource/7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "timestamp": "2021-10-29T20:30:58.821Z",
    },
    "8888-9999-0000-1111": {
        "user": "jpnelson",
        "group": "stanford",
    },
}

mock_resource_attributes = {
    "0000-1111-2222-3333": {
        "email": "dscully@stanford.edu",
        "group": "stanford",
        "target": "ils",
    },
    "4444-5555-6666-7777": {
        "email": "fmulder@stanford.edu",
        "group": "yale",
        "target": "ils",
    },
    "8888-9999-0000-1111": {
        "email": "fmulder@stanford.edu",
        "group": "yale",
        "target": "ils",
    },
}

overlay_resources = [
    {
        "resource_uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
        "catkey": [{"SIRSI": CATKEY}],
    },
    {
        "resource_uri": "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
        "catkey": [],
    },
]


mock_push_store: dict = {}


def mock_message():
    return [
        {
            "Body": """{ "user": { "email": "dscully@stanford.edu" },
                         "group": "stanford",
                         "target": "ils",
                         "resource": { "uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333" }}"""
        },
        {
            "Body": """{ "user": { "email": "fmulder@stanford.edu" },
                         "group": "yale",
                         "target": "ils",
                         "resource": { "uri": "https://api.development.sinopia.io/resource/4444-5555-6666-7777" }}"""
        },
        {
            "Body": """{ "group": "yale",
                         "target": "ils",
                         "resource": { "uri": "https://api.development.sinopia.io/resource/8888-9999-0000-1111" }}"""
        },
    ]


def marc_as_json():
    with open("tests/fixtures/record.json") as data:
        return json.load(data)


return_marc_tasks = [
    "process_symphony.convert_to_symphony_json",
    "process_symphony.marc_json_to_s3",
]

folio_properties = {
    "contributorTypes": [
        {"id": "6e09d47d-95e2-4d8a-831b-f777b8ef6d81", "name": "Author"}
    ],
    "contributorNameTypes": [
        {"id": "2b94c631-fca9-4892-a730-03ee529ffe2a", "name": "Personal name"}
    ],
    "identifierTypes": [
        {"id": "8261054f-be78-422d-bd51-4ed9f33c3422", "name": "ISBN"},
        {"id": "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef", "name": "OCLC"},
    ],
    "instanceFormats": [
        {"id": "8d511d33-5e85-4c5d-9bce-6e3c9cd0c324", "name": "unmediated -- volume"},
    ],
    "instanceTypes": [{"id": "6312d172-f0cf-40f6-b27d-9fa8feaf332f", "name": "text"}],
    "issuanceModes": [
        {"id": "9d18a02f-5897-4c31-9106-c9abb5c7ae8b", "name": "single unit"}
    ],
    "instanceNoteTypes": [
        {"id": "6a2533a7-4de2-4e64-8466-074c2fa9308c", "name": "General note"},
    ],
}

folio_ids = {
    "0000-1111-2222-3333": {
        "id": "98a0337a-ec22-53aa-8ffc-933a86d10159",
        "hrid": "in000789",
        "electronicAccess": [
            {
                "uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
            }
        ],
    },
    "4444-5555-6666-7777": {
        "id": "147b1171-740e-513e-84d5-b63a9642792c",
        "hrid": "in000001234",
        "electronicAccess": [
            {
                "uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
            }
        ],
    },
}


@pytest.fixture
def mock_requests_okapi(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        get_response.text = json.dumps(folio_properties)
        return get_response

    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "some_jwt_token"}
        post_response.raise_for_status = lambda: None

        return post_response

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 201
        put_response.text = ""
        put_response.raise_for_status = lambda: None
        return put_response

    def mock_raise_for_status(*args, **kwargs):
        error_response = mocker.stub(name="post_error")
        error_response.status_code = 500
        error_response.text = "Internal server error"

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)
    monkeypatch.setattr(requests.Response, "raise_for_status", mock_raise_for_status)


@pytest.fixture
def mock_task_instance(monkeypatch, tmp_path):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids", [""])
        if key == "resources":
            return [
                "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
                "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
            ]
        elif task_ids == "get-message-from-context":
            return json.loads(mock_message()[0]["Body"])
        elif key == "messages":
            return mock_message()
        elif key in mock_resources and task_ids == "sqs-message-parse":
            return {
                "email": mock_resource_attributes[key]["email"],
                "group": mock_resource_attributes[key]["group"],
                "target": mock_resource_attributes[key]["target"],
                "resource_uri": f"https://api.development.sinopia.io/resource/{key}",
                "resource": mock_resources[key],
            }
        elif key == "overlay_resources":
            return overlay_resources
        elif task_ids in return_marc_tasks:
            return json.dumps(marc_as_json())
        elif key == "new_resources":
            return mock_resources
        elif task_ids == "process_symphony.download_marc":
            return "tests/fixtures/record.mar"
        elif key == "conversion_failures" and task_ids == "process_symphony.rdf2marc":
            return ["https://api.development.sinopia.io/resource/8888-9999-0000-1111"]
        elif isinstance(task_ids, str):
            if task_ids.endswith("title_task"):
                return [["Great force", None, None, None]]
            if task_ids.endswith("Person_task"):
                return [["Brioni, Simone", "Author"]]
            if task_ids.endswith("build-folio"):
                return folio_ids[key]
            if task_ids.endswith("process_alma.download_marc"):
                return "tests/fixtures/record.mar"
        else:
            return mock_push_store.get(key)

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = value
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)
