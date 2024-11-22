import datetime
import json

from unittest.mock import MagicMock

import pytest

from airflow.models import Connection

from tasks import test_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import folio_properties


import ils_middleware.tasks.folio.build as folio_build

from ils_middleware.tasks.folio.build import (
    build_records,
    _default_transform,
    _identifiers,
    _instance_format_ids,
    _instance_type_id,
    _inventory_record,
    _language,
    _mode_of_issuance_id,
    _notes,
    _physical_descriptions,
    _publication,
    _subjects,
    _title,
    _user_folio_id,
)

instance_uri = "https://api.development.sinopia.io/resource/0000-1111-2222-3333"
okapi_uri = "https://okapi-folio.dev.edu"


@pytest.fixture
def mock_variable(monkeypatch):
    datetime_mock = MagicMock(wrap=datetime.datetime)
    datetime_mock.isoformat.return_value = "2021-12-06T15:30:28.000124"

    monkeypatch.setattr(datetime, "datetime", datetime_mock)


class MockFolioClient(object):
    def __init__(self, *args):
        self.okapi_url = okapi_uri
        self.username = "folio_user"
        self.contributor_types = [
            {"id": "6e09d47d-95e2-4d8a-831b-f777b8ef6d81", "name": "Author"}
        ]
        self.contrib_name_types = [
            {"id": "2b94c631-fca9-4892-a730-03ee529ffe2a", "name": "Personal name"},
            {"id": "e8b311a6-3b21-43f2-a269-dd9310cb2d0a", "name": "Meeting name"},
        ]

        self.identifier_types = [
            {"id": "39554f54-d0bb-4f0a-89a4-e422f6136316", "name": "DOI"},
            {"id": "8261054f-be78-422d-bd51-4ed9f33c3422", "name": "ISBN"},
            {"id": "913300b2-03ed-469a-8179-c1092c991227", "name": "ISSN"},
            {"id": "c858e4f2-2b6b-4385-842b-60732ee14abb", "name": "LCCN"},
            {"id": "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef", "name": "OCLC"},
        ]

        self.instance_formats = [
            {
                "id": "8d511d33-5e85-4c5d-9bce-6e3c9cd0c324",
                "name": "unmediated -- volume",
            }
        ]

        self.instance_types = [
            {"id": "6312d172-f0cf-40f6-b27d-9fa8feaf332f", "name": "text"}
        ]

        self.modes_of_issuance = [
            {"id": "9d18a02f-5897-4c31-9106-c9abb5c7ae8b", "name": "single unit"}
        ]
        self.instance_note_types = [
            {"id": "6a2533a7-4de2-4e64-8466-074c2fa9308c", "name": "General note"},
        ]

    def folio_get(self, *args, **kwargs):
        get_response = MagicMock()
        get_response.status_code = 200
        get_response.text = json.dumps(folio_properties)
        if args[0].endswith("electronic-access-relationships"):
            return [{"name": "Resource", "id": "d2f38edc-b225-4cb4-a412-734d8bbbc855"}]
        return get_response

    def folio_put(self, *args, **kwargs):
        put_response = MagicMock()
        put_response.status_code = 201
        put_response.text = ""
        put_response.raise_for_status = lambda: None
        return put_response

    def folio_post(self, *args, **kwargs):
        post_response = MagicMock()
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "some_jwt_token"}
        post_response.raise_for_status = lambda: None

        return post_response


@pytest.fixture
def mock_folio_client(monkeypatch):
    monkeypatch.setattr(folio_build, "FolioClient", MockFolioClient)


@pytest.fixture
def mock_airflow_connection():
    return Connection(
        conn_id="stanford_folio",
        conn_type="http",
        host=okapi_uri,
        login="folio_user",
        password="pass",
        extra={"tenant": "sul "},
    )


def test_happypath_build_records(
    mocker,
    mock_airflow_connection,
    mock_folio_client,  # noqa: F811
    mock_requests_okapi,  # noqa: F811
    mock_task_instance,  # noqa: F811
):  # noqa: F811

    mocker.patch(
        "ils_middleware.tasks.folio.build.Connection.get_connection_from_secrets",
        return_value=mock_airflow_connection,
    )

    build_records(
        task_instance=test_task_instance(),  #
        task_groups_ids=[],
        folio_url=okapi_uri,
        folio_connection_id="stanford_folio",
    )
    instance_uuid = instance_uri.split("/")[-1]
    record = test_task_instance().xcom_pull(key=instance_uuid)

    assert record["metadata"]["createdByUserId"].startswith(
        "faecc486-50f1-5082-a6d0-5e967e6f4786"
    )
    assert record["electronicAccess"][0]["uri"].startswith(instance_uri)
    assert record["title"] == "Great force"


def test_default_transform_value_listing():
    folio_field = "contributor.Primary"
    name = "Butler, Octavia"
    default_tuple = _default_transform(
        folio_field=folio_field,
        values=[
            [
                name,
            ],
        ],
    )

    assert default_tuple[0].startswith(folio_field)
    assert name in default_tuple[1][0]


def test_identifiers_doi(mock_folio_client, mock_task_instance):  # noqa: F811
    identifiers = _identifiers(
        values=[["10.1111/j.1753-4887.2008.00114.x:"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.doi",
        record={},
    )

    assert identifiers[1][0]["identifierTypeId"].startswith(
        "39554f54-d0bb-4f0a-89a4-e422f6136316"
    )
    assert identifiers[1][0]["value"].startswith("10.1111/j.1753-4887.2008.00114.x:")


def test_identifiers_issn(mock_folio_client, mock_task_instance):  # noqa: F811
    identifiers = _identifiers(
        values=[["123456"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.issn",
        record={},
    )

    assert (identifiers[1][0]["identifierTypeId"]).startswith(
        "913300b2-03ed-469a-8179-c1092c991227"
    )

    assert (identifiers[1][0]["value"]).startswith("123456")


def test_identifiers_isbn(mock_folio_client, mock_task_instance):  # noqa: F811

    identifiers = _identifiers(
        values=[["123456"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.isbn",
        record={},
    )

    assert (identifiers[0]).startswith("identifiers")
    assert (identifiers[1][0]["identifierTypeId"]).startswith(
        "8261054f-be78-422d-bd51-4ed9f33c3422"
    )
    assert (identifiers[1][0]["value"]).startswith("123456")


def test_identifiers_lccn(mock_folio_client, mock_task_instance):  # noqa: F811
    identifiers = _identifiers(
        values=[["2023045856"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.lccn",
        record={},
    )

    assert identifiers[1][0]["identifierTypeId"].startswith(
        "c858e4f2-2b6b-4385-842b-60732ee14abb"
    )
    assert identifiers[1][0]["value"].startswith("2023045856")


def test_identifiers_oclc(mock_task_instance):  # noqa: F811
    identifiers = _identifiers(
        values=[["654321"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.oclc",
        record={
            "identifiers": [
                {
                    "identifierTypeId": "8261054f-be78-422d-bd51-4ed9f33c3422",
                    "value": "44556676",
                }
            ]
        },
    )

    assert (identifiers[0]).startswith("identifiers")
    assert (identifiers[1][0]["value"]).startswith("44556676")
    assert (identifiers[1][1]["identifierTypeId"]).startswith(
        "439bfbae-75bc-4f74-9fc7-b2a2d47ce3ef"
    )
    assert (identifiers[1][1]["value"]).startswith("654321")


def test_instance_format_ids(mock_task_instance):  # noqa: F811
    format_ids = _instance_format_ids(
        values=[["unmediated", "volume"]], folio_client=MockFolioClient()
    )
    assert (format_ids[0]).startswith("instanceFormatIds")
    assert (format_ids[1][0]).startswith("8d511d33-5e85-4c5d-9bce-6e3c9cd0c324")


def test_inventory_record(mock_task_instance):  # noqa: F811
    record = _inventory_record(
        instance_uri=instance_uri,
        task_instance=test_task_instance(),
        task_groups_ids=[""],
        folio_url=okapi_uri,
        folio_client=MockFolioClient(),
    )
    assert record["electronicAccess"][0]["uri"].startswith(instance_uri)


def test_inventory_record_existing_metadata(
    mock_task_instance,  # noqa: F811
):  # noqa: F811
    metadata = {
        "createdDate": "2021-12-06T15:45:28.140795",
        "createdByUserId": "9b80f3af-a07a-5e6a-a5fb-3d5723ea94de",
    }
    record = _inventory_record(
        instance_uri=instance_uri,
        task_instance=test_task_instance(),
        task_groups_ids=["folio"],
        folio_url=okapi_uri,
        username="test_user",
        folio_client=MockFolioClient(),
        metadata=metadata,
    )
    assert record["electronicAccess"][0]["uri"].startswith(instance_uri)
    assert record["metadata"]["createdDate"].startswith("2021-12-06T15:45:28.140795")


def test_inventory_record_no_values():
    with pytest.raises(KeyError, match="instance_uri"):
        _inventory_record()


def test_instance_type_id():
    instance_type_id = _instance_type_id(
        values=[["Text"]], folio_client=MockFolioClient()
    )
    assert instance_type_id[0].startswith("instanceTypeId")
    assert (instance_type_id[1]).startswith("6312d172-f0cf-40f6-b27d-9fa8feaf332f")


def test_unknown_instance_type_id():
    with pytest.raises(ValueError, match="instanceTypeId for foo not found"):
        _instance_type_id(values=[["foo"]], folio_client=MockFolioClient())


def test_language():
    languages = _language(
        values=[["http://id.loc.gov/vocabulary/languages/eng", "English"]]
    )

    assert (languages[0]).startswith("languages")
    assert (languages[1][0]).startswith("eng")


def test_mode_of_issuance_id():
    mode_of_issuance = _mode_of_issuance_id(
        values=[["single unit"]], folio_client=MockFolioClient()
    )
    assert (mode_of_issuance[0]).startswith("modeOfIssuance")
    assert (mode_of_issuance[1]).startswith("9d18a02f-5897-4c31-9106-c9abb5c7ae8b")


def test_notes():  # noqa: F811
    notes = _notes(values=[["A great note"]], folio_client=MockFolioClient())
    assert (notes[0]).startswith("notes")
    assert (notes[1][0]["instanceNoteId"]).startswith(
        "6a2533a7-4de2-4e64-8466-074c2fa9308c"
    )
    assert (notes[1][0]["note"]).startswith("A great note")
    assert notes[1][0]["staffOnly"] is False


def test_physical_descriptions():
    phys_desc = _physical_descriptions(
        values=[["xxix, 609 pages", "29 cm"]],
    )
    assert (phys_desc[0]).startswith("physicalDescriptions")
    assert (phys_desc[1][0]).startswith("xxix, 609 pages, 29 cm")


def test_publication():
    publications = _publication(values=[["Heyday Books", "2020", "Berkeley (Calif.)"]])
    assert (publications[0]).startswith("publication")
    assert (publications[1][0]["publisher"]).startswith("Heyday Books")
    assert (publications[1][0]["dateOfPublication"]).startswith("2020")
    assert (publications[1][0]["place"]).startswith("Berkeley (Calif.)")


def test_subjects():
    subjects = _subjects(values=[["California"], ["Forest biodiversity"]])
    assert (subjects[0]).startswith("subjects")
    assert (subjects[1][0]).startswith("California")
    assert (subjects[1][1]).startswith("Forest biodiversity")


def test_title_transform_all():
    title_tuple = _title(values=[["COVID-19", "Survivors", "California", "1st"]])
    assert title_tuple[0].startswith("title")
    assert title_tuple[1].startswith("COVID-19 : Survivors. California, 1st")


def test_title_none_parts():
    title_tuple = _title(values=[["COVID-19", None, None, None]])
    assert title_tuple[0].startswith("title")
    assert title_tuple[1].startswith("COVID-19")


def test_folio_uuid():
    user_uuid = _user_folio_id(okapi_uri, "dschully")

    assert user_uuid.startswith("5415dbd9-8f80-50a2-9d6c-b1c932a4a6a5")
