import datetime
import json
from unittest.mock import MagicMock

import pytest
from airflow.models import Connection
from tasks import (
    folio_properties,
    mock_requests_okapi,  # noqa: F401
    mock_task_instance,  # noqa: F401
    test_task_instance,  # noqa: F401
)

import ils_middleware.tasks.folio.build as folio_build
from ils_middleware.tasks.folio.build import (
    _alternative_titles,
    _cataloged_date,
    _classifications,
    _default_transform,
    _editions,
    _electronic_access,
    _genre,
    _identifiers,
    _instance_format_ids,
    _instance_type_id,
    _inventory_record,
    _language,
    _mode_of_issuance_id,
    _non_primary_contributor,
    _notes,
    _physical_descriptions,
    _publication,
    _publication_frequency,
    _series,
    _subjects,
    _title,
    _user_folio_id,
    build_records,
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
            {"id": "7e591197-f335-4afb-bc6d-a6d76ca3bace", "name": "Local identifier"},
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
        self.subject_types = [
            {"id": "d6488f88-1e74-40ce-81b5-b19a928ff5b7", "name": "Topical term"},
            {"id": "d6488f88-1e74-4674-9e7f-a294b9a6451d", "name": "Genre/form"},
        ]
        self.alternative_title_types = [
            {"id": "bba5b222-1cc8-4745-9a75-3f1d3b5c3a67", "name": "Abbreviated title"},
            {"id": "4bb300a4-04c9-414b-bfbc-9c032f74b7b2", "name": "Parallel title"},
            {"id": "35bbe7f2-1a49-11ed-861d-0242ac120002", "name": "Variant title"},
        ]
        self.classification_types = [
            {"id": "ce176ace-a53e-4b4d-aa89-725ed7b2edac", "name": "LC"},
            {"id": "42471af9-7d25-4f3a-bf78-60d29dcf463b", "name": "Dewey"},
            {"id": "a7f4d03f-b0d8-496c-aebf-4e9cdb678200", "name": "NLM"},
        ]
        self.instance_statuses = [
            {"id": "9634a5ab-9228-4703-baf2-4d12ebc77d56", "name": "Cataloged"},
        ]

    def folio_get(self, *args, **kwargs):
        get_response = MagicMock()
        get_response.status_code = 200
        get_response.text = json.dumps(folio_properties)
        if args[0].endswith("electronic-access-relationships"):
            return [{"name": "Resource", "id": "d2f38edc-b225-4cb4-a412-734d8bbbc855"}]
        if args[0].endswith("subject-types"):
            return [
                {"name": "Genre/form", "id": "d6488f88-1e74-4674-9e7f-a294b9a6451d"}
            ]
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
    assert record["sourceUri"].startswith(instance_uri)
    assert record["title"] == "Great force"
    assert record["source"] == "BIBFRAME"


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
    assert record["sourceUri"].startswith(instance_uri)


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
    assert record["sourceUri"].startswith(instance_uri)
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
    assert (notes[1][0]["instanceNoteTypeId"]).startswith(
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
    subjects = _subjects(
        values=[["California"], ["Forest biodiversity"]],
        record={},
        folio_client=MockFolioClient(),
    )
    assert subjects[0] == "subjects"
    assert subjects[1][0] == {
        "value": "California",
        "typeId": "d6488f88-1e74-40ce-81b5-b19a928ff5b7",
    }
    assert subjects[1][1] == {
        "value": "Forest biodiversity",
        "typeId": "d6488f88-1e74-40ce-81b5-b19a928ff5b7",
    }


def test_editions_accumulates():
    _, instance_editions = _editions(values=[["First edition"]], record={})
    _, merged = _editions(
        values=[["Première édition"]], record={"editions": instance_editions}
    )
    assert len(merged) == 2
    assert "First edition" in merged
    assert "Première édition" in merged


def test_non_primary_contributor():
    field, contributors = _non_primary_contributor(
        values=[["Butler, Octavia", "Author"]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert field == "contributors"
    assert contributors[0]["primary"] is False
    assert contributors[0]["name"] == "Butler, Octavia"


def test_identifiers_local():
    identifiers = _identifiers(
        values=[["(OCoLC)1272909598"]],
        folio_client=MockFolioClient(),
        folio_field="identifiers.local",
        record={},
    )
    assert identifiers[0] == "identifiers"
    assert (
        identifiers[1][0]["identifierTypeId"] == "7e591197-f335-4afb-bc6d-a6d76ca3bace"
    )
    assert identifiers[1][0]["value"] == "(OCoLC)1272909598"


def test_genre_merges_into_subjects():
    existing_subjects = [{"value": "Mining engineering"}]
    field, subjects = _genre(
        values=[["Science fiction"]],
        folio_client=MockFolioClient(),
        record={"subjects": existing_subjects},
    )
    assert field == "subjects"
    assert len(subjects) == 2
    assert subjects[0] == {"value": "Mining engineering"}
    assert subjects[1]["value"] == "Science fiction"
    assert subjects[1]["typeId"] == "d6488f88-1e74-4674-9e7f-a294b9a6451d"


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


def test_cataloged_date():
    result = _cataloged_date(values=[["2021-10-01"]])
    assert result[0] == "catalogedDate"
    assert result[1] == "2021-10-01"


def test_publication_frequency():
    result = _publication_frequency(values=[["annual"], ["monthly"]])
    assert result[0] == "publicationFrequency"
    assert result[1] == ["annual", "monthly"]


def test_electronic_access():
    result = _electronic_access(
        values=[
            ["https://purl.stanford.edu/mf283yt5578"],
            ["https://phaidra.cab.unipd.it/detail/o:445140"],
        ]
    )
    assert result[0] == "electronicAccess"
    assert result[1][0] == {"uri": "https://purl.stanford.edu/mf283yt5578"}
    assert result[1][1] == {"uri": "https://phaidra.cab.unipd.it/detail/o:445140"}


def test_series():
    field, series = _series(values=[["Italian studies series"]], record={})
    assert field == "series"
    assert series[0] == {"value": "Italian studies series"}


def test_series_accumulates():
    _, first_series = _series(values=[["Diaspore"]], record={})
    _, merged = _series(
        values=[["Italian studies series"]], record={"series": first_series}
    )
    assert len(merged) == 2
    assert {"value": "Diaspore"} in merged
    assert {"value": "Italian studies series"} in merged


def test_classifications_lcc():
    result = _classifications(
        folio_field="classifications.lcc",
        values=[["BP52.5"]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "classifications"
    assert result[1][0]["classificationNumber"] == "BP52.5"
    assert (
        result[1][0]["classificationTypeId"] == "ce176ace-a53e-4b4d-aa89-725ed7b2edac"
    )


def test_classifications_ddc():
    result = _classifications(
        folio_field="classifications.ddc",
        values=[["297.09451"]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "classifications"
    assert result[1][0]["classificationNumber"] == "297.09451"
    assert (
        result[1][0]["classificationTypeId"] == "42471af9-7d25-4f3a-bf78-60d29dcf463b"
    )


def test_classifications_nlm():
    result = _classifications(
        folio_field="classifications.nlm",
        values=[["BP52"]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "classifications"
    assert result[1][0]["classificationNumber"] == "BP52"
    assert (
        result[1][0]["classificationTypeId"] == "a7f4d03f-b0d8-496c-aebf-4e9cdb678200"
    )


def test_classifications_accumulates():
    _, first_class = _classifications(
        folio_field="classifications.lcc",
        values=[["BP52.5"]],
        folio_client=MockFolioClient(),
        record={},
    )
    _, merged = _classifications(
        folio_field="classifications.ddc",
        values=[["297.09451"]],
        folio_client=MockFolioClient(),
        record={"classifications": first_class},
    )
    assert len(merged) == 2
    assert merged[0]["classificationNumber"] == "BP52.5"
    assert merged[1]["classificationNumber"] == "297.09451"


def test_alternative_titles_parallel():
    result = _alternative_titles(
        folio_field="alternative_titles.parallel",
        values=[["Writing about Islam", "narrating a diaspora", None, None]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "alternativeTitles"
    assert (
        result[1][0]["alternativeTitleTypeId"] == "4bb300a4-04c9-414b-bfbc-9c032f74b7b2"
    )
    assert (
        result[1][0]["alternativeTitle"] == "Writing about Islam : narrating a diaspora"
    )


def test_alternative_titles_variant_no_subtitle():
    result = _alternative_titles(
        folio_field="alternative_titles.variant",
        values=[["Scrivere Islam", None, None, None]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "alternativeTitles"
    assert (
        result[1][0]["alternativeTitleTypeId"] == "35bbe7f2-1a49-11ed-861d-0242ac120002"
    )
    assert result[1][0]["alternativeTitle"] == "Scrivere Islam"


def test_alternative_titles_abbreviated():
    result = _alternative_titles(
        folio_field="alternative_titles.abbreviated",
        values=[["Islam writing", None, None, None]],
        folio_client=MockFolioClient(),
        record={},
    )
    assert result[0] == "alternativeTitles"
    assert (
        result[1][0]["alternativeTitleTypeId"] == "bba5b222-1cc8-4745-9a75-3f1d3b5c3a67"
    )
    assert result[1][0]["alternativeTitle"] == "Islam writing"


def test_alternative_titles_accumulates():
    _, first_titles = _alternative_titles(
        folio_field="alternative_titles.parallel",
        values=[["Writing about Islam", None, None, None]],
        folio_client=MockFolioClient(),
        record={},
    )
    _, merged = _alternative_titles(
        folio_field="alternative_titles.variant",
        values=[["Scrivere Islam", None, None, None]],
        folio_client=MockFolioClient(),
        record={"alternativeTitles": first_titles},
    )
    assert len(merged) == 2
    assert merged[0]["alternativeTitle"] == "Writing about Islam"
    assert merged[1]["alternativeTitle"] == "Scrivere Islam"
