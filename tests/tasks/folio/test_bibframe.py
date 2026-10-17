import httpx
import pytest
import rdflib
from bluecore_models.utils.graph import load_jsonld

from ils_middleware.tasks.folio.bibframe import (
    folio_client,
    folio_institution,
    folio_institution_for,
    folio_to_bf_instance,
    folio_to_bf_work,
    generate_bibframe,
    ingest_into_bluecore,
    reference_data_lookups,
    retrieve_folio_instance,
    update_source_uri,
)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

REFERENCE_DATA = {
    "alt_title_types": [{"id": "alt-1", "name": "Variant title"}],
    "class_types": [
        {"id": "class-lc", "name": "LC"},
        {"id": "class-other", "name": "SUDOC"},
    ],
    "contrib_name_types": [
        {"id": "name-personal", "name": "Personal name"},
        {"id": "name-corporate", "name": "Corporate name"},
    ],
    "contributor_types": [{"id": "role-composer", "name": "composer"}],
    "identifier_types": [
        {"id": "id-oclc", "name": "OCLC"},
        {"id": "id-upc", "name": "UPC"},
        {"id": "id-other", "name": "Publisher or distributor number"},
    ],
    "instance_formats": [
        {"id": "format-cd", "name": "audio -- audio disc"},
    ],
    "instance_note_types": [
        {"id": "note-general", "name": "General note"},
        {"id": "note-summary", "name": "Summary"},
        {"id": "note-source", "name": "Source of Description note"},
    ],
    "instance_types": [
        {"id": "type-music", "name": "performed music"},
        {"id": "type-unspecified", "name": "unspecified"},
    ],
    "modes_of_issuance": [{"id": "mode-single", "name": "single unit"}],
}

FOLIO_INSTANCE = {
    "id": "8c9b5c6f-6e2e-4f43-9f4b-3a0b3e4f1d2a",
    "hrid": "a123",
    "title": "Kind of blue",
    "alternativeTitles": [
        {"alternativeTitleTypeId": "alt-1", "alternativeTitle": "Blue"}
    ],
    "editions": ["Legacy edition"],
    "instanceTypeId": "type-music",
    "modeOfIssuanceId": "mode-single",
    "instanceFormatIds": ["format-cd"],
    "catalogedDate": "2024-05-01",
    "contributors": [
        {
            "name": "Davis, Miles",
            "contributorNameTypeId": "name-personal",
            "contributorTypeText": "instrumentalist.",
            "primary": True,
        },
        {
            "name": "Columbia Records",
            "contributorNameTypeId": "name-corporate",
            "contributorTypeId": "role-composer",
            "primary": False,
        },
    ],
    "subjects": ["Jazz", {"value": "Modal jazz", "authorityId": None}],
    "classifications": [
        {"classificationTypeId": "class-lc", "classificationNumber": "M1366.D38"},
        {"classificationTypeId": "class-other", "classificationNumber": "X 1.2"},
    ],
    "languages": ["eng"],
    "series": [
        {"value": "Columbia jazz masterpieces"},
        {"value": "Jazz hub series", "authorityId": "auth-1"},
    ],
    "identifiers": [
        {"identifierTypeId": "id-oclc", "value": "(OCoLC)123"},
        {"identifierTypeId": "id-upc", "value": "074646"},
        {"identifierTypeId": "id-other", "value": "074646"},
    ],
    "physicalDescriptions": ["1 audio disc ; 4 3/4 in."],
    "publication": [
        {
            "role": "Publication",
            "publisher": "Columbia",
            "place": "New York",
            "dateOfPublication": "1959",
        },
        {"role": "Copyright notice date", "dateOfPublication": "℗1959"},
    ],
    "electronicAccess": [
        {
            "uri": "https://example.org/kob",
            "linkText": "Liner notes",
            "publicNote": "Stanford only",
        }
    ],
    "notes": [
        {
            "instanceNoteTypeId": "note-general",
            "note": "Recorded 1959.",
            "staffOnly": False,
        },
        {"instanceNoteTypeId": "note-summary", "note": "A modal jazz album."},
        {"instanceNoteTypeId": "note-source", "note": "Description based on disc."},
        {
            "instanceNoteTypeId": "note-general",
            "note": "Staff only!",
            "staffOnly": True,
        },
    ],
}


@pytest.fixture
def reference_data():
    class MockFolioClient:
        pass

    client = MockFolioClient()
    for prop, rows in REFERENCE_DATA.items():
        setattr(client, prop, rows)
    return reference_data_lookups(client)


def _graph(doc: dict) -> rdflib.Graph:
    # As ingest reads it: the @context URL is resolved from the installed
    # bibframe-json, not fetched
    return load_jsonld(doc)


def test_folio_to_bf_work(reference_data):
    work = folio_to_bf_work(FOLIO_INSTANCE, reference_data)

    assert work["@type"] == "bf:Work"
    assert work["@id"].startswith("urn:uuid:")
    assert "bf:hasInstance" not in work
    assert work["bf:content"] == {"rdfs:label": "performed music"}
    assert work["bf:title"] == [
        {"@type": "bf:Title", "bf:mainTitle": "Kind of blue"},
        {"@type": "bf:VariantTitle", "bf:mainTitle": "Blue"},
    ]
    assert work["bf:editionStatement"] == ["Legacy edition"]

    primary, secondary = work["bf:contribution"]
    assert primary == {
        "@type": "bf:PrimaryContribution",
        "bf:agent": {"@type": "bf:Person", "rdfs:label": "Davis, Miles"},
        "bf:role": {"rdfs:label": "Instrumentalist"},
    }
    assert secondary["@type"] == "bf:Contribution"
    assert secondary["bf:agent"]["@type"] == "bf:Organization"
    assert secondary["bf:role"] == {"rdfs:label": "Composer"}

    assert work["bf:subject"] == [{"rdfs:label": "Jazz"}, {"rdfs:label": "Modal jazz"}]
    assert work["bf:classification"] == [
        {"@type": "bf:ClassificationLcc", "bf:classificationPortion": "M1366.D38"},
        {"@type": "bf:Classification", "bf:classificationPortion": "X 1.2"},
    ]
    assert work["bf:language"] == [{"rdfs:label": "eng"}]
    series, hub = work["bf:relation"]
    assert series["bf:associatedResource"]["@type"] == "bf:Series"
    assert hub["bf:associatedResource"] == {
        "@type": "bf:Hub",
        "rdfs:label": "Jazz hub series",
    }
    assert work["bf:summary"] == [
        {"@type": "bf:Summary", "rdfs:label": "A modal jazz album."}
    ]
    # Summary, Source of Description and staff-only notes are not Work notes
    assert work["bf:note"] == [
        {
            "@type": "bf:Note",
            "bf:noteType": "General note",
            "rdfs:label": "Recorded 1959.",
        }
    ]

    graph = _graph(work)
    assert (rdflib.URIRef(work["@id"]), rdflib.RDF.type, BF.Work) in graph


def test_folio_to_bf_work_omits_placeholder_content(reference_data):
    folio_instance = {**FOLIO_INSTANCE, "instanceTypeId": "type-unspecified"}

    work = folio_to_bf_work(folio_instance, reference_data)

    assert "bf:content" not in work


def test_folio_to_bf_instance(reference_data):
    work_uri = "urn:uuid:work"
    bf_instance = folio_to_bf_instance(
        FOLIO_INSTANCE, reference_data, work_uri=work_uri
    )

    assert bf_instance["@type"] == "bf:Instance"
    assert bf_instance["bf:instanceOf"] == {"@id": work_uri}
    assert bf_instance["bf:title"][0]["bf:mainTitle"] == "Kind of blue"
    assert "bf:adminMetadata" not in bf_instance
    # The same value under two identifier types is kept twice
    assert bf_instance["bf:identifiedBy"] == [
        {
            "@type": "bf:Oclc",
            "rdf:value": "(OCoLC)123",
            "bf:source": {"rdfs:label": "OCLC"},
        },
        {"@type": "bf:Upc", "rdf:value": "074646", "bf:source": {"rdfs:label": "UPC"}},
        {
            "@type": "bf:Identifier",
            "rdf:value": "074646",
            "bf:source": {"rdfs:label": "Publisher or distributor number"},
        },
    ]
    assert bf_instance["bf:media"] == [{"rdfs:label": "audio"}]
    assert bf_instance["bf:carrier"] == [{"rdfs:label": "audio disc"}]
    assert bf_instance["bf:issuance"] == {"rdfs:label": "single unit"}
    assert bf_instance["bf:extent"] == [{"rdfs:label": "1 audio disc ; 4 3/4 in."}]
    assert bf_instance["bf:provisionActivity"] == [
        {
            "@type": "bf:Publication",
            "bflc:simpleAgent": "Columbia",
            "bflc:simplePlace": "New York",
            "bflc:simpleDate": "1959",
        },
        {"@type": "bf:CopyrightDate", "bflc:simpleDate": "℗1959"},
    ]
    assert bf_instance["bf:electronicLocator"] == [
        {
            "rdf:value": "https://example.org/kob",
            "rdfs:label": "Liner notes",
            "bf:note": "Stanford only",
        }
    ]
    assert bf_instance["bf:note"] == [
        {
            "@type": "bf:Note",
            "bf:noteType": "Source of Description note",
            "rdfs:label": "Description based on disc.",
        }
    ]

    graph = _graph(bf_instance)
    subject = rdflib.URIRef(bf_instance["@id"])
    assert (subject, rdflib.RDF.type, BF.Instance) in graph
    assert (subject, BF.instanceOf, rdflib.URIRef(work_uri)) in graph


def test_minimal_instance_has_no_empty_properties(reference_data):
    minimal = {"id": "0f0e1d2c-0000-4000-8000-000000000000", "title": "Untitled"}

    work = folio_to_bf_work(minimal, reference_data)
    bf_instance = folio_to_bf_instance(minimal, reference_data)

    assert set(work) == {"@context", "@id", "@type", "bf:title"}
    assert set(bf_instance) == {"@context", "@id", "@type", "bf:title"}


def test_minted_uris_are_deterministic(reference_data):
    first = folio_to_bf_work(FOLIO_INSTANCE, reference_data)["@id"]
    second = folio_to_bf_work(FOLIO_INSTANCE, reference_data)["@id"]
    instance_uri = folio_to_bf_instance(FOLIO_INSTANCE, reference_data)["@id"]

    assert first == second
    assert first != instance_uri


def test_generate_bibframe(mocker):
    mock_client = mocker.MagicMock()
    for prop, rows in REFERENCE_DATA.items():
        setattr(mock_client, prop, rows)
    get_folio_client = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.folio_client",
        return_value=mock_client,
    )

    work, bf_instance = generate_bibframe.function(FOLIO_INSTANCE, "cornell")

    get_folio_client.assert_called_once_with("cornell")

    assert work["@type"] == "bf:Work"
    assert bf_instance["@type"] == "bf:Instance"
    assert bf_instance["bf:instanceOf"] == {"@id": work["@id"]}


@pytest.mark.parametrize(
    "groups,institution",
    [(["stanford"], "stanford"), (["pcc", "cornell"], "cornell")],
)
def test_folio_institution_for(groups, institution):
    assert folio_institution_for(groups) == institution


@pytest.mark.parametrize("groups", [[], ["pcc"], ["stanford", "cornell"]])
def test_folio_institution_for_needs_exactly_one(groups):
    with pytest.raises(ValueError, match="Expected one FOLIO institution"):
        folio_institution_for(groups)


def test_folio_institution(mocker):
    get_user_profile = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.get_user_profile",
        return_value={"username": "cataloger", "groups": ["cornell"]},
    )

    assert folio_institution.function("abc-123") == "cornell"
    get_user_profile.assert_called_once_with("abc-123")


def test_folio_institution_needs_user():
    with pytest.raises(ValueError, match="Missing user_uid"):
        folio_institution.function("")


def test_folio_client_from_connection(mocker):
    connection = mocker.MagicMock(
        host="https://okapi.example.edu",
        login="app_user",
        password="s3cret",
        extra_dejson={"tenant": "sul"},
    )
    get_connection = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.Connection.get",
        return_value=connection,
    )
    mock_folio_client = mocker.patch("ils_middleware.tasks.folio.bibframe.FolioClient")

    client = folio_client("stanford")

    get_connection.assert_called_once_with("stanford_folio")
    mock_folio_client.assert_called_once_with(
        "https://okapi.example.edu", "sul", "app_user", "s3cret"
    )
    assert client is mock_folio_client.return_value


def test_retrieve_folio_instance(mocker):
    mock_client = mocker.MagicMock()
    mock_client.folio_get.return_value = [FOLIO_INSTANCE]
    mocker.patch(
        "ils_middleware.tasks.folio.bibframe.folio_client",
        return_value=mock_client,
    )

    assert retrieve_folio_instance.function(" a123\n", "cornell") == FOLIO_INSTANCE
    mock_client.folio_get.assert_called_once_with(
        "/inventory/instances", key="instances", query='hrid=="a123"'
    )


@pytest.mark.parametrize(
    "found,error",
    [
        ([], "No FOLIO Instance found for a123"),
        ([FOLIO_INSTANCE, FOLIO_INSTANCE], "Expected one FOLIO Instance for a123"),
    ],
)
def test_retrieve_folio_instance_needs_exactly_one(mocker, found, error):
    mock_client = mocker.MagicMock()
    mock_client.folio_get.return_value = found
    mocker.patch(
        "ils_middleware.tasks.folio.bibframe.folio_client",
        return_value=mock_client,
    )

    with pytest.raises(ValueError, match=error):
        retrieve_folio_instance.function("a123", "stanford")


def test_retrieve_folio_instance_already_in_bluecore(mocker):
    mock_client = mocker.MagicMock()
    mock_client.folio_get.return_value = [
        {**FOLIO_INSTANCE, "sourceUri": "https://bcld.info/instances/1234"}
    ]
    mocker.patch(
        "ils_middleware.tasks.folio.bibframe.folio_client",
        return_value=mock_client,
    )

    with pytest.raises(ValueError, match="a123 already is in Blue Core"):
        retrieve_folio_instance.function("a123", "stanford")


def test_ingest_into_bluecore(mocker, reference_data):
    work = folio_to_bf_work(FOLIO_INSTANCE, reference_data)
    bf_instance = folio_to_bf_instance(
        FOLIO_INSTANCE, reference_data, work_uri=work["@id"]
    )
    mocker.patch("ils_middleware.tasks.folio.bibframe.get_bluecore_db")
    mocker.patch("ils_middleware.tasks.folio.bibframe.get_engine")
    current_user_id = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.CURRENT_USER_ID"
    )
    # Stands in for Blue Core minting its own URIs
    save_graph = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.save_graph",
        side_effect=lambda session_maker, graph, **kwargs: graph,
    )

    uri = ingest_into_bluecore.function([work, bf_instance], user_uid="abc-123")

    current_user_id.set.assert_called_once_with("abc-123")
    graph = save_graph.call_args.args[1]
    assert (
        rdflib.URIRef(bf_instance["@id"]),
        BF.instanceOf,
        rdflib.URIRef(work["@id"]),
    ) in graph
    assert uri == bf_instance["@id"]


BF_INSTANCE_URI = "https://bcld.info/instances/1234"


@pytest.fixture
def mock_folio_put(mocker):
    mock_client = mocker.MagicMock()
    # update_source_uri uses the client as a context manager
    mock_client.__enter__.return_value = mock_client
    get_folio_client = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.folio_client",
        return_value=mock_client,
    )
    return get_folio_client, mock_client.folio_put


def test_update_source_uri(mock_folio_put):
    get_folio_client, folio_put = mock_folio_put

    update_source_uri.function(dict(FOLIO_INSTANCE), BF_INSTANCE_URI, "stanford")

    get_folio_client.assert_called_once_with("stanford")
    folio_put.assert_called_once()
    (path,) = folio_put.call_args.args
    assert path == f"/inventory/instances/{FOLIO_INSTANCE['id']}"
    payload = folio_put.call_args.kwargs["payload"]
    assert payload["sourceUri"] == BF_INSTANCE_URI
    assert payload["hrid"] == FOLIO_INSTANCE["hrid"]


def test_update_source_uri_failure_fails_task(mock_folio_put):
    _, folio_put = mock_folio_put
    folio_put.side_effect = httpx.HTTPStatusError(
        "422 Unprocessable Entity",
        request=httpx.Request("PUT", "https://okapi.example.edu"),
        response=httpx.Response(422),
    )

    with pytest.raises(httpx.HTTPStatusError, match="422"):
        update_source_uri.function(dict(FOLIO_INSTANCE), BF_INSTANCE_URI, "cornell")


def test_ingest_into_bluecore_from_marc_branch(mocker):
    mocker.patch("ils_middleware.tasks.folio.bibframe.get_bluecore_db")
    mocker.patch("ils_middleware.tasks.folio.bibframe.get_engine")
    save_graph = mocker.patch(
        "ils_middleware.tasks.folio.bibframe.save_graph",
        side_effect=lambda session_maker, graph, **kwargs: graph,
    )
    graph = rdflib.Graph()
    work = rdflib.URIRef("urn:folio:stanford:a123#Work")
    instance = rdflib.URIRef("urn:folio:stanford:a123#Instance")
    graph.add((work, rdflib.RDF.type, BF.Work))
    graph.add((instance, rdflib.RDF.type, BF.Instance))
    graph.add((instance, BF.instanceOf, work))

    # The JSON branch was skipped, so its output arrives as None
    uri = ingest_into_bluecore.function(
        None, user_uid="", bibframe_rdf_xml=graph.serialize(format="xml")
    )

    assert uri == str(instance)
    saved = save_graph.call_args.args[1]
    assert (instance, BF.instanceOf, work) in saved


def test_ingest_into_bluecore_needs_bibframe():
    with pytest.raises(ValueError, match="No BIBFRAME to ingest"):
        ingest_into_bluecore.function(None, user_uid="", bibframe_rdf_xml=None)
