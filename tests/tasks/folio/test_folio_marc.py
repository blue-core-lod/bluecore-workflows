import io
import json
import pathlib

import pymarc
import pytest
import rdflib

from ils_middleware.tasks.folio.marc import (
    marc_base_uri,
    marc_json_to_xml,
    marc_to_bibframe,
    retrieve_marc_record,
)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

FIXTURE = pathlib.Path(__file__).parents[2] / "fixtures" / "record.xml"

FOLIO_INSTANCE = {
    "id": "8c9b5c6f-6e2e-4f43-9f4b-3a0b3e4f1d2a",
    "hrid": "a123",
    "source": "MARC",
}


@pytest.fixture
def marc_json() -> dict:
    """The fixture record as SRS stores it, MARC-in-JSON with the HRID in 001"""
    (record,) = pymarc.parse_xml_to_array(io.StringIO(FIXTURE.read_text()))
    record.add_ordered_field(pymarc.Field(tag="001", data="a123"))
    return json.loads(record.as_json())


@pytest.fixture
def mock_srs(mocker, marc_json):
    mock_client = mocker.MagicMock()
    mock_client.folio_get.return_value = {"parsedRecord": {"content": marc_json}}
    get_folio_client = mocker.patch(
        "ils_middleware.tasks.folio.marc.folio_client", return_value=mock_client
    )
    return get_folio_client, mock_client.folio_get


def test_marc_json_to_xml(marc_json):
    marc_xml = marc_json_to_xml(marc_json)

    (record,) = pymarc.parse_xml_to_array(io.StringIO(marc_xml))
    assert record["001"].data == "a123"
    assert record.title.startswith(
        pymarc.parse_xml_to_array(io.StringIO(FIXTURE.read_text()))[0].title
    )


def test_retrieve_marc_record(mock_srs):
    get_folio_client, folio_get = mock_srs

    marc_xml = retrieve_marc_record.function(FOLIO_INSTANCE, "cornell")

    get_folio_client.assert_called_once_with("cornell")
    folio_get.assert_called_once_with(
        f"/source-storage/records/{FOLIO_INSTANCE['id']}/formatted",
        query_params={"idType": "INSTANCE"},
    )
    (record,) = pymarc.parse_xml_to_array(io.StringIO(marc_xml))
    assert record["001"].data == "a123"


def test_retrieve_marc_record_needs_marc_source(mock_srs):
    _, folio_get = mock_srs

    with pytest.raises(ValueError, match="a123 has no MARC record in FOLIO"):
        retrieve_marc_record.function({**FOLIO_INSTANCE, "source": "FOLIO"}, "stanford")
    folio_get.assert_not_called()


def test_marc_to_bibframe(marc_json):
    rdf_xml = marc_to_bibframe.function(marc_json_to_xml(marc_json), "stanford")

    graph = rdflib.Graph()
    graph.parse(data=rdf_xml, format="xml")
    base = marc_base_uri("stanford")
    work = rdflib.URIRef(f"{base}a123#Work")
    instance = rdflib.URIRef(f"{base}a123#Instance")
    assert (work, rdflib.RDF.type, BF.Work) in graph
    assert (instance, rdflib.RDF.type, BF.Instance) in graph
    assert (instance, BF.instanceOf, work) in graph
