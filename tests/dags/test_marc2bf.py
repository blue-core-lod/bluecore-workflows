import io
import pathlib

import pymarc
import rdflib

from ils_middleware.dags.marc2bf import marc_to_bibframe_dag

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")

FIXTURE = pathlib.Path(__file__).parents[1] / "fixtures" / "record.xml"

BASE = "http://id.loc.gov/resources"


def test_transform_to_bf_mints_heading_uris():
    """A heading with no $0 keeps a URI minted under the base, not a blank node"""
    (record,) = pymarc.parse_xml_to_array(io.StringIO(FIXTURE.read_text()))
    record.add_ordered_field(pymarc.Field(tag="001", data="a123"))
    record.add_ordered_field(
        pymarc.Field(
            tag="650",
            indicators=pymarc.Indicators(" ", "0"),
            subfields=[pymarc.Subfield("a", "Sagas")],
        )
    )
    marc_xml = pymarc.record_to_xml(record, namespace=True).decode("utf-8")

    transform_to_bf = marc_to_bibframe_dag.get_task("transform_to_bf")
    rdf_xml = transform_to_bf.python_callable(marc_xml, BASE)

    graph = rdflib.Graph()
    graph.parse(data=rdf_xml, format="xml")
    work = rdflib.URIRef(f"{BASE}a123#Work")
    (sagas,) = [
        topic
        for topic in graph.objects(work, BF.subject)
        if str(graph.value(topic, rdflib.RDFS.label)) == "Sagas"
    ]
    assert isinstance(sagas, rdflib.URIRef)
    assert sagas.startswith(f"{BASE}a123#Topic650-")
