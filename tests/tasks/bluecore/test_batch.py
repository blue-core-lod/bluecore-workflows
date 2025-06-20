import pytest
import rdflib

from ils_middleware.tasks.bluecore.batch import is_zip, parse_file_to_graph

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")


def test_is_zip():
    assert is_zip("test.zip")
    assert is_zip("test.tar.gz")
    assert not is_zip("test.txt")


def test_parse_file_to_graph(test_graph, tmp_path):
    test_graph_file = tmp_path / "bf-record.jsonld"

    with test_graph_file.open("w+") as fo:
        fo.write(test_graph.serialize(format="json-ld"))

    resources = parse_file_to_graph(str(test_graph_file))

    assert resources[0]["class"].startswith("Work")
    # TODO when we have BlueCore URL schema, should replace Sinopia fixture URLs with BlueCore URLs
    assert resources[0]["uri"].startswith(
        "https://api.stage.sinopia.io/resource/c96d8b55-e0ac-48a5-9a9b-b0684758c99e"
    )
    assert resources[1]["bibframe_resource_uri"] == str(resources[0]["uri"])
    assert resources[13]["class"].startswith("Instance")
    assert resources[14]["bibframe_resource_uri"] == str(resources[13]["uri"])

    instance_graph = rdflib.Graph()
    instance_graph.parse(data=resources[13]["resource"], format="json-ld")

    dimensions = instance_graph.value(
        subject=rdflib.URIRef(
            "https://api.stage.sinopia.io/resource/b0319047-acd0-4f30-bd8b-98e6c1bac6b0"
        ),
        predicate=BF.dimensions,
    )

    assert str(dimensions).startswith("30 cm by 15 cm")


def test_parse_file_to_graph_missing_file():
    with pytest.raises(
        ValueError, match="test.txt does not exist or Airflow cannot read"
    ):
        parse_file_to_graph("test.txt")


def test_parse_file_to_graph_work_bnode(tmp_path, caplog):
    graph = rdflib.Graph()
    work_bnode = rdflib.BNode()
    graph.add((work_bnode, rdflib.RDF.type, BF.Work))

    record_path = tmp_path / "bf-work-bnode.jsonld"

    with record_path.open("w+") as fo:
        fo.write(graph.serialize(format="json-ld"))

    resources = parse_file_to_graph(str(record_path))

    assert resources == []
    assert "is a blank node, not processing" in caplog.text


def test_parse_file_to_graph_instance_bnode(tmp_path, caplog):
    graph = rdflib.Graph()
    instance_bnode = rdflib.BNode()
    graph.add((instance_bnode, rdflib.RDF.type, BF.Instance))

    record_path = tmp_path / "bf-instance-bnode.jsonld"

    with record_path.open("w+") as fo:
        fo.write(graph.serialize(format="json-ld"))

    resources = parse_file_to_graph(str(record_path))

    assert resources == []
    assert f"{instance_bnode} is a blank node" in caplog.text
