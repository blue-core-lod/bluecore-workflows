import os
import pytest
import rdflib

from ils_middleware.tasks.bluecore.batch import (
    delete_upload,
    is_zip,
    parse_file_to_graph,
)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")


def test_delete_upload(tmp_path):
    upload_path = tmp_path / "sub_dir"
    os.mkdir(upload_path)
    upload_file = upload_path / "bf-record.jsonld"
    upload_file.touch()
    upload_file2 = upload_path / "bf-record2.jsonld"
    upload_file2.touch()

    assert upload_path.exists()
    assert upload_file.exists()
    assert upload_file2.exists()

    # It should delete the parent directory only if it's empty and remove_empty_parent is True
    delete_upload(str(upload_file))
    assert upload_path.exists()
    assert not upload_file.exists()

    # It should keep the parent directory as remove_empty_parent is not set
    delete_upload(str(upload_file2))
    assert upload_path.exists()
    assert not upload_file2.exists()

    # Delete the second file again with the remove_empty_parent flag set to True
    upload_file2.touch()
    delete_upload(str(upload_file2), remove_empty_parent=True)
    assert not upload_path.exists()


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
    assert resources[0]["uri"].startswith("https://bcld.info/works")
    expected_uuid = resources[0]["uri"].split("/")[-1]
    assert resources[0]["uuid"] == expected_uuid
    assert resources[1]["bibframe_resource_uri"] == str(resources[0]["uri"])
    assert resources[13]["class"].startswith("Instance")
    assert resources[14]["bibframe_resource_uri"] == str(resources[13]["uri"])

    instance_graph = rdflib.Graph()
    instance_graph.parse(data=resources[13]["resource"][0], format="json-ld")
    instance = instance_graph.value(predicate=rdflib.RDF.type, object=BF.Instance)

    dimensions = instance_graph.value(
        subject=instance,
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
