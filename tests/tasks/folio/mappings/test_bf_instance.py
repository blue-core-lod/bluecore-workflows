import typing

import pytest  # noqa: F401
import rdflib

import ils_middleware.tasks.folio.mappings.bf_instance as bf_instance_map

uri = "https://api.stage.sinopia.io/resource/b0319047-acd0-4f30-bd8b-98e6c1bac6b0"
bf_agent_uri = "https://api.stage.sinopia.io/resource/bf-agent-instance"


@typing.no_type_check
def test_isbn(test_graph: rdflib.Graph):
    sparql = bf_instance_map.identifier.format(bf_instance=uri, bf_class="bf:Isbn")

    isbns = [row[0] for row in test_graph.query(sparql)]

    assert str(isbns[0]).startswith("9788869694110")
    assert str(isbns[1]).startswith("9788869694103")


@typing.no_type_check
def test_instance_format_id(test_graph: rdflib.Graph):
    sparql = bf_instance_map.instance_format_id.format(bf_instance=uri)
    instance_formats = [row for row in test_graph.query(sparql)]

    assert str(instance_formats[0][0]).startswith("computer")
    assert str(instance_formats[0][1]).startswith("online resource")


@typing.no_type_check
def test_local_identifier(test_graph: rdflib.Graph):
    sparql = bf_instance_map.local_identifier.format(bf_instance=uri)
    local_idents = [row[0] for row in test_graph.query(sparql)]

    assert str(local_idents[0]).startswith("1272909598")


@typing.no_type_check
def test_mode_of_issuance(test_graph: rdflib.Graph):
    sparql = bf_instance_map.mode_of_issuance.format(bf_instance=uri)
    modes = [row[0] for row in test_graph.query(sparql)]

    assert str(modes[0]).startswith("single unit")


@typing.no_type_check
def test_note(test_graph: rdflib.Graph):
    sparql = bf_instance_map.note.format(bf_instance=uri)
    notes = [row[0] for row in test_graph.query(sparql)]

    assert len(str(notes[0])) == 50
    assert len(str(notes[1])) == 90


@typing.no_type_check
def test_physical_description(test_graph: rdflib.Graph):
    sparql = bf_instance_map.physical_description.format(bf_instance=uri)
    physical_descriptions = [row for row in test_graph.query(sparql)]

    assert str(physical_descriptions[0][0]).startswith("1 online resource (128 pages)")
    assert str(physical_descriptions[0][1]).startswith("30 cm by 15 cm")


@typing.no_type_check
def test_publication(test_graph: rdflib.Graph):
    sparql = bf_instance_map.publication.format(bf_instance=uri)
    publications = [row for row in test_graph.query(sparql)]

    assert str(publications[0][0]).startswith("Edizioni Ca'Foscari")
    assert str(publications[0][1]).startswith("2020")
    assert str(publications[0][2]).startswith("Venice (Italy)")


@typing.no_type_check
def test_publication_bf_agent(test_graph: rdflib.Graph):
    sparql = bf_instance_map.publication.format(bf_instance=bf_agent_uri)
    publications = [row for row in test_graph.query(sparql)]

    assert str(publications[0][0]).startswith("Oxford University Press")
    assert str(publications[0][1]).startswith("2021")
    assert str(publications[0][2]).startswith("Oxford (England)")


@typing.no_type_check
def test_main_title(test_graph: rdflib.Graph):
    sparql = bf_instance_map.title.format(bf_instance=uri, bf_class="bf:Title")
    titles = [row for row in test_graph.query(sparql)]

    assert str(titles[0][0]).startswith("Scrivere di Islam")
    assert str(titles[0][1]).startswith("raccontere la diaspora")


@typing.no_type_check
def test_parallel_title(test_graph: rdflib.Graph):
    sparql = bf_instance_map.title.format(bf_instance=uri, bf_class="bf:ParallelTitle")
    titles = [row for row in test_graph.query(sparql)]

    assert str(titles[0][0]).startswith("Writing about Islam")
    assert str(titles[0][1]).startswith("narrating a diaspora")
