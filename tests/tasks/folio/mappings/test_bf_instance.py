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
    results = [row for row in test_graph.query(sparql)]
    notes = [row[0] for row in results]
    note_types = {str(row[0]): row[1] for row in results}

    assert len(str(notes[0])) == 50
    assert len(str(notes[1])) == 90

    assert str(
        note_types["Includes bibliographical references (page 117-128)"]
    ).startswith("http://id.loc.gov/vocabulary/mnotetype/biblio")
    assert (
        note_types[
            "Description based on online resource (Stanford Digital Repository, "
            "viewed October 1, 2021)"
        ]
        is None
    )


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


@typing.no_type_check
def test_cataloged_date(test_graph: rdflib.Graph):
    sparql = bf_instance_map.cataloged_date.format(bf_instance=uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("2021-10-01")


@typing.no_type_check
def test_alternative_title_variant(test_graph: rdflib.Graph):
    sparql = bf_instance_map.alternative_title.format(
        bf_instance=uri, bf_class="bf:VariantTitle"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Scrivere Islam")


@typing.no_type_check
def test_alternative_title_abbreviated(test_graph: rdflib.Graph):
    sparql = bf_instance_map.alternative_title.format(
        bf_instance=uri, bf_class="bf:AbbreviatedTitle"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Islam writing")


@typing.no_type_check
def test_alternative_title_parallel(test_graph: rdflib.Graph):
    sparql = bf_instance_map.alternative_title.format(
        bf_instance=uri, bf_class="bf:ParallelTitle"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Writing about Islam")


@typing.no_type_check
def test_electronic_locator(test_graph: rdflib.Graph):
    sparql = bf_instance_map.electronic_locator.format(bf_instance=uri)
    results = [row for row in test_graph.query(sparql)]

    urls = [str(row[0]) for row in results]
    assert any(url.startswith("https://purl.stanford.edu/mf283yt5578") for url in urls)
    assert any(
        url.startswith("https://phaidra.cab.unipd.it/detail/o:445140") for url in urls
    )


@typing.no_type_check
def test_publication_frequency(test_graph: rdflib.Graph):
    sparql = bf_instance_map.publication_frequency.format(bf_instance=uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("annual")


@typing.no_type_check
def test_publication_range(test_graph: rdflib.Graph):
    sparql = bf_instance_map.publication_range.format(bf_instance=uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]) == "Vol. 1"
    assert str(results[0][1]) == "Vol. 10"
