import typing
import pytest  # noqa: F401
import rdflib

import ils_middleware.tasks.folio.mappings.bf_work as bf_work_map


work_uri = "https://api.stage.sinopia.io/resource/c96d8b55-e0ac-48a5-9a9b-b0684758c99e"


@typing.no_type_check
def test_contributor_author_person(test_graph: rdflib.Graph):
    sparql = bf_work_map.contributor.format(bf_work=work_uri, bf_class="bf:Person")

    contributors = [row for row in test_graph.query(sparql)]

    assert str(contributors[0][0]).startswith("Ramzanali Fazel, Shirin")
    assert str(contributors[0][1]).startswith("author")


@typing.no_type_check
def test_edition(test_graph: rdflib.Graph):
    sparql = bf_work_map.editions.format(bf_work=work_uri)

    editions = [row[0] for row in test_graph.query(sparql)]

    assert str(editions[0]).startswith("1st edition")


@typing.no_type_check
def test_instance_type_id(test_graph: rdflib.Graph):
    sparql = bf_work_map.instance_type_id.format(bf_work=work_uri)

    type_idents = [row[0] for row in test_graph.query(sparql)]

    assert str(type_idents[0]).startswith("Text")


@typing.no_type_check
def test_language(test_graph: rdflib.Graph):
    sparql = bf_work_map.language.format(bf_work=work_uri)

    languages = [row for row in test_graph.query(sparql)]

    assert str(languages[0][0]).startswith("http://id.loc.gov/vocabulary/languages/ita")


@typing.no_type_check
def test_primary_contributor(test_graph: rdflib.Graph):
    sparql = bf_work_map.primary_contributor.format(
        bf_work=work_uri, bf_class="bf:Person"
    )

    primary_contributors = [row for row in test_graph.query(sparql)]

    assert str(primary_contributors[0][0]).startswith("Brioni, Simone")
    assert str(primary_contributors[0][1]).startswith("author")

    assert str(primary_contributors[1][0]).startswith("Blow, C. Joe")
    assert str(primary_contributors[1][1]).startswith("author")


@typing.no_type_check
def test_subject(test_graph: rdflib.Graph):
    sparql = bf_work_map.subject.format(bf_work=work_uri)

    subjects = [row for row in test_graph.query(sparql)]

    assert len(subjects) == 3


@typing.no_type_check
def test_genre(test_graph: rdflib.Graph):
    sparql = bf_work_map.genre.format(bf_work=work_uri)

    genres = [row for row in test_graph.query(sparql)]

    assert str(genres[0][0]).startswith("Informational works")


@typing.no_type_check
def test_classification_lcc(test_graph: rdflib.Graph):
    sparql = bf_work_map.classification.format(
        bf_work=work_uri, bf_class="bf:ClassificationLcc"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("BP52.5")


@typing.no_type_check
def test_classification_ddc(test_graph: rdflib.Graph):
    sparql = bf_work_map.classification.format(
        bf_work=work_uri, bf_class="bf:ClassificationDdc"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("297.09451")


@typing.no_type_check
def test_classification_nlm(test_graph: rdflib.Graph):
    sparql = bf_work_map.classification.format(
        bf_work=work_uri, bf_class="bf:ClassificationNlm"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("BP52")


@typing.no_type_check
def test_series_controlled(test_graph: rdflib.Graph):
    sparql = bf_work_map.series_controlled.format(bf_work=work_uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Diaspore")


@typing.no_type_check
def test_series_uncontrolled(test_graph: rdflib.Graph):
    sparql = bf_work_map.series_uncontrolled.format(bf_work=work_uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Italian studies series")


@typing.no_type_check
def test_alternative_title_variant_work(test_graph: rdflib.Graph):
    sparql = bf_work_map.alternative_title.format(
        bf_work=work_uri, bf_class="bf:VariantTitle"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Islam writing work variant")


@typing.no_type_check
def test_summary(test_graph: rdflib.Graph):
    sparql = bf_work_map.summary.format(bf_work=work_uri)
    results = [row[0] for row in test_graph.query(sparql)]

    assert str(results[0]).startswith('"Scrivere di Islam')


@typing.no_type_check
def test_note(test_graph: rdflib.Graph):
    sparql = bf_work_map.note.format(bf_work=work_uri)
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("In Italian.")
    assert str(results[0][1]).startswith("http://id.loc.gov/vocabulary/mnotetype/lang")


@typing.no_type_check
def test_alternative_title_abbreviated_work(test_graph: rdflib.Graph):
    sparql = bf_work_map.alternative_title.format(
        bf_work=work_uri, bf_class="bf:AbbreviatedTitle"
    )
    results = [row for row in test_graph.query(sparql)]

    assert str(results[0][0]).startswith("Islam wr.")
