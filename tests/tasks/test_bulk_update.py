import types

import pytest
import rdflib
from bluecore_models.namespaces import BF
from bluecore_models.utils.graph import load_jsonld

from ils_middleware.tasks import bulk_update
from ils_middleware.tasks.bulk_update import (
    BulkUpdateError,
    _triples,
    batch_uris,
    check_query,
    merge_reports,
    read_uris,
    summarize,
    update_resources,
)

WORK_URI = "https://bcld.info/works/1234"
INSTANCE_URI = "https://bcld.info/instances/5678"

WORK_DATA = {
    "@id": WORK_URI,
    "@type": "Work",
    "adminMetadata": {
        "@type": "AdminMetadata",
        "assigner": {"@id": "http://id.loc.gov/vocabulary/organizations/dlc"},
    },
    "note": [{"@type": "Note", "note": "internal note"}],
    "title": {"@type": "Title", "mainTitle": "A title"},
}

WORK_DATA_WITH_AGENT = dict(
    WORK_DATA,
    contribution={
        "@type": "Contribution",
        "agent": {"@id": "http://id.loc.gov/authorities/names/n79021164"},
    },
)

DELETE_NOTES = """
PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
DELETE { ?resource bf:note ?note . ?note ?p ?o }
WHERE  { ?resource bf:note ?note . ?note ?p ?o }
"""


def stored(uri=WORK_URI, type_="works", data=None):
    """A stand-in for a resource_base row."""
    return types.SimpleNamespace(uri=uri, type=type_, data=data or dict(WORK_DATA))


@pytest.fixture
def fake_db(mocker):
    """
    Stub out everything that talks to Postgres: the engine, the sessionmaker,
    and save_graph. Returns the save_graph mock so a test can see what was
    written.
    """
    mocker.patch("ils_middleware.tasks.bulk_update.get_engine")
    mocker.patch("ils_middleware.tasks.bulk_update.sessionmaker")
    return mocker.patch("ils_middleware.tasks.bulk_update.save_graph")


@pytest.fixture
def rows(mocker):
    """Let a test say which resource_base rows exist, keyed by uri."""
    resources: dict = {}
    mocker.patch(
        "ils_middleware.tasks.bulk_update._fetch",
        side_effect=lambda session, uris: {
            uri: resources[uri] for uri in uris if uri in resources
        },
    )
    return resources


# --- check_query ------------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        DELETE_NOTES,
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> DELETE WHERE { ?s bf:note ?n }",
        (
            "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
            "INSERT { ?resource bf:note 'x' } WHERE { ?resource a bf:Work }"
        ),
        "INSERT DATA { <https://bcld.info/works/1> <http://id.loc.gov/ontologies/bibframe/note> 'a' }",
        "DELETE DATA { <https://bcld.info/works/1> <http://id.loc.gov/ontologies/bibframe/note> 'a' }",
    ],
)
def test_check_query_allows_update_forms(query):
    assert check_query(query) is not None


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("", "Missing SPARQL update query"),
        ("   ", "Missing SPARQL update query"),
        ("DELETE WHERE { ?s ?p", "Could not parse SPARQL update"),
        ("SELECT * WHERE { ?s ?p ?o }", "Could not parse SPARQL update"),
        ("DROP ALL", "Drop is not allowed"),
        ("DROP GRAPH <http://example.com/g>", "Drop is not allowed"),
        ("CLEAR DEFAULT", "Clear is not allowed"),
        ("LOAD <http://example.com/data.ttl>", "Load is not allowed"),
        ("ADD DEFAULT TO GRAPH <http://example.com/g>", "Add is not allowed"),
        ("MOVE DEFAULT TO GRAPH <http://example.com/g>", "Move is not allowed"),
        ("COPY DEFAULT TO GRAPH <http://example.com/g>", "Copy is not allowed"),
        ("CREATE GRAPH <http://example.com/g>", "Create is not allowed"),
        (
            "WITH <http://example.com/g> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }",
            "WITH is not allowed",
        ),
        (
            "DELETE { GRAPH <http://example.com/g> { ?s ?p ?o } } WHERE { ?s ?p ?o }",
            "GRAPH",
        ),
        (
            "DELETE { ?s ?p ?o } WHERE { GRAPH <http://example.com/g> { ?s ?p ?o } }",
            "GRAPH",
        ),
        (
            "INSERT DATA { GRAPH <http://example.com/g> { <http://x> <http://y> 'z' } }",
            "GRAPH",
        ),
        # SERVICE is the one rejected form that really does reach the network:
        # rdflib evaluates it from inside the WHERE clause, so this would call
        # the endpoint once per resource in the run.
        (
            (
                "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
                "INSERT { ?resource bf:note 'x' } "
                "WHERE { SERVICE <http://example.com/sparql> { ?resource ?p ?o } }"
            ),
            "SERVICE",
        ),
        # USING names the graph the WHERE clause reads from, and rdflib fetches
        # it -- an http url or a local file -- so it is a way into the resource
        # for data from outside. It is a key on the operation, not a node in the
        # WHERE clause, so the algebra walk alone does not catch it.
        (
            (
                "PREFIX ex: <http://example.org/> "
                "INSERT { ?resource ex:injected ?v } "
                "USING <http://example.com/side.ttl> "
                "WHERE { ?resource ex:injected ?v }"
            ),
            "USING is not allowed",
        ),
        (
            (
                "INSERT { ?s <http://x/> 1 } "
                "USING NAMED <http://example.com/g> WHERE { ?s ?p ?o }"
            ),
            "USING is not allowed",
        ),
        # a forbidden operation hiding behind an allowed one
        ("INSERT DATA { <http://x> <http://y> 'z' }; DROP ALL", "Drop is not allowed"),
    ],
)
def test_check_query_rejects(query, message):
    with pytest.raises(BulkUpdateError, match=message):
        check_query(query)


# --- read_uris --------------------------------------------------------------


def test_read_uris(tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(
        f"uri,note\n{WORK_URI},keep this\n\n{INSTANCE_URI},\n{WORK_URI},again\n"
    )

    # the uri column is read by name, the blank line ignored, the note column
    # left alone, and the repeated uri only listed once
    assert read_uris(str(csv_file)) == [WORK_URI, INSTANCE_URI]


def test_read_uris_reads_the_column_by_name(tmp_path):
    """The uri column doesn't have to be first, and its case doesn't matter."""
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"Local ID,URI\na1234,{WORK_URI}\nb5678,{INSTANCE_URI}\n")

    assert read_uris(str(csv_file)) == [WORK_URI, INSTANCE_URI]


def test_read_uris_ignores_a_byte_order_mark(tmp_path):
    """A CSV saved by a spreadsheet often starts with one."""
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"uri\n{WORK_URI}\n", encoding="utf-8-sig")

    assert read_uris(str(csv_file)) == [WORK_URI]


def test_read_uris_requires_a_header(tmp_path):
    """A bare list of URIs is refused: the file has to say what it holds."""
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"{WORK_URI}\n{INSTANCE_URI}\n")

    with pytest.raises(BulkUpdateError, match="has no uri column"):
        read_uris(str(csv_file))


def test_read_uris_names_the_columns_it_found(tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"resource,note\n{WORK_URI},a note\n")

    with pytest.raises(BulkUpdateError, match="no uri column, only note, resource"):
        read_uris(str(csv_file))


def test_read_uris_no_file():
    with pytest.raises(BulkUpdateError, match="Missing CSV file"):
        read_uris("")


def test_read_uris_missing_file(tmp_path):
    with pytest.raises(BulkUpdateError, match="does not exist"):
        read_uris(str(tmp_path / "nope.csv"))


def test_read_uris_empty_file(tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text("")

    with pytest.raises(BulkUpdateError, match="is empty"):
        read_uris(str(csv_file))


def test_read_uris_header_but_no_rows(tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text("uri\n\n")

    with pytest.raises(BulkUpdateError, match="No resource URIs found"):
        read_uris(str(csv_file))


def test_read_uris_not_a_uri(tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"uri\n{WORK_URI}\n1234\n")

    # row 3, as a spreadsheet would number it
    with pytest.raises(BulkUpdateError, match="row 3: 1234 is not a URI"):
        read_uris(str(csv_file))


def test_read_uris_too_many(tmp_path, monkeypatch):
    monkeypatch.setattr(bulk_update, "MAX_RESOURCES", 2)
    csv_file = tmp_path / "uris.csv"
    rows = "".join(f"https://bcld.info/works/{i}\n" for i in range(3))
    csv_file.write_text(f"uri\n{rows}")

    with pytest.raises(BulkUpdateError, match="exceeds the 2 allowed"):
        read_uris(str(csv_file))


# --- batch_uris -------------------------------------------------------------


def test_batch_uris():
    uris = [f"https://bcld.info/works/{i}" for i in range(5)]

    assert batch_uris(uris, 2) == [uris[0:2], uris[2:4], uris[4:5]]
    assert batch_uris(uris, 100) == [uris]


def test_batch_uris_bad_size():
    with pytest.raises(BulkUpdateError, match="at least 1"):
        batch_uris(["https://bcld.info/works/1"], 0)


# --- postconditions ---------------------------------------------------------


def apply(query, data=None, uri=WORK_URI):
    """Run a query against a resource's graph, returning (before, after)."""
    graph = load_jsonld(dict(data or WORK_DATA))
    before = rdflib.Graph()
    before += graph
    graph.update(check_query(query), initBindings={"resource": rdflib.URIRef(uri)})
    return before, graph


def test_postcondition_allows_an_ordinary_change():
    before, after = apply(DELETE_NOTES)

    assert bulk_update._postcondition_failure(WORK_URI, before, after) is None


def test_postcondition_refuses_deleting_the_resource():
    before, after = apply("DELETE WHERE { ?resource ?p ?o }")

    assert (
        bulk_update._postcondition_failure(WORK_URI, before, after)
        == "update would delete the resource"
    )


def test_postcondition_refuses_a_type_change():
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
        "INSERT { ?resource rdf:type bf:Instance } WHERE { ?resource a bf:Work }"
    )

    assert "rdf:type" in bulk_update._postcondition_failure(WORK_URI, before, after)


def test_postcondition_refuses_an_admin_metadata_change():
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "DELETE { ?resource bf:adminMetadata ?admin } "
        "WHERE  { ?resource bf:adminMetadata ?admin }"
    )

    assert "adminMetadata" in bulk_update._postcondition_failure(
        WORK_URI, before, after
    )


def test_postcondition_refuses_rewriting_inside_admin_metadata():
    """
    The provenance is a blank node, so an update can leave the bf:adminMetadata
    arc alone while rewriting what it says.
    """
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "DELETE { ?admin bf:assigner ?assigner } "
        "INSERT { ?admin bf:assigner <http://example.org/forged> } "
        "WHERE  { ?resource bf:adminMetadata ?admin . ?admin bf:assigner ?assigner }"
    )

    # the arc itself is untouched, so this is only caught by comparing the
    # blank node's contents
    assert _triples(before, rdflib.URIRef(WORK_URI), BF.adminMetadata) == _triples(
        after, rdflib.URIRef(WORK_URI), BF.adminMetadata
    )
    assert "adminMetadata" in bulk_update._postcondition_failure(
        WORK_URI, before, after
    )


def test_postcondition_refuses_describing_another_resource():
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "INSERT { <https://bcld.info/works/9999> bf:note 'sneaky' } "
        "WHERE  { ?resource a bf:Work }"
    )

    failure = bulk_update._postcondition_failure(WORK_URI, before, after)
    assert failure == (
        "update would start describing other resources: https://bcld.info/works/9999"
    )


def test_postcondition_refuses_a_relationship_change():
    """
    Asserting bf:hasInstance about another resource re-parents it when
    bluecore-models saves, so a bulk update is not allowed to.
    """
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "INSERT { ?resource bf:hasInstance <https://bcld.info/instances/999> } "
        "WHERE  { ?resource a bf:Work }"
    )

    assert bulk_update._postcondition_failure(WORK_URI, before, after) == (
        "update would change how this resource relates to others: hasInstance"
    )


def test_postcondition_refuses_removing_a_relationship():
    data = dict(WORK_DATA, instanceOf={"@id": INSTANCE_URI})
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "DELETE { ?resource bf:instanceOf ?instance } "
        "WHERE  { ?resource bf:instanceOf ?instance }",
        data=data,
    )

    assert "instanceOf" in bulk_update._postcondition_failure(WORK_URI, before, after)


def test_postcondition_refuses_triples_the_write_would_discard():
    """
    generate_entity_graph drops dcterms and lclocal triples, so a preview
    promising one would be a preview of something that never happens.
    """
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "PREFIX dcterms: <http://purl.org/dc/terms/> "
        "INSERT { ?resource dcterms:modified '2026-09-09' } "
        "WHERE  { ?resource a bf:Work }"
    )

    assert bulk_update._postcondition_failure(WORK_URI, before, after) == (
        "update would add triples Blue Core does not store: modified"
    )


def test_postcondition_refuses_an_excluded_triple_type():
    """bluecore-models strips bf:identifiedBy blank nodes typed bf:OclcNumber."""
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "INSERT { ?resource bf:identifiedBy [ a bf:OclcNumber ; rdf:value '12345' ] } "
        "WHERE  { ?resource a bf:Work }"
    )

    assert "identifiedBy" in bulk_update._postcondition_failure(WORK_URI, before, after)


def test_postcondition_allows_an_ordinary_insert():
    """The new checks don't get in the way of a plain field edit."""
    before, after = apply(
        "PREFIX bf: <http://id.loc.gov/ontologies/bibframe/> "
        "INSERT { ?resource bf:summary [ a bf:Summary ; rdfs:label 'a summary' ] } "
        "WHERE  { ?resource a bf:Work }"
    )

    assert bulk_update._postcondition_failure(WORK_URI, before, after) is None


# --- update_resources -------------------------------------------------------


def test_update_resources_dry_run(fake_db, rows):
    rows[WORK_URI] = stored()

    report = update_resources([WORK_URI], DELETE_NOTES, "postgresql://db")

    assert report["dry_run"] is True
    assert report["processed"] == 1
    assert report["updated"] == [WORK_URI]
    assert report["changes"][0]["removed_count"] == 3
    assert report["changes"][0]["added_count"] == 0
    assert any("internal note" in triple for triple in report["changes"][0]["removed"])
    # nothing is written on a dry run
    fake_db.assert_not_called()


def test_update_resources_applied(fake_db, rows):
    rows[WORK_URI] = stored()

    report = update_resources(
        [WORK_URI], DELETE_NOTES, "postgresql://db", user_uid="uid-1", dry_run=False
    )

    assert report["updated"] == [WORK_URI]
    fake_db.assert_called_once()
    graph = fake_db.call_args.args[1]
    assert (rdflib.URIRef(WORK_URI), BF.note, None) not in graph
    # naming the kind being written is what keeps save_graph to this resource
    # and leaves the descriptions it only references alone
    assert fake_db.call_args.kwargs["primary_class"] == BF.Work


def test_update_resources_applied_instance(fake_db, rows):
    data = dict(WORK_DATA, **{"@id": INSTANCE_URI, "@type": "Instance"})
    rows[INSTANCE_URI] = stored(uri=INSTANCE_URI, type_="instances", data=data)

    report = update_resources(
        [INSTANCE_URI], DELETE_NOTES, "postgresql://db", dry_run=False
    )

    assert report["updated"] == [INSTANCE_URI]
    assert fake_db.call_args.kwargs["primary_class"] == BF.Instance


def test_update_resources_unchanged(fake_db, rows):
    rows[WORK_URI] = stored(
        data={"@id": WORK_URI, "@type": "Work", "title": {"mainTitle": "A title"}}
    )

    report = update_resources(
        [WORK_URI], DELETE_NOTES, "postgresql://db", dry_run=False
    )

    assert report["unchanged"] == [WORK_URI]
    assert report["updated"] == []
    assert report["changes"] == []
    fake_db.assert_not_called()


def test_update_resources_not_found(fake_db, rows):
    report = update_resources([WORK_URI], DELETE_NOTES, "postgresql://db")

    assert report["skipped"] == [{"uri": WORK_URI, "reason": "not found in Blue Core"}]
    assert report["processed"] == 1


def test_update_resources_skips_other_resources(fake_db, rows):
    uri = "http://id.loc.gov/authorities/names/n79021164"
    rows[uri] = stored(uri=uri, type_="other_resources", data={"@id": uri})

    report = update_resources([uri], DELETE_NOTES, "postgresql://db")

    assert report["skipped"] == [
        {
            "uri": uri,
            "reason": "cannot bulk update other_resources (only works, instances and hubs)",
        }
    ]


def test_update_resources_reports_a_refused_change(fake_db, rows):
    rows[WORK_URI] = stored()

    report = update_resources(
        [WORK_URI], "DELETE WHERE { ?resource ?p ?o }", "postgresql://db", dry_run=False
    )

    assert report["updated"] == []
    assert report["skipped"] == [
        {"uri": WORK_URI, "reason": "update would delete the resource"}
    ]
    fake_db.assert_not_called()


def test_update_resources_carries_on_after_an_error(fake_db, rows, caplog):
    fake_db.side_effect = [RuntimeError("deadlock"), None]
    rows[WORK_URI] = stored()
    other = dict(WORK_DATA, **{"@id": INSTANCE_URI, "@type": "Instance"})
    rows[INSTANCE_URI] = stored(uri=INSTANCE_URI, type_="instances", data=other)

    report = update_resources(
        [WORK_URI, INSTANCE_URI], DELETE_NOTES, "postgresql://db", dry_run=False
    )

    assert report["processed"] == 2
    assert report["errors"] == [{"uri": WORK_URI, "error": "deadlock"}]
    assert report["updated"] == [INSTANCE_URI]


def test_update_resources_sets_the_current_user(fake_db, rows, mocker):
    current_user = mocker.patch("ils_middleware.tasks.bulk_update.CURRENT_USER_ID")
    rows[WORK_URI] = stored()

    update_resources([WORK_URI], DELETE_NOTES, "postgresql://db", user_uid="uid-1")

    current_user.set.assert_called_once_with("uid-1")


def test_update_resources_truncates_a_large_diff(fake_db, rows, monkeypatch):
    monkeypatch.setattr(bulk_update, "MAX_DIFF_TRIPLES", 1)
    rows[WORK_URI] = stored()

    report = update_resources([WORK_URI], DELETE_NOTES, "postgresql://db")

    change = report["changes"][0]
    assert change["removed_count"] == 3
    assert len(change["removed"]) == 1
    assert change["truncated"] is True


# --- other resource links --------------------------------------------------


AGENT = "http://id.loc.gov/authorities/names/n79021164"


def test_referenced_others(mocker):
    """The stored description of a referenced authority is read back."""
    row = types.SimpleNamespace(
        uri=AGENT, data={"@id": AGENT, "rdfs:label": "Twain, Mark, 1835-1910"}
    )
    session = mocker.MagicMock()
    session.query.return_value.where.return_value = [row]
    session_maker = mocker.MagicMock()
    session_maker.return_value.__enter__.return_value = session

    others = bulk_update._referenced_others(
        session_maker, load_jsonld(dict(WORK_DATA_WITH_AGENT))
    )

    assert (
        rdflib.URIRef(AGENT),
        rdflib.RDFS.label,
        rdflib.Literal("Twain, Mark, 1835-1910"),
    ) in others


def test_referenced_others_without_references(mocker):
    session_maker = mocker.MagicMock()
    graph = rdflib.Graph()
    graph.add((rdflib.URIRef(WORK_URI), BF.note, rdflib.Literal("a note")))

    assert len(bulk_update._referenced_others(session_maker, graph)) == 0
    session_maker.assert_not_called()


def test_update_resources_restores_other_resource_links(fake_db, rows, mocker):
    """
    save_graph rebuilds a resource's Other Resource links from the graph it is
    given, so the descriptions of the resources it references have to be in
    there or the links are dropped.
    """
    others = rdflib.Graph()
    others.add((rdflib.URIRef(AGENT), rdflib.RDFS.label, rdflib.Literal("Twain")))
    mocker.patch(
        "ils_middleware.tasks.bulk_update._referenced_others", return_value=others
    )
    rows[WORK_URI] = stored(data=dict(WORK_DATA_WITH_AGENT))

    update_resources([WORK_URI], DELETE_NOTES, "postgresql://db", dry_run=False)

    graph = fake_db.call_args.args[1]
    assert (rdflib.URIRef(AGENT), rdflib.RDFS.label, rdflib.Literal("Twain")) in graph


def test_update_resources_leaves_references_alone_on_a_dry_run(fake_db, rows, mocker):
    referenced = mocker.patch("ils_middleware.tasks.bulk_update._referenced_others")
    rows[WORK_URI] = stored(data=dict(WORK_DATA_WITH_AGENT))

    update_resources([WORK_URI], DELETE_NOTES, "postgresql://db", dry_run=True)

    referenced.assert_not_called()


# --- reporting --------------------------------------------------------------


def test_merge_reports():
    merged = merge_reports(
        [
            {
                "dry_run": True,
                "processed": 2,
                "updated": [WORK_URI],
                "unchanged": [],
                "skipped": [{"uri": "https://bcld.info/works/2", "reason": "nope"}],
                "errors": [],
                "changes": [{"uri": WORK_URI}],
            },
            {
                "dry_run": True,
                "processed": 1,
                "updated": [],
                "unchanged": [INSTANCE_URI],
                "skipped": [],
                "errors": [],
                "changes": [],
            },
            None,
        ]
    )

    assert merged["dry_run"] is True
    assert merged["processed"] == 3
    assert merged["updated"] == [WORK_URI]
    assert merged["unchanged"] == [INSTANCE_URI]
    assert len(merged["skipped"]) == 1
    assert merged["changes"] == [{"uri": WORK_URI}]


def test_merge_reports_says_when_something_was_written():
    merged = merge_reports([{"dry_run": False, "processed": 1, "updated": [WORK_URI]}])

    assert merged["dry_run"] is False


def test_summarize_dry_run():
    summary, sections = summarize(
        bulk_update.new_report(dry_run=True) | {"processed": 3}
    )

    assert summary["Mode"] == "dry run -- nothing was written"
    assert summary["Resources processed"] == 3
    assert summary["Resources that would be updated"] == 0
    assert "Changes that would be made" in sections


def test_summarize_applied():
    summary, sections = summarize(
        bulk_update.new_report(dry_run=False) | {"updated": [WORK_URI]}
    )

    assert summary["Mode"] == "applied"
    assert summary["Resources updated"] == 1
    assert "Changes" in sections
