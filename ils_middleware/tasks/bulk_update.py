"""
Apply a SPARQL UPDATE to a list of Blue Core Works and Instances.

The DAG hands us a CSV of Blue Core resource URIs and one SPARQL UPDATE. For
each URI we rebuild the stored resource as an rdflib Graph, run the update
against that graph alone, and -- unless this is a dry run -- save it back
through bluecore-models, which records a version for every write.

Two things keep a thousand-record run from being a thousand-record accident:
the query is checked before anything is touched (see check_query), and every
resource's post-update graph is checked before it is saved (see
_postcondition_failure). Anything that fails either check is reported rather
than written.
"""

import copy
import csv
import logging
import os
import pathlib

import rdflib
from bluecore_models.bluecore_graph import save_graph
from bluecore_models.models import OtherResource, ResourceBase
from bluecore_models.models.version import CURRENT_USER_ID
from bluecore_models.namespaces import BF, RDF
from bluecore_models.utils.graph import load_jsonld
from rdflib import URIRef
from rdflib.plugins.sparql import prepareUpdate
from rdflib.plugins.sparql.sparql import Update
from sqlalchemy.orm import sessionmaker

from ils_middleware.tasks.bluecore import get_engine

logger = logging.getLogger(__name__)

# The kinds of resource a bulk update can write, by the polymorphic type
# discriminator stored on the row, mapped to the primary_class save_graph needs
# for that kind. Other Resources are missing on purpose: they are external
# descriptions Blue Core doesn't author (and save_graph can't write one on its
# own -- it only finds them by walking Works and Instances).
PRIMARY_CLASSES = {
    "works": BF.Work,
    "instances": BF.Instance,
    "hubs": BF.Hub,
}

# The SPARQL update forms we allow. Everything else in the grammar either
# targets a named graph (ADD, MOVE, COPY, CREATE, DROP, CLEAR) or pulls in data
# from elsewhere (LOAD), neither of which means anything when the graph is a
# single stored resource -- but rdflib will happily execute them.
ALLOWED_OPERATIONS = frozenset({"Modify", "DeleteWhere", "InsertData", "DeleteData"})

# Algebra nodes that reach outside the resource's own graph.
FORBIDDEN_NODES = frozenset({"Graph", "Service", "ServiceGraphPattern"})

# The most resources one run will accept, per the bulk operations epic. A
# too-large CSV fails in setup rather than part way through the fan-out.
MAX_RESOURCES = int(os.environ.get("BLUECORE_BULK_UPDATE_MAX", "1000"))

# Resources per mapped task.
DEFAULT_BATCH_SIZE = int(os.environ.get("BLUECORE_BULK_UPDATE_BATCH_SIZE", "100"))

# How many added and removed triples to list per resource in the report. The
# reports travel through XCom, so a dry run of a thousand resources with an
# unbounded diff each would put megabytes of triples in the Airflow database.
# This is a preview: the counts are always exact, the listing is a sample.
MAX_DIFF_TRIPLES = 10

# Column names a CSV may use for the URI column, so a spreadsheet exported with
# a header row works without editing.
URI_HEADERS = frozenset({"uri", "url", "resource", "resource_uri"})


class BulkUpdateError(Exception):
    """The run can't proceed: a bad query, a bad CSV, or nothing to do."""


def check_query(query: str) -> Update:
    """
    Parse a SPARQL UPDATE and refuse the forms that have no business running
    against a single stored resource. Returns the prepared update, which the
    caller reuses across the resources in its batch.

    The check walks the parsed algebra rather than matching on the query text,
    so it can't be talked around with comments, casing or whitespace.
    """
    if not query or not query.strip():
        raise BulkUpdateError("Missing SPARQL update query")

    try:
        prepared = prepareUpdate(query)
    except Exception as error:
        raise BulkUpdateError(f"Could not parse SPARQL update: {error}") from error

    for operation in prepared.algebra:
        if operation.name not in ALLOWED_OPERATIONS:
            raise BulkUpdateError(
                f"{operation.name} is not allowed in a bulk update: use "
                "INSERT/DELETE ... WHERE, DELETE WHERE, INSERT DATA or DELETE DATA"
            )
        # dict.get, not operation.get: rdflib's CompValue.get returns the key
        # itself for a key it doesn't have.
        if dict.get(operation, "withClause") is not None:
            raise BulkUpdateError(
                "WITH is not allowed in a bulk update: the update always applies "
                "to the resource being updated"
            )
        _check_no_named_graphs(operation)

    return prepared


def _check_no_named_graphs(node, seen: set[int] | None = None) -> None:
    """
    Raise if any part of an operation names a graph or a remote service.

    A GRAPH clause survives the operation-name check -- `DELETE { GRAPH <g> {
    ?s ?p ?o } } WHERE { ... }` is still a Modify -- so the quads it produces
    and the Graph nodes in its WHERE clause have to be found by walking.
    """
    if seen is None:
        seen = set()
    if id(node) in seen:
        return
    seen.add(id(node))

    name = getattr(node, "name", None)
    if name in FORBIDDEN_NODES:
        raise BulkUpdateError(
            "GRAPH and SERVICE are not allowed in a bulk update: the update "
            "applies to the resource's own triples"
        )

    if isinstance(node, dict):
        for key, value in node.items():
            # quads is a mapping of graph uri -> triples; anything in it came
            # from a GRAPH clause.
            if key == "quads" and value:
                raise BulkUpdateError(
                    "GRAPH is not allowed in a bulk update: the update applies "
                    "to the resource's own triples"
                )
            _check_no_named_graphs(value, seen)
    elif isinstance(node, (list, tuple, set)):
        for value in node:
            _check_no_named_graphs(value, seen)


def read_uris(csv_file: str) -> list[str]:
    """
    Read the resource URIs from a CSV, which may or may not have a header row.
    Only the first column is read; anything else in the row (a note, a local
    identifier) is left alone. Duplicates are dropped, keeping the first
    occurrence, so a resource is never updated twice in one run.
    """
    if not csv_file:
        raise BulkUpdateError("Missing CSV file")

    csv_path = pathlib.Path(csv_file)
    if not csv_path.exists():
        raise BulkUpdateError(f"{csv_path} does not exist")

    uris: list[str] = []
    seen: set[str] = set()
    with csv_path.open(newline="") as fh:
        for row in csv.reader(fh):
            if not row:
                continue
            value = row[0].strip()
            if not value or value.lower() in URI_HEADERS:
                continue
            if not value.startswith("http"):
                raise BulkUpdateError(f"{value} is not a URI")
            if value in seen:
                logger.info(f"skipping duplicate {value}")
                continue
            seen.add(value)
            uris.append(value)

    if not uris:
        raise BulkUpdateError(f"No resource URIs found in {csv_path}")
    if len(uris) > MAX_RESOURCES:
        raise BulkUpdateError(
            f"{len(uris):,} resources exceeds the {MAX_RESOURCES:,} allowed in "
            "one bulk update"
        )
    return uris


def batch_uris(
    uris: list[str], batch_size: int = DEFAULT_BATCH_SIZE
) -> list[list[str]]:
    """Split the URIs into batches, one per mapped task."""
    if batch_size < 1:
        raise BulkUpdateError(f"batch size must be at least 1, got {batch_size}")
    return [uris[i : i + batch_size] for i in range(0, len(uris), batch_size)]


def _fetch(session, uris: list[str]) -> dict[str, ResourceBase]:
    """
    Look up a batch's resources in one query, keyed by uri, rather than a query
    per resource.
    """
    return {
        resource.uri: resource
        for resource in session.query(ResourceBase).where(ResourceBase.uri.in_(uris))
    }


def _referenced_others(
    session_maker: sessionmaker, graph: rdflib.Graph
) -> rdflib.Graph:
    """
    The stored descriptions of the Other Resources a graph refers to.

    save_graph rebuilds a Work's or Instance's links to its Other Resources from
    the graph it is handed, and it only counts a resource as an Other Resource if
    that graph says something about it. An ingest record or an editor payload
    carries those descriptions inline, so this works. A graph rebuilt from a
    stored resource does not: an authority Blue Core holds lives in its own row,
    and the resource that references it keeps only the reference. Saving such a
    graph as it stands drops every Other Resource link the resource has, so put
    the descriptions of the ones we hold back first.

    Only resources already in the database are added, and only their stored
    description -- so this restores links without inventing anything. A reference
    to a URI Blue Core doesn't hold as an Other Resource stays a bare reference,
    as it was before the update.

    A workaround: the Other Resources a Work or Instance links to are the ones
    its own JSON-LD cites, which we already store. Delete this and its call in
    _update_resource once bluecore-models maintains the links from those
    citations rather than from what the graph it is handed happens to describe.
    """
    referenced = {str(o) for o in graph.objects() if isinstance(o, URIRef)}
    others = rdflib.Graph()
    if not referenced:
        return others
    with session_maker() as session:
        rows = session.query(OtherResource).where(OtherResource.uri.in_(referenced))
        for row in rows:
            others += load_jsonld(copy.deepcopy(row.data))
    return others


def _triples(graph: rdflib.Graph, subject: URIRef, predicate) -> set:
    return set(graph.triples((subject, predicate, None)))


def _uri_subjects(graph: rdflib.Graph) -> set[URIRef]:
    return {s for s in graph.subjects() if isinstance(s, URIRef)}


def _postcondition_failure(
    uri: str, before: rdflib.Graph, after: rdflib.Graph
) -> str | None:
    """
    Describe what makes the updated graph unsafe to save, or return None if it
    is fine.

    A bulk update is meant to change statements about resources the record
    already describes. It is not meant to delete the resource, change what kind
    of thing it is, rewrite the provenance Blue Core keeps about it, or start
    describing some resource that wasn't in the record -- that last one matters
    because save_graph authoritatively writes every resource of the primary kind
    it finds in the graph, so an injected subject would overwrite whatever that
    URI already holds.
    """
    subject = URIRef(uri)

    if (subject, None, None) not in after:
        return "update would delete the resource"

    if _triples(before, subject, RDF.type) != _triples(after, subject, RDF.type):
        return "update would change the resource's rdf:type"

    if _triples(before, subject, BF.adminMetadata) != _triples(
        after, subject, BF.adminMetadata
    ):
        return "update would change the resource's bf:adminMetadata"

    introduced = _uri_subjects(after) - _uri_subjects(before)
    if introduced:
        described = ", ".join(sorted(str(s) for s in introduced))
        return f"update would start describing other resources: {described}"

    return None


def _diff(added: set, removed: set) -> dict:
    """Summarize a resource's change for the report."""

    def listing(triples: set) -> list[str]:
        return [
            " ".join(term.n3() for term in triple)
            for triple in sorted(triples, key=str)[:MAX_DIFF_TRIPLES]
        ]

    return {
        "added_count": len(added),
        "removed_count": len(removed),
        "added": listing(added),
        "removed": listing(removed),
        "truncated": len(added) > MAX_DIFF_TRIPLES or len(removed) > MAX_DIFF_TRIPLES,
    }


def new_report(dry_run: bool) -> dict:
    """An empty report, so every caller agrees on the shape."""
    return {
        "dry_run": dry_run,
        "processed": 0,
        "updated": [],
        "unchanged": [],
        "skipped": [],
        "errors": [],
        "changes": [],
    }


def update_resources(
    uris: list[str],
    query: str,
    bluecore_db: str,
    user_uid: str | None = None,
    dry_run: bool = True,
) -> dict:
    """
    Apply the SPARQL update to each resource in this batch and return a report
    of what happened. A resource that can't be updated is reported and the batch
    carries on -- one bad resource in a thousand shouldn't fail the run.

    When dry_run is True nothing is written: every resource that would change is
    reported with the triples the update adds and removes, which is the preview
    of a bulk operation before it is committed.
    """
    prepared = check_query(query)
    report = new_report(dry_run)

    if user_uid:
        # so the version bluecore-models writes for each update records who
        # asked for it (see bluecore_models.utils.db.add_version)
        CURRENT_USER_ID.set(user_uid)
        logger.info("Using CURRENT_USER_ID: %s", user_uid)

    bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")
    session_maker = sessionmaker(bind=get_engine(bluecore_db))

    # Read the batch's resources up front and close the session: save_graph
    # opens (and commits) its own, and holding a read transaction open for the
    # length of the batch would keep an idle transaction on the database.
    with session_maker() as session:
        resources = {
            uri: (resource.type, copy.deepcopy(resource.data))
            for uri, resource in _fetch(session, uris).items()
        }

    for uri in uris:
        report["processed"] += 1
        try:
            _update_resource(
                uri, resources.get(uri), prepared, session_maker, bc_url, report
            )
        except Exception as error:  # noqa: BLE001 -- one bad resource shouldn't abort the batch
            logger.error(f"Error {error} for {uri}")
            report["errors"].append({"uri": uri, "error": str(error)})

    logger.info(
        f"{'previewed' if dry_run else 'updated'} {len(report['updated']):,} of "
        f"{report['processed']:,} resources "
        f"({len(report['unchanged']):,} unchanged, "
        f"{len(report['skipped']):,} skipped, {len(report['errors']):,} errors)"
    )
    return report


def _update_resource(
    uri: str,
    stored: tuple[str, dict] | None,
    prepared: Update,
    session_maker: sessionmaker,
    bc_url: str,
    report: dict,
) -> None:
    """Update one resource, recording the outcome in the report."""
    if stored is None:
        report["skipped"].append({"uri": uri, "reason": "not found in Blue Core"})
        return

    resource_type, data = stored
    primary_class = PRIMARY_CLASSES.get(resource_type)
    if primary_class is None:
        report["skipped"].append(
            {
                "uri": uri,
                "reason": (
                    f"cannot bulk update {resource_type} "
                    "(only works, instances and hubs)"
                ),
            }
        )
        return

    graph = load_jsonld(data)
    before = rdflib.Graph()
    before += graph

    graph.update(prepared, initBindings={"resource": URIRef(uri)})

    added = set(graph) - set(before)
    removed = set(before) - set(graph)
    if not added and not removed:
        report["unchanged"].append(uri)
        return

    failure = _postcondition_failure(uri, before, graph)
    if failure:
        logger.warning(f"refusing to update {uri}: {failure}")
        report["skipped"].append({"uri": uri, "reason": failure})
        return

    change = {"uri": uri, **_diff(added, removed)}

    if not report["dry_run"]:
        # Put the referenced Other Resources back into the graph so that saving
        # it doesn't drop this resource's links to them. Added after the diff and
        # the checks above, which are about the resource's own triples.
        graph += _referenced_others(session_maker, graph)
        save_graph(
            session_maker,
            graph,
            namespace=bc_url,
            primary_class=primary_class,
            # A bulk update rewrites the listed resource. Everything it links to
            # is only referenced, so leave those descriptions alone.
            update_other_resources=False,
            source=f"bulk_update:{uri}",
        )
        logger.info(f"updated {uri}")

    report["updated"].append(uri)
    report["changes"].append(change)


def summarize(report: dict) -> tuple[dict, dict]:
    """
    Turn a report into the headline counts and detail tables the report writer
    renders. The counts are the bulk reporting metrics asked for in
    https://github.com/blue-core-lod/bluecore-workflows/issues/30 -- minus
    "created", since a bulk update only ever rewrites resources that already
    exist.
    """
    dry_run = report.get("dry_run", False)
    summary = {
        "Mode": "dry run -- nothing was written" if dry_run else "applied",
        "Resources processed": report["processed"],
        ("Resources that would be updated" if dry_run else "Resources updated"): len(
            report["updated"]
        ),
        "Resources unchanged": len(report["unchanged"]),
        "Resources skipped": len(report["skipped"]),
        "Errors": len(report["errors"]),
    }
    sections = {
        "Skipped": report["skipped"],
        "Errors": report["errors"],
        ("Changes that would be made" if dry_run else "Changes"): report["changes"],
    }
    return summary, sections


def merge_reports(reports: list[dict]) -> dict:
    """Combine the mapped tasks' reports into the report for the whole run."""
    reports = [report for report in reports if report]
    # every batch of a run gets the same dry_run, but say it this way round so
    # the merged report only claims nothing was written when nothing was.
    merged = new_report(dry_run=all(report.get("dry_run", False) for report in reports))
    for report in reports:
        merged["processed"] += report.get("processed", 0)
        for key in ("updated", "unchanged", "skipped", "errors", "changes"):
            merged[key].extend(report.get(key, []))
    return merged
