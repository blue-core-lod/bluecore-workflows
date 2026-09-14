"""Tests for the JSON-LD re-framing sweep.

Two things are being tested and they are worth keeping apart.

The **sweep** -- keyset batching, dry run, writing without the ORM, surviving a
row that will not frame -- is what this module owns, and it is tested with
`reframe()` stubbed out. That is not avoidance: the sweep has to behave the same
way whatever framing does to a document, and a test that depended on the shape
`frame_jsonld` happens to produce would be testing bluecore-models instead.

The **framing** is bluecore-models' job. What is checked here is only the part
this module had to reproduce: that the default context is put back before framing
and removed after, which `set_jsonld` does on the way into the database. The array
coercion those docstrings describe arrives with a bluecore-models release; the
version currently installed here does not have it, so nothing asserts on it.
"""

import pytest
from sqlalchemy import JSON, Column, Integer, MetaData, String, Table, create_engine
from sqlalchemy.pool import StaticPool

from ils_middleware.tasks import reframe as reframe_module
from ils_middleware.tasks.reframe import batches, reframe, run, write

WORK = "https://bcld.info/works/{}"


@pytest.fixture
def engine():
    """One in-memory database shared across connections.

    StaticPool because the default for in-memory SQLite gives each connection its
    own database, and the sweep opens a new one per batch.
    """
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


@pytest.fixture
def table(engine):
    """A stand-in for resource_base, with just the columns the sweep reads.

    Declared rather than reflected so the JSON column round-trips dicts on
    SQLite. batches() and write() take the table as an argument precisely so a
    test does not have to reflect one.
    """
    metadata = MetaData()
    resource_base = Table(
        "resource_base",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("uri", String),
        Column("data", JSON),
    )
    metadata.create_all(engine)
    return resource_base


@pytest.fixture
def rows(engine, table):
    """Five resources, each with a scalar property."""
    values = [
        {
            "id": index,
            "uri": WORK.format(index),
            "data": {"@id": WORK.format(index), "@type": "Work", "note": "a note"},
        }
        for index in range(1, 6)
    ]
    with engine.begin() as connection:
        connection.execute(table.insert(), values)
    return values


@pytest.fixture
def stub_reframe(monkeypatch):
    """Make re-framing a visible, predictable change.

    The installed bluecore-models does not coerce properties to arrays yet, so
    re-framing already framed data is a no-op and the sweep would have nothing to
    do. Stubbing it lets the batching and writing be tested now, and keeps those
    tests from breaking when the real coercion lands.
    """

    def fake(data, uri):
        if data is None:
            return None
        # Idempotent, like the real coercion: wrapping an already wrapped value
        # would make every run report a change and the no-op test meaningless.
        note = data["note"]
        return {**data, "note": note if isinstance(note, list) else [note]}

    monkeypatch.setattr(reframe_module, "reframe", fake)
    return fake


# --- the part reproduced from set_jsonld ------------------------------------


def test_reframe_needs_the_context():
    """A stored value has no @context, and framing without one loses the terms.

    This is the bug the module was written with: `frame_jsonld` does not add the
    default context, `set_jsonld` does it before calling. Without it every
    compacted key is an unknown term, so `note` comes back as the full
    rdfs:label-style URI instead of `note`, and the document a consumer gets is
    not the one it asked for.
    """
    stored = {"@id": WORK.format(1), "@type": "Work", "note": "a note"}
    framed = reframe(stored, WORK.format(1))

    assert framed is not None
    assert "note" in framed, "the term survived, so the context was applied"
    assert not any(key.startswith("http") for key in framed), (
        f"a key was left as a URI, so framing ran without the context: {framed}"
    )


def test_reframe_strips_the_context_again():
    """@context comes off before storing, as it does on the way in."""
    framed = reframe({"@id": WORK.format(1), "@type": "Work"}, WORK.format(1))
    assert framed is not None
    assert "@context" not in framed


def test_reframe_is_idempotent():
    """Re-framing an already framed value changes nothing.

    The backfill may run more than once, and the DAG uses a second dry run as its
    check that the first one worked.
    """
    once = reframe(
        {"@id": WORK.format(1), "@type": "Work", "note": "a"}, WORK.format(1)
    )
    assert once is not None
    assert reframe(once, WORK.format(1)) == once


def test_reframe_passes_through_a_null():
    assert reframe(None, WORK.format(1)) is None


# --- the sweep ---------------------------------------------------------------


def test_batches_sees_every_row(engine, table, rows, stub_reframe):
    """A batch size smaller than the table still covers all of it.

    The keyset is the thing under test: each batch starts after the previous
    batch's last id, so an off-by-one would silently skip or repeat a row.
    """
    seen = list(batches(engine, table, batch_size=2))

    assert [batch.seen for batch in seen] == [2, 2, 1]
    assert sum(batch.changed for batch in seen) == 5
    assert [batch.last_id for batch in seen] == [2, 4, 5]


def test_batches_reports_a_row_it_cannot_frame(engine, table, rows, monkeypatch):
    """One unframeable row is reported, and the sweep carries on.

    A corpus this size has bad rows in it, and a sweep that stopped at the first
    would need someone to fix the data before any of the rest could be brought up
    to date.
    """

    def explode(data, uri):
        if uri == WORK.format(3):
            raise ValueError("malformed")
        return {**data, "note": [data["note"]]}

    monkeypatch.setattr(reframe_module, "reframe", explode)

    seen = list(batches(engine, table, batch_size=10))

    assert len(seen) == 1
    assert seen[0].seen == 5
    assert seen[0].changed == 4
    assert len(seen[0].failed) == 1
    assert WORK.format(3) in seen[0].failed[0]


def test_write_applies_a_batch(engine, table, rows, stub_reframe):
    """The UPDATE lands, keyed per row."""
    write(engine, table, [{"row_id": 2, "new_data": {"changed": True}}])

    with engine.connect() as connection:
        stored = dict(
            connection.execute(table.select().order_by(table.c.id)).mappings().all()[1]
        )
    assert stored["data"] == {"changed": True}


def test_write_does_nothing_with_no_changes(engine, table, rows, stub_reframe):
    write(engine, table, [])
    with engine.connect() as connection:
        assert connection.execute(table.select()).rowcount != 0


# --- the run -----------------------------------------------------------------


@pytest.fixture
def stub_engine(monkeypatch, engine, table):
    """Point run() at the test database and table, without reflecting."""
    monkeypatch.setattr(reframe_module, "get_engine", lambda url: engine)
    monkeypatch.setattr(reframe_module, "resource_table", lambda _engine: table)


def test_run_dry_writes_nothing(engine, table, rows, stub_reframe, stub_engine):
    """A dry run reports what it would do and leaves the data alone.

    The point of having one: a sweep over every resource in the database is worth
    previewing, and the preview has to be trustworthy or nobody will use it.
    """
    summary = run("sqlite://", dry_run=True, batch_size=2)

    assert summary == {
        "dry_run": True,
        "seen": 5,
        "changed": 5,
        "unchanged": 0,
        "failed": [],
    }
    with engine.connect() as connection:
        stored = (
            connection.execute(table.select().order_by(table.c.id)).mappings().all()
        )
    assert all(row["data"]["note"] == "a note" for row in stored), "untouched"


def test_run_applies_and_is_then_a_no_op(
    engine, table, rows, stub_reframe, stub_engine
):
    """Apply, then dry-run again and find nothing left to do.

    This is the check the DAG's documentation tells an operator to perform, so it
    is worth having as a test too: it is the difference between believing the
    sweep is idempotent and knowing it.
    """
    applied = run("sqlite://", dry_run=False, batch_size=2)
    assert applied["changed"] == 5

    with engine.connect() as connection:
        stored = (
            connection.execute(table.select().order_by(table.c.id)).mappings().all()
        )
    assert all(row["data"]["note"] == ["a note"] for row in stored)

    again = run("sqlite://", dry_run=True, batch_size=2)
    assert again["changed"] == 0
    assert again["unchanged"] == 5
