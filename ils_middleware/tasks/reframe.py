"""Re-serialise every framed resource's stored JSON-LD, without changing what it says.

A resource's stored `data` is whatever `set_jsonld` wrote on the day it was
written. When the bluecore-models JSON-LD context changes we want to be able to
upgrade our data to use it.

Profiles are not included: they share `resource_base` with the rest but are
never framed on the way in either. See REFRAMABLE_TYPES.

Three things follow from re-framing changing no triples, and they shape the whole
module.

**It writes through SQLAlchemy Core, not the ORM.** `after_update` on Work,
Instance, Hub and OtherResource calls `add_version()`, so an ORM-mediated write
would record a Version per resource -- a few hundred thousand entries saying a
re-serialisation was a cataloguing edit, with `update_bf_classes` churn beside
it. Bypassing the ORM skips those events, which is also the honest answer: there
is no edit to record.

**It is safe to run twice.** Coercion is idempotent, so a second pass is a no-op.
The DAG uses that as its own check: apply it, then run it again in dry-run mode
and expect zero changes.

**It can be interrupted.** Each batch is written in its own transaction and the
sweep is by keyset on `id`, so a failed run is resumed by running it again rather
than by being undone.
"""

import logging
import os
from collections.abc import Iterator
from typing import Any, NamedTuple

from bluecore_models.utils.graph import framed_for_storage
from sqlalchemy import MetaData, Table, bindparam, select, update
from sqlalchemy.engine import Engine

from ils_middleware.tasks.bluecore import get_engine

logger = logging.getLogger(__name__)

# How many rows to read, re-frame and write per transaction. Framing is the
# expensive part, at roughly 30ms a resource, so batches exist to bound memory
# and to give an interrupted run somewhere to resume from.
BATCH_SIZE = int(os.environ.get("BLUECORE_REFRAME_BATCH_SIZE", "500"))

# The table holding every Work, Instance, Item, Hub and Other Resource.
# Reflected rather than imported from bluecore_models, so that this module writes
# columns and not mapped objects: importing the model would make it easy to
# reintroduce the ORM events this exists to avoid.
TABLE = "resource_base"

# The polymorphic types this sweep covers, matched against `resource_base.type`.
#
# Profiles live in the same table and must not be re-framed: `set_jsonld` in
# bluecore-models returns a Profile's data untouched, because sinopia-editor
# requires it in its own shape -- a top-level JSON-LD array of Sinopia vocabulary
# nodes rather than a framed BIBFRAME document. Framing one would not re-serialise
# it, it would replace it with something the editor cannot read.
#
# An allowlist rather than an exclusion of profiles, so that a resource type added
# later has to opt in rather than being swept up by a sweep that has never seen it.
REFRAMABLE_TYPES = ("works", "instances", "hubs", "other_resources")


class Batch(NamedTuple):
    """One window of rows, and what re-framing them would do.

    `changes` is in the shape an executemany wants. `unchanged` is the
    interesting number on a second run: once the backfill has succeeded,
    re-framing should change nothing, and anything else means either the
    coercion is not idempotent or something wrote a row in the old shape.
    """

    seen: int
    changes: list[dict[str, Any]]
    unchanged: int
    failed: list[str]
    last_id: int

    @property
    def changed(self) -> int:
        return len(self.changes)


def resource_table(engine: Engine) -> Table:
    """The resource table, reflected from the live database."""
    return Table(TABLE, MetaData(), autoload_with=engine)


def reframe(data: dict[str, Any] | None, uri: str) -> dict[str, Any] | None:
    """The stored JSON-LD, re-framed.

    `framed_for_storage` is the write path `set_jsonld` uses, so this is the
    same transformation the ORM would apply, without the ORM applying it. That
    matters for more than tidiness. This function used to reproduce the logic
    instead, with a test standing guard against the copy drifting, and the copy
    drifted: bluecore-models began storing `@context` so a row could say which
    vocabulary framed it, while this was still stripping it on the way out. Run
    unchanged, it would have removed the marker from every row it touched and
    left exactly the document that cannot be read back reliably -- framed with
    the current context, saying nothing about which.

    So a resource now carries `@context` after this runs. That is the point of
    the change, not a side effect of it.
    """
    if data is None:
        return None
    if not isinstance(data, dict):
        # `dict()` is not a safe way to find this out. Given a list of JSON-LD
        # node objects it reads each one as a key/value pair, which a node with
        # exactly two keys satisfies, so it half-consumes the document before
        # raising on the first larger node -- an error about a "dictionary update
        # sequence" that says nothing about the resource being the wrong shape.
        raise TypeError(f"expected a JSON-LD object, got {type(data).__name__}")
    return framed_for_storage(uri, data)


def batches(
    engine: Engine, table: Table, batch_size: int = BATCH_SIZE
) -> Iterator[Batch]:
    """Walk the table by keyset, re-framing as it goes.

    Keyset rather than OFFSET: OFFSET makes the database count past every row it
    has already skipped, so a sweep gets slower the further it gets, and a row
    inserted mid-run shifts the window underneath it.

    Only REFRAMABLE_TYPES are read, so a profile is never seen rather than being
    seen and failing.
    """
    after: int | None = None

    while True:
        query = (
            select(table.c.id, table.c.uri, table.c.data)
            .where(table.c.type.in_(REFRAMABLE_TYPES))
            .order_by(table.c.id)
        )
        if after is not None:
            query = query.where(table.c.id > after)
        with engine.connect() as connection:
            rows = connection.execute(query.limit(batch_size)).all()

        if not rows:
            return

        changes: list[dict[str, Any]] = []
        unchanged = 0
        failed: list[str] = []
        for row in rows:
            try:
                framed = reframe(row.data, row.uri)
            except Exception as error:  # noqa: BLE001 - one bad row must not stop the sweep
                logger.warning(f"{row.uri}: could not re-frame: {error}")
                failed.append(f"{row.uri}: {type(error).__name__}: {error}")
                continue
            if framed == row.data:
                unchanged += 1
            else:
                changes.append({"row_id": row.id, "new_data": framed})

        yield Batch(
            seen=len(rows),
            changes=changes,
            unchanged=unchanged,
            failed=failed,
            last_id=rows[-1].id,
        )
        after = rows[-1].id


def write(engine: Engine, table: Table, changes: list[dict[str, Any]]) -> None:
    """Apply one batch of re-framed values.

    An executemany against the table, bypassing the ORM so that no Version rows
    or Bibframe class updates are triggered. The bind parameters are `row_id` and
    `new_data` rather than `id` and `data` because a bindparam cannot share a name
    with a column being set in an UPDATE.
    """
    if not changes:
        return
    statement = (
        update(table)
        .where(table.c.id == bindparam("row_id"))
        .values(data=bindparam("new_data"))
    )
    with engine.begin() as connection:
        connection.execute(statement, changes)


def run(bluecore_db: str, dry_run: bool = True, batch_size: int = BATCH_SIZE) -> dict:
    """Re-frame every resource, or report what that would change.

    Returns a summary rather than writing a report file. The report helpers in
    this repo (`write_report`, `run_dir`) arrived with the bulk update work and
    are not on main yet, so this logs and returns instead; worth replacing with
    them once they land.
    """
    engine = get_engine(bluecore_db)
    table = resource_table(engine)
    total = changed = unchanged = 0
    failed: list[str] = []

    for batch in batches(engine, table, batch_size):
        if not dry_run:
            write(engine, table, batch.changes)

        total += batch.seen
        changed += batch.changed
        unchanged += batch.unchanged
        failed.extend(batch.failed)
        logger.info(
            f"{total} seen, {changed} {'changed' if not dry_run else 'to change'}, "
            f"{unchanged} already current, {len(failed)} failed"
        )

    summary = {
        "dry_run": dry_run,
        "seen": total,
        "changed": changed,
        "unchanged": unchanged,
        "failed": failed,
    }
    logger.info(f"reframe finished: {summary}")
    return summary
