"""Re-serialise every resource's stored JSON-LD, without changing what it says.

bluecore-models used to frame a resource's `data` and leave a property with one
value as a bare value, so the same property arrived as a scalar on one resource
and a list on the next -- and often both on one resource. `frame_jsonld` now
coerces every property to a list. Rows written after that change get the new
shape; rows already in the database keep the old one until they are re-framed,
which is what this does.

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

from bluecore_models.utils.graph import CONTEXT, frame_jsonld
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

    This has to reproduce `set_jsonld` in bluecore-models, not just call
    `frame_jsonld`. A stored value has had its `@context` removed, and framing
    without one leaves every compacted key -- `title`, `note` -- as an unknown
    term rather than expanding it to its BIBFRAME URI. So the default context
    goes back on before framing and comes off again after, exactly as the ORM
    handler does on the way in.

    Reproducing it is the price of not going through the ORM, whose `after_update`
    events are what this module exists to avoid. `test_reframe_needs_the_context`
    is the guard against the duplication drifting.
    """
    if data is None:
        return None
    document = dict(data)
    if "@context" not in document:
        document["@context"] = CONTEXT
    framed = frame_jsonld(uri, document)
    framed.pop("@context", None)
    return framed


def batches(
    engine: Engine, table: Table, batch_size: int = BATCH_SIZE
) -> Iterator[Batch]:
    """Walk the table by keyset, re-framing as it goes.

    Keyset rather than OFFSET: OFFSET makes the database count past every row it
    has already skipped, so a sweep gets slower the further it gets, and a row
    inserted mid-run shifts the window underneath it.
    """
    after: int | None = None

    while True:
        query = select(table.c.id, table.c.uri, table.c.data).order_by(table.c.id)
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
