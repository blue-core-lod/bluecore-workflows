"""Re-frame every Blue Core resource's stored JSON-LD."""

import logging
from datetime import datetime

from airflow.sdk import Param, dag, get_current_context, task

from ils_middleware.tasks.bluecore import get_bluecore_db
from ils_middleware.tasks.reframe import BATCH_SIZE, run

logger = logging.getLogger(__name__)

DOC_MD = f"""
Re-serialises the `data` column of every Work, Instance, Item, Hub and Other
Resource, so that it matches the current JSON-LD context in bluecore-models.

Profiles are left alone. They are stored in the same table, but bluecore-models
never frames them since sinopia-editor currently requires a profile's data in its own
shape.

It doesn't change the RDF triples, just how they are expressed as JSON-LD.

- **dry_run** (on by default) reports how many resources would change and writes
  nothing.
- **batch_size** rows are read, re-framed and written per transaction, {BATCH_SIZE}
  by default. An interrupted run is resumed by running it again: the sweep is by
  keyset on `id` and each batch commits on its own, so nothing needs undoing.

Re-framing is idempotent, which gives the run its own check. Apply it, then
trigger it again with `dry_run` on: the second run should report every resource
as already current and none to change.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 6),
    catchup=False,
    tags=["bulk", "jsonld", "maintenance"],
    default_args={"owner": "airflow"},
    doc_md=DOC_MD,
    params={
        "dry_run": Param(
            True,
            type="boolean",
            description="Report what would change without writing anything.",
        ),
        "batch_size": Param(
            BATCH_SIZE,
            type="integer",
            minimum=1,
            description="Rows per transaction.",
        ),
    },
)
def reframe_jsonld():
    @task
    def reframe_resources() -> dict:
        """Sweep the table, and report what changed.

        One task rather than a mapped task per batch, unlike the bulk update DAG:
        that one is given a list of URIs up front and can partition it, whereas
        this does not know how many resources there are until it has walked them,
        and the walk has to be sequential because each batch's starting point is
        the previous batch's last id.
        """
        context = get_current_context()
        params = context.get("params") or {}
        dry_run = bool(params.get("dry_run", True))
        batch_size = int(params.get("batch_size", BATCH_SIZE))

        logger.info(f"re-framing with dry_run={dry_run}, batch_size={batch_size}")
        summary = run(get_bluecore_db(), dry_run=dry_run, batch_size=batch_size)

        if summary["failed"]:
            # Reported rather than raised: a row that will not re-frame is worth
            # knowing about and is not a reason to abandon the rest of the sweep,
            # which is resumable and can be run again once the row is fixed.
            logger.error(
                f"{len(summary['failed'])} resources could not be re-framed: "
                f"{summary['failed'][:10]}"
            )
        return summary

    reframe_resources()


reframe_jsonld_dag = reframe_jsonld()
