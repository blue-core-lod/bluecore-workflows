"""Apply a SPARQL UPDATE to a list of Blue Core Works and Instances."""

import logging
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.sdk import Param, dag, get_current_context, task

from ils_middleware.tasks.bluecore import get_bluecore_db
from ils_middleware.tasks.bulk_update import (
    DEFAULT_BATCH_SIZE,
    MAX_RESOURCES,
    batch_uris,
    check_query,
    merge_reports,
    read_uris,
    summarize,
    update_resources,
)
from ils_middleware.tasks.report import write_report

logger = logging.getLogger(__name__)

DOC_MD = f"""
Applies one SPARQL UPDATE to each of the Blue Core resources listed in a CSV.

- **file** is a CSV in the uploads directory with a header row naming a `uri`
  column, holding Blue Core Work, Instance or Hub URIs. Any other columns are
  ignored, so the list can carry a local identifier or a note. At most
  {MAX_RESOURCES:,} resources per run.
- **query** is a single SPARQL UPDATE, applied to each resource's own triples.
  `?resource` is bound to the resource being updated, so a query can be written
  once and used for the whole list:

      PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>
      DELETE {{ ?resource bf:note ?note }}
      WHERE  {{ ?resource bf:note ?note }}

  `GRAPH`, `WITH`, `USING`, `SERVICE`, `LOAD`, `DROP`, `CLEAR`, `ADD`, `MOVE`
  and `COPY` are rejected: there is one graph here, the resource's own, and
  `USING` and `SERVICE` would fetch another. A resource is skipped and reported,
  rather than saved, if the update would delete it, change its `rdf:type` or
  anything under its `bf:adminMetadata`, start describing some other resource,
  change how it relates to another Work or Instance (`bf:hasInstance`,
  `bf:instanceOf`, `bf:hasExpression`, `bf:expressionOf`), or add triples the
  database does not keep (`dcterms:`, `lclocal:`, OCLC numbers).
- **dry_run** (on by default) reports the triples the update would add and
  remove for every resource, without writing anything. Turn it off to apply it.

Every resource written gets a version recorded against the Keycloak user who
triggered the run. The run's report is written to the reports volume, under
`bulk_update/<run_id>/`.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 9, 8),
    catchup=False,
    tags=["bulk", "update", "sparql"],
    default_args={"owner": "airflow"},
    doc_md=DOC_MD,
    params={
        "file": Param(
            "",
            type="string",
            title="CSV file",
            description="Path to a CSV with a header row naming a uri column of Blue Core resource URIs.",
        ),
        "query": Param(
            "",
            type="string",
            format="multiline",
            title="SPARQL update",
            description="The SPARQL UPDATE to apply to each resource. ?resource is bound to the resource being updated.",
        ),
        "dry_run": Param(
            True,
            type="boolean",
            title="Dry run",
            description="Report what would change without writing anything.",
        ),
        "batch_size": Param(
            DEFAULT_BATCH_SIZE,
            type="integer",
            minimum=1,
            title="Resources per task",
            description="How many resources each parallel task updates.",
        ),
    },
)
def bulk_update():
    @task
    def plan() -> list[list[str]]:
        """
        Check the query and the CSV, and split the URIs into batches. Both
        checks happen here, before any resource is touched, so a bad query or a
        missing file fails the run rather than part of it.
        """
        params = get_current_context().get("params") or {}
        check_query(params.get("query") or "")
        uris = read_uris(params.get("file") or "")
        batches = batch_uris(uris, int(params.get("batch_size") or DEFAULT_BATCH_SIZE))
        logger.info(
            f"{len(uris):,} resources in {len(batches):,} batches, "
            f"dry_run={params.get('dry_run', True)}"
        )
        return batches

    @task
    def get_keycloak_user_uid() -> str | None:
        """Pull keycloak user uid from dag_run.conf"""
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        uid = conf.get("user_uid")
        logger.info(f"user_uid from conf: {uid!r}")
        return uid

    @task
    def bluecore_db_info() -> str:
        return get_bluecore_db()

    @task(max_active_tis_per_dag=5)
    def update_batch(uris: list[str], bluecore_db: str, user_uid: str | None) -> dict:
        params = get_current_context().get("params") or {}
        return update_resources(
            uris,
            params.get("query") or "",
            bluecore_db,
            user_uid=user_uid,
            dry_run=bool(params.get("dry_run", True)),
        )

    # all_done, not the default all_success: if a batch dies outright -- an out
    # of memory worker, a failure building the engine -- the resources the other
    # batches did update still need reporting. Without this the report task is
    # skipped as upstream_failed and the run says nothing about what it did.
    @task(trigger_rule="all_done")
    def report(reports: list[dict]) -> str:
        """
        Write the run's report, then fail if any resource errored, or if a batch
        failed before it could report -- a bulk operation that only partly
        worked shouldn't look like one that worked. Resources that were
        deliberately skipped are not errors.
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        merged = merge_reports(reports)
        summary, sections = summarize(merged)
        path = write_report(
            dag_id=getattr(dag_run, "dag_id", "bulk_update"),
            run_id=getattr(dag_run, "run_id", "unknown"),
            report=merged,
            title="Blue Core Bulk Update",
            summary=summary,
            sections=sections,
        )
        # A batch that failed outright returns nothing, so count the gaps too --
        # merge_reports drops them, and they are resources nobody has heard
        # about rather than resources that were left alone.
        missing = sum(1 for report in reports if not report)
        if merged["errors"] or missing:
            raise AirflowException(
                f"{len(merged['errors']):,} resources errored and {missing:,} "
                f"batches did not report, see {path}"
            )
        return path

    batches = plan()
    user_uid = get_keycloak_user_uid()
    bluecore_db = bluecore_db_info()

    # One mapped task per batch, which Airflow runs in parallel up to its own
    # concurrency limits, and each batch updates its resources one at a time.
    #
    # Batching rather than one task per resource, because a task instance is not
    # free: each one is a queued message, a task runner process, a row in
    # task_instance, an XCom, and a fresh database engine in the worker child
    # that picks it up -- all to do a single save_graph. A thousand of those
    # would spend most of the run on scheduling rather than on updating.
    #
    # Batching rather than one task for the whole list, because a batch is a
    # useful unit to retry, has a log of its own to read, and reports separately
    # -- and because batch_size is then the knob for how much write concurrency
    # this run puts on the database: one batch is fully sequential, ten run at
    # once. Sequential is the gentler setting if a run ever contends with
    # ingest.
    results = update_batch.partial(bluecore_db=bluecore_db, user_uid=user_uid).expand(
        uris=batches
    )
    report(results)


bulk_update_dag = bulk_update()
