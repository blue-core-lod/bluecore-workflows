"""BFDB activity streams consumer DAG."""

import logging
import pathlib

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from ils_middleware.tasks.bfdb_activity_streams import (
    ACTIVITY_STREAM_FEEDS,
    combine_downloads,
    create_activity_stream_run,
    current_run_date,
    process_activity_stream_feed,
)
from ils_middleware.tasks.bluecore import get_bluecore_db

logger = logging.getLogger(__name__)

BFDB_AGENT_USER_UID = "BFDB Agent"


def _resource_loader_conf(file_path: str) -> dict[str, str]:
    return {"file": file_path, "user_uid": BFDB_AGENT_USER_UID}


# schedule="0 0 * * *",
@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 11, tz="America/New_York"),
    catchup=False,
    tags=["bfdb", "activity-streams"],
    default_args={"owner": "airflow"},
)
def process_activity_streams():
    @task
    def create_run_id() -> str:
        return create_activity_stream_run()

    @task
    def bluecore_db_info() -> str:
        return get_bluecore_db()

    @task
    def read_run_date() -> str:
        return current_run_date()

    @task(
        map_index_template=(
            "Process {{ task.op_kwargs['feed_config']['feed_name'] }} feed"
        )
    )
    def process_feed(
        feed_config: dict[str, str],
        activity_stream_run_id: str,
        bluecore_db: str,
        current_date: str,
    ) -> list[str]:
        feed_name = feed_config["feed_name"]
        feed_url = feed_config["feed_url"]
        logger.info(f"Processing activity stream feed {feed_url}")
        return process_activity_stream_feed(
            bluecore_db, feed_url, feed_name, current_date, activity_stream_run_id
        )

    @task
    def collect_downloads(feed_downloads: list[list[str]]) -> list[str]:
        return combine_downloads(feed_downloads)

    @task(map_index_template="Load {{ task.op_kwargs['file_path'] }}")
    def trigger_resource_loader(file_path: str, **kwargs):
        logger.info(f"Triggering resource_loader for {file_path}")
        TriggerDagRunOperator(
            task_id=f"resource-loader-{pathlib.Path(file_path).stem}",
            trigger_dag_id="resource_loader",
            conf=_resource_loader_conf(file_path),
            wait_for_completion=True,
            poke_interval=30,
        ).execute(kwargs)

    activity_stream_run_id = create_run_id()
    bluecore_db = bluecore_db_info()
    current_date = read_run_date()
    feed_downloads = process_feed.partial(
        activity_stream_run_id=activity_stream_run_id,
        bluecore_db=bluecore_db,
        current_date=current_date,
    ).expand(feed_config=ACTIVITY_STREAM_FEEDS)

    # TODO: Change triggering resource loader DAG to bulk upload DAG when it is complete.

    trigger_resource_loader.expand(file_path=collect_downloads(feed_downloads))


process_activity_streams_dag = process_activity_streams()
