"""BFDB activity streams consumer DAG."""

import logging
import pathlib

import pendulum
from airflow.sdk import dag, task

from ils_middleware.tasks.bfdb_activity_streams import (
    ACTIVITY_STREAM_FEEDS,
    combine_downloads,
    create_activity_stream_run,
    current_run_date,
    process_activity_stream_feed,
)
from ils_middleware.tasks.bluecore import (
    batch_files,
    delete_upload,
    get_bluecore_db,
    load_files,
)

logger = logging.getLogger(__name__)

BFDB_AGENT_USER_UID = "BFDB Agent"


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

    @task
    def delete_file_path(files: list[str], errors: list[str]):
        if not files:
            return

        if len(errors) > 0:
            msg = f"Errors exist; keeping {files}"
            logger.error(msg)
            return

        current_path = pathlib.Path(files[0])
        remove_empty_parent = current_path.parent.name != "uploads"
        delete_upload(upload=files, remove_empty_parent=remove_empty_parent)

    @task
    def bfdb_batch_files(files: list[str]) -> list[list[str]]:
        """Extracts list of CBD files from zip and creates batches of filenames"""
        return batch_files(files)

    @task
    def bfdb_file_loader(**kwargs):
        return load_files(
            bluecore_db=kwargs["bluecore_db"],
            user_uid=kwargs["user_uid"],
            files=kwargs["files"],
        )

    activity_stream_run_id = create_run_id()
    bluecore_db = bluecore_db_info()
    current_date = read_run_date()
    feed_downloads = process_feed.partial(
        activity_stream_run_id=activity_stream_run_id,
        bluecore_db=bluecore_db,
        current_date=current_date,
    ).expand(feed_config=ACTIVITY_STREAM_FEEDS)

    files = collect_downloads(feed_downloads)
    bfdb_batches = bfdb_batch_files(files=files)
    errors = bfdb_file_loader.partial(
        bluecore_db=bluecore_db,
        user_uid=BFDB_AGENT_USER_UID,
    ).expand(files=bfdb_batches)
    delete_task = delete_file_path(files=files, errors=errors)
    errors >> delete_task


process_activity_streams_dag = process_activity_streams()
