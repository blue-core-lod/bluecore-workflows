"""Record Loader Workflow for a single file."""

import logging
import os

from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from ils_middleware.tasks.amazon.bluecore_records_s3 import get_file

from ils_middleware.tasks.bluecore.batch import (
    delete_upload,
    is_zip,
    parse_file_to_graph,
)
from ils_middleware.tasks.bluecore.storage import (
    get_bluecore_db,
    store_bluecore_resources,
)

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2025, 2, 20),
    catchup=False,
    tags=["ingest", "record"],
    default_args={"owner": "airflow"},
)
def resource_loader():
    @task()
    def ingest() -> str:
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("Missing params")
        file_path = params.get("file")
        if file_path is None:
            raise ValueError("Missing file path")
        logger.info(f"Ingesting {file_path}")
        local_file_path = get_file(file=file_path)
        return local_file_path

    @task.branch(task_id="process_choice")
    def choose_processing(**kwargs) -> list:
        file_path = kwargs.get("file", "")
        if is_zip(file_path):
            return ["process_zip"]
        return ["process"]

    @task(trigger_rule="one_success")
    def process(*args, **kwargs) -> list:
        file_path = kwargs.get("file", "")
        logger.info(f"Processing data {file_path}")
        resources = parse_file_to_graph(file_path)
        logger.info(f"{len(resources)} Resources to process")
        return resources

    @task
    def process_zip(**kwargs):
        # Placeholder to be implemented in a follow-up PR
        logger.info("Process zip file")

    @task
    def bluecore_db_info(**kwargs) -> str:
        return get_bluecore_db()

    @task
    def delete_file_path(file_path: str):
        parent_dir = os.path.dirname(file_path)
        remove_empty_parent = parent_dir != "uploads"
        delete_upload(upload=file_path, remove_empty_parent=remove_empty_parent)

    file_path = ingest()
    next_task = choose_processing(file=file_path)
    records = process(file=file_path)
    process_zip_task = process_zip(file=file_path)
    bluecore_db = bluecore_db_info()

    next_task >> [records, process_zip_task]
    process_zip_task >> records

    store_bluecore_resources(
        records=records, bluecore_db=bluecore_db
    ) >> delete_file_path(file_path)


resource_loader()
