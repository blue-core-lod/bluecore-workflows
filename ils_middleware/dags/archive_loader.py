"""Loads all CBD files in a zip file into Blue Core"""

import logging
import os
import pathlib
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from ils_middleware.tasks.bluecore import (
    batch_archived_files,
    delete_upload,
    get_bluecore_db,
    load_cbd_files,
    zip_to_tar_gz,
)

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2026, 3, 29),
    catchup=False,
    tags=["ingest", "record", "zip"],
    default_args={"owner": "airflow"},
)
def archived_file_loader():
    @task()
    def setup() -> dict:
        """Retrieves and checks for cbd zip file"""
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("Missing params")
        zip_file = params.get("file")
        if zip_file is None:
            raise ValueError("Missing zip file path")
        zip_file_path = pathlib.Path(zip_file)
        if not zip_file_path.exists():
            raise FileNotFoundError(f"{zip_file_path} does not exist")
        logger.info(f"Ingesting zip file {zip_file_path}")
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        uid = conf.get("user_uid")
        return {"archive_file": str(zip_file_path), "uid": uid}

    @task
    def convert_zip_to_tar_gz(archived_file_path):
        """Converts zip file to tar.gz file"""
        if archive_file_path.endswith(".gz"):
            return archive_file_path
        return zip_to_tar_gz(archive_file_path)

    @task
    def batch_cbd_files(archive_file: str) -> list:
        """Extracts list of CBD files from zip and creates batches of filenames"""
        return batch_archived_files(archive_file)

    @task
    def bluecore_db_info(**kwargs) -> str:
        return get_bluecore_db()

    @task
    def delete_file_path(archive_file_path: str):
        parent_dir = os.path.dirname(archive_file_path)
        remove_empty_parent = parent_dir != "uploads"
        delete_upload(upload=archive_file_path, remove_empty_parent=remove_empty_parent)

    dag_run_config = setup()
    archive_file_path = convert_zip_to_tar_gz(dag_run_config["archive_file"])
    bc_db_info = bluecore_db_info()
    cbd_batches = batch_cbd_files(archive_file=archive_file_path)
    errors = load_cbd_files.partial(
        bluecore_db=bc_db_info,
        archived_file_path=archive_file_path,
        user_uid="test_uuid",
    ).expand(cbd_files=cbd_batches)
    errors >> delete_file_path(archive_file_path=archive_file_path)


archived_file_loader_dag = archived_file_loader()
