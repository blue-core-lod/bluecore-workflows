"""Loads all CBD files in a zip file into Blue Core"""

import logging
import pathlib
from datetime import datetime

from airflow.sdk import dag, task, get_current_context

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
    @task
    def setup() -> str:
        """Retrieves and checks for cbd zip file"""
        context = get_current_context()
        params = context.get("params")
        logger.info(f"Params are {params}")
        if params is None:
            raise ValueError("Missing params")
        zip_file = params.get("file")
        logger.info(f"Zip file {zip_file}")
        if zip_file is None:
            raise ValueError("Missing zip file path")
        zip_file_path = pathlib.Path(zip_file)
        if not zip_file_path.exists():
            raise FileNotFoundError(f"{zip_file_path} does not exist")
        logger.info(f"Ingesting zip file {zip_file_path}")
        return str(zip_file_path)

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
    def convert_zip_to_tar_gz(archive_file_path):
        """Converts zip file to tar.gz file"""
        if archive_file_path.endswith(".gz"):
            return archive_file_path
        return zip_to_tar_gz(archive_file_path)

    @task
    def batch_cbd_files(archive_file: str) -> list:
        """Extracts list of CBD files from zip and creates batches of filenames"""
        return batch_archived_files(archive_file)

    @task
    def bluecore_db_info() -> str:
        return get_bluecore_db()

    @task
    def delete_file_path(archive_file_path: str):
        current_path = pathlib.Path(archive_file_path)
        remove_empty_parent = current_path.parent.name != "uploads"
        delete_upload(upload=archive_file_path, remove_empty_parent=remove_empty_parent)

    @task
    def cbd_file_loader(**kwargs):
        return load_cbd_files(
            bluecore_db=kwargs["bluecore_db"],
            archived_file_path=kwargs["archived_file_path"],
            user_uid=kwargs["user_uid"],
            cbd_files=kwargs["cbd_files"],
        )

    zip_file_path = setup()
    user_uid = get_keycloak_user_uid()
    archive_file_path = convert_zip_to_tar_gz(zip_file_path)
    bc_db_info = bluecore_db_info()
    cbd_batches = batch_cbd_files(archive_file=archive_file_path)
    errors = cbd_file_loader.partial(
        bluecore_db=bc_db_info,
        archived_file_path=archive_file_path,
        user_uid=user_uid,
    ).expand(cbd_files=cbd_batches)
    delete_task = delete_file_path(archive_file_path=archive_file_path)
    errors >> delete_task


archived_file_loader_dag = archived_file_loader()
