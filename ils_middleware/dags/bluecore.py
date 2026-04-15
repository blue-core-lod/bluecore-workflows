"""Record Loader Workflow for a single file."""

import logging
import pathlib
from datetime import datetime

from airflow.sdk import dag, get_current_context, task

from ils_middleware.tasks.amazon.bluecore_records_s3 import get_file
from ils_middleware.tasks.bluecore import delete_upload, get_bluecore_db, load

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

    @task()
    def get_keycloak_user_uid() -> str | None:
        """
        Pull keycloak user uid from dag_run.conf
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        uid = conf.get("user_uid")
        logger.info(f"user_uid from conf: {uid!r}")
        return uid

    @task
    def bluecore_db_info(**kwargs) -> str:
        return get_bluecore_db()

    @task
    def load_resource(file_path: str, user_uid: str, bluecore_db: str):
        load(file_path, user_uid, bluecore_db)

    @task
    def delete_file_path(file_path: str):
        current_path = pathlib.Path(file_path)
        remove_empty_parent = current_path.parent.name != "uploads"
        delete_upload(upload=file_path, remove_empty_parent=remove_empty_parent)

    file_path = ingest()
    user_uid = get_keycloak_user_uid()
    bluecore_db = bluecore_db_info()
    load_task = load_resource(file_path, user_uid, bluecore_db)
    load_task >> delete_file_path(file_path)


resource_loader_dag = resource_loader()
