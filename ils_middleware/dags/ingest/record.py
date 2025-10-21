"""Record Loader Workflow for a single file."""

import logging
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from ils_middleware.tasks.amazon.bluecore_records_s3 import get_file
from ils_middleware.tasks.bluecore.batch import delete_upload
from ils_middleware.tasks.bluecore.storage import get_bluecore_db

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

    # we need to run in a virtualenv so that we can use the latest
    # bluecore-models and sqlalchemy 2
    @task.virtualenv(requirements=["bluecore-models @ git+https://github.com/blue-core-lod/bluecore-models@save-graph"], system_site_packages=False)
    def load(file_path: str, user_uid: str, bluecore_db: str):
        import logging

        logger = logging.getLogger(__name__)
        if not logger.handlers:
            logging.basicConfig(level=logging.INFO)
        """Stores Work or Instance in the Blue Core Database

        Note: Virtualenv is needed because bluecore.models uses SQLAlchemy 2.+
        and Airflow uses a 1.x version of SQLAlchemy
        """
        try:
            """
            Set CURRENT_USER_ID from DAG-provided user_uid so #add_version can write
            versions.keycloak_user_id during ORM events triggered by inserts/updates.
            """
            from bluecore_models.models.version import CURRENT_USER_ID

            CURRENT_USER_ID.set(user_uid)
            logger.info("Using CURRENT_USER_ID: %s", user_uid)
        except Exception as e:
            logger.error("Failed to set CURRENT_USER_ID: %s", e)

        import rdflib
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from bluecore_models.bluecore_graph import save_graph

        # create the database session maker
        engine = create_engine(bluecore_db)
        session_maker = sessionmaker(bind=engine)

        # parse the RDF into a graph
        graph = rdflib.Graph()
        graph.parse(file_path)

        # get the bluecore namespace, ideally we would use Airflow's Variable
        # here but we are running in a virtualenv without Airflow installed
        bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")

        # save the graph and return the number of triples processed
        bc_graph = save_graph(session_maker, graph, namespace=bc_url)
        logger.info(f"processed {len(bc_graph)} triples")

    @task
    def delete_file_path(file_path: str):
        parent_dir = os.path.dirname(file_path)
        remove_empty_parent = parent_dir != "uploads"
        delete_upload(upload=file_path, remove_empty_parent=remove_empty_parent)

    file_path = ingest()
    user_uid = get_keycloak_user_uid()
    bluecore_db = bluecore_db_info()
    load(file_path, user_uid, bluecore_db) >> delete_file_path(file_path)


resource_loader()
