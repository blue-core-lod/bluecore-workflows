import logging
import os
import pathlib

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

BLUECORE_URL = os.environ.get("BLUECORE_URL", "https://bcld.info/")

logger = logging.getLogger(__name__)


def delete_upload(upload: str, remove_empty_parent: bool = False) -> None:
    """
    Deletes upload file
    """
    upload_path = pathlib.Path(upload)
    upload_path.unlink()
    if remove_empty_parent:
        parent_dir = upload_path.parent
        if parent_dir.exists() and not any(parent_dir.iterdir()):
            parent_dir.rmdir()


def is_zip(file_name: str) -> bool:
    """Determines if file is a zip file"""
    if file_name.endswith(".zip") or file_name.endswith(".gz"):
        return True
    return False


def get_bluecore_db() -> str:
    pg_hook = PostgresHook("bluecore_db")
    return str(pg_hook.sqlalchemy_url)


# we need to run in a virtualenv so that we can use the latest
# bluecore-models and sqlalchemy 2
@task.virtualenv(
    requirements=[
        "bluecore-models @ git+https://github.com/blue-core-lod/bluecore-models@save-graph"
    ],
    system_site_packages=False,
)
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
