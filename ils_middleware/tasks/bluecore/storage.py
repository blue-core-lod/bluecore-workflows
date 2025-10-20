import logging

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_bluecore_db():
    pg_hook = PostgresHook("bluecore_db")
    return str(pg_hook.sqlalchemy_url)


@task.virtualenv(
    requirements=["bluecore-models"],
    system_site_packages=False,
)
def load_file(file_path: str, user_uid: str, bluecore_db: str) -> int:
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
    from bluecore_models.bluecore_graph import save_graph

    # parse the RDF and give it to the BluecoreGraph
    graph = rdflib.Graph()
    graph.parse(file_path)

    # create the database engine
    engine = create_engine(bluecore_db)

    # save the graph and return the number of triples processed
    bc_graph = save_graph(engine, graph)
    return len(bc_graph)

