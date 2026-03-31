import io
import logging
import os
import pathlib
import tarfile
import zipfile


from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

BLUECORE_URL = os.environ.get("BLUECORE_URL", "https://bcld.info/")

logger = logging.getLogger(__name__)


def batch_archived_files(
    archive_file_location: str, number_of_batches: int = 5
) -> list:
    """
    Retrieves list of files from a zip file and batches the file names into
    lists
    """
    archive_file_path = pathlib.Path(archive_file_location)

    if not archive_file_path.exists():
        raise FileNotFoundError(f"{archive_file_path} does not exist")

    archive_file = tarfile.open(archive_file_path, "r")
    file_names = archive_file.getnames()

    total_names = len(file_names)
    batch_size = int(total_names / number_of_batches)
    batches = []
    for i in range(0, total_names, batch_size):
        batch = file_names[i : i + batch_size]
        batches.append(batch)
    return batches


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


def zip_to_tar_gz(zip_file: str) -> str:
    """
    Converts zip file to tar.gz file
    """
    zip_path = pathlib.Path(zip_file)
    tar_path = zip_path.parent / f"{zip_path.stem}.tar.gz"

    with zipfile.ZipFile(zip_path) as cbd_zip_file:
        with tarfile.open(tar_path, "w:gz") as cbd_tar_file:
            for zip_info in cbd_zip_file.infolist():
                if zip_info.is_dir():
                    continue
                tar_info = tarfile.TarInfo(name=zip_info.filename)
                tar_info.size = zip_info.file_size
                with cbd_zip_file.open(zip_info.filename) as infile:
                    cbd_tar_file.addfile(
                        tarinfo=tar_info, fileobj=io.BytesIO(infile.read())
                    )
    zip_path.unlink()
    return str(tar_path)


# we need to run in a virtualenv so that we can use the latest
# bluecore-models and sqlalchemy 2
#
# when testing bluecore-models it can be helpful to reference a branch here
# e.g. "bluecore-models @ git+https://github.com/blue-core-lod/bluecore-models@my-branch"
@task.virtualenv(
    requirements=["bluecore-models"],
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
    graph.parse(file_path, format="json-ld")

    # get the bluecore namespace, ideally we would use Airflow's Variable
    # here but we are running in a virtualenv without Airflow installed
    bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")

    # save the graph and return the number of triples processed
    bc_graph = save_graph(session_maker, graph, namespace=bc_url)
    logger.info(f"processed {len(bc_graph)} triples")


@task.virtualenv(
    requirements=["bluecore-models"],
    system_site_packages=False,
)
def load_cbd_files(
    cbd_files: list, bluecore_db: str, user_uid: str, archived_file_path: str
):
    """"""

    import logging
    import tarfile

    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)

    try:
        from bluecore_models.models.version import CURRENT_USER_ID

        CURRENT_USER_ID.set(user_uid)
        logger.info("Using CURRENT_USER_ID: %s", user_uid)
    except Exception as e:
        logger.error("Failed to set CURRENT_USER_ID: %s", e)

    import rdflib

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from bluecore_models.bluecore_graph import save_graph

    bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")

    engine = create_engine(bluecore_db)
    session_maker = sessionmaker(bind=engine)

    errors = []

    logger.info(f"Starting load of {len(cbd_files):,} files")

    with tarfile.open(archived_file_path, "r") as cbd_archived_file:
        for i, name in enumerate(cbd_files):
            graph_format = rdflib.util.guess_format(name)
            graph = rdflib.Graph()
            cbd_file_buf = cbd_archived_file.extractfile(name)
            try:
                graph.parse(data=cbd_file_buf.read(), format=graph_format)
                save_graph(session_maker, graph, namespace=bc_url)
            except Exception:
                errors.append(name)

            if i > 0 and not i % 100:
                logger.info(f"{i:,} graphs saved")
    logger.info(f"Finished load of {len(cbd_files):,} with {len(errors):,} errors")
    return errors
