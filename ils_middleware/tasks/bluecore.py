import io
import logging
import os
import pathlib
import tarfile
import time
import zipfile

import rdflib

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from bluecore_models.bluecore_graph import save_graph
from bluecore_models.models.version import CURRENT_USER_ID

from airflow.providers.postgres.hooks.postgres import PostgresHook

BLUECORE_URL = os.environ.get("BLUECORE_URL", "https://bcld.info/")

logging.getLogger("rdflib").setLevel(logging.ERROR)
logging.getLogger("bluecore_models").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# Pool sizing per engine (i.e. per Celery worker child process). The load tasks
# save graphs sequentially, so a single pooled connection is enough; overflow
# gives a little slack. Keep this small: the worst-case bluecore connection
# count from a fully-busy worker is worker_concurrency * (POOL_SIZE +
# MAX_OVERFLOW), and that shares the database server's connection budget with
# Airflow, Keycloak, and any other clients on the same (often shared/external)
# Postgres. Bounding it explicitly keeps this workflow's footprint predictable.
POOL_SIZE = int(os.environ.get("BLUECORE_DB_POOL_SIZE", "1"))
MAX_OVERFLOW = int(os.environ.get("BLUECORE_DB_MAX_OVERFLOW", "2"))
# Recycle connections older than this (seconds) so we don't hand out a
# connection Postgres has already closed server-side.
POOL_RECYCLE = int(os.environ.get("BLUECORE_DB_POOL_RECYCLE", "1800"))

# Engines are cached per database URL and reused across task invocations within
# a worker child process, rather than created (and their pools leaked) per task.
# The cache is populated lazily on first use *inside* each forked child, so no
# pooled connection is ever shared across processes.
_engines: dict[str, Engine] = {}


def get_engine(bluecore_db: str) -> Engine:
    """Return a process-local, pooled Engine for the given database URL,
    creating and caching it on first use."""
    engine = _engines.get(bluecore_db)
    if engine is None:
        engine = create_engine(
            bluecore_db,
            pool_pre_ping=True,
            pool_recycle=POOL_RECYCLE,
            pool_size=POOL_SIZE,
            max_overflow=MAX_OVERFLOW,
        )
        _engines[bluecore_db] = engine
    return engine


def batch_archived_files(
    archive_file_location: str, number_of_batches: int = 5
) -> list[list[str]]:
    """
    Retrieves list of files from a zip file and batches the file names into
    lists
    """
    archive_file_path = pathlib.Path(archive_file_location)

    if not archive_file_path.exists():
        raise FileNotFoundError(f"{archive_file_path} does not exist")

    archive_file = tarfile.open(archive_file_path, "r")
    file_names = [
        name
        for name in archive_file.getnames()
        if rdflib.util.guess_format(name) is not None
        and not pathlib.Path(name).name.startswith("._")
    ]

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
    return pg_hook.sqlalchemy_url.render_as_string(hide_password=False)


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


def load(file_path: str, user_uid: str, bluecore_db: str):
    """
    Stores Work or Instance in the Blue Core Database
    """
    try:
        """
        Set CURRENT_USER_ID from DAG-provided user_uid so #add_version can write
        versions.keycloak_user_id during ORM events triggered by inserts/updates.
        """
        CURRENT_USER_ID.set(user_uid)
        logger.info("Using CURRENT_USER_ID: %s", user_uid)
    except Exception as e:
        logger.error("Failed to set CURRENT_USER_ID: %s", e)

    # create the database session maker from the cached, process-local engine
    engine = get_engine(bluecore_db)
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


def load_cbd_files(
    cbd_files: list, bluecore_db: str, user_uid: str, archived_file_path: str
):
    """
    Load CBD files
    """
    logger = logging.getLogger(__name__)

    try:
        CURRENT_USER_ID.set(user_uid)
        logger.info("Using CURRENT_USER_ID: %s", user_uid)
    except Exception as e:
        logger.error("Failed to set CURRENT_USER_ID: %s", e)

    bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")

    engine = get_engine(bluecore_db)
    session_maker = sessionmaker(bind=engine)

    errors = []

    logger.info(f"Starting load of {len(cbd_files):,} files")

    with tarfile.open(archived_file_path, "r") as cbd_archived_file:
        for i, name in enumerate(cbd_files):
            if pathlib.Path(name).name.startswith("._"):
                continue
            graph_format = rdflib.util.guess_format(name)
            if graph_format is None:
                continue
            graph = rdflib.Graph()
            cbd_file_buf = cbd_archived_file.extractfile(name)
            if cbd_file_buf is None:
                errors.append(name)
                continue
            graph.parse(data=cbd_file_buf.read(), format=graph_format)
            # If deadlock occurs, try saving the graph again by sleep up to 2 seconds
            # to see if deadlock continues
            for attempt in range(3):
                try:
                    # Bulk load: Works/Instances are authoritative, but the many
                    # shared Other Resources these records reference are only
                    # linked, not re-described, on every record (their
                    # descriptions are maintained by a separate process).
                    save_graph(
                        session_maker,
                        graph,
                        namespace=bc_url,
                        update_other_resources=False,
                    )
                    break
                except OperationalError as e:
                    if attempt == 2:
                        raise
                    logger.error(f"Operational Error {e} for {name}")
                    time.sleep(2**attempt)
                except Exception as e:
                    logger.error(f"Error {e} for {name}")
                    errors.append(name)
                    break

            if i > 0 and not i % 100:
                logger.info(f"{i:,} graphs saved")
    logger.info(f"Finished load of {len(cbd_files):,} with {len(errors):,} errors")
    return errors
