"""MARC21 File Loader Workflow.

This DAG processes uploaded MARC21 files by:
1. Converting MARC21 to BIBFRAME (via marc2bibframe2 XSLT)
2. Converting BIBFRAME to JSON-LD
3. Loading the JSON-LD into the Blue Core database
4. Cleaning up temporary files
"""

import logging
import os
import pathlib
from datetime import datetime

from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from ils_middleware.tasks.bluecore import delete_upload, get_bluecore_db, load
from ils_middleware.tasks.marc import marc21_to_jsonld, save_jsonld_to_file

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2025, 2, 11),
    catchup=False,
    tags=["ingest", "marc21", "record"],
    default_args={"owner": "airflow"},
)
def marc21_loader():
    """
    Process MARC21 files uploaded via the API.

    Expected dag_run.conf:
    {
        "file": "/opt/airflow/uploads/{uuid}/{filename}",
        "user_uid": "{keycloak_user_uid}"
    }
    """

    @task()
    def get_marc_file_path() -> str:
        """
        Extract the MARC file path from dag_run params.

        Returns:
            Path to uploaded MARC21 file

        Raises:
            ValueError: If file path is missing or invalid
        """
        context = get_current_context()
        params = context.get("params")

        if params is None:
            raise ValueError("Missing params in DAG run context")

        file_path = params.get("file")
        if file_path is None:
            raise ValueError("Missing 'file' parameter in DAG run config")

        logger.info(f"Processing MARC21 file: {file_path}")

        # Verify file exists
        marc_file = pathlib.Path(file_path)
        if not marc_file.exists():
            raise ValueError(f"MARC file not found: {file_path}")

        return file_path

    @task()
    def get_keycloak_user_uid() -> str | None:
        """
        Pull Keycloak user UID from dag_run.conf for version tracking.

        Returns:
            Keycloak user UID or None if not provided
        """
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
        uid = conf.get("user_uid")

        logger.info(f"User UID from config: {uid!r}")
        return uid

    @task()
    def convert_marc_to_jsonld(marc_file_path: str) -> str:
        """
        Convert MARC21 file to JSON-LD format.

        This performs the complete conversion pipeline:
        1. MARC21/MARC XML → BIBFRAME (via XSLT)
        2. BIBFRAME → JSON-LD

        Args:
            marc_file_path: Path to MARC21 file

        Returns:
            Path to generated JSON-LD file
        """
        logger.info(f"Converting MARC21 to JSON-LD: {marc_file_path}")

        try:
            # Perform conversion
            jsonld = marc21_to_jsonld(marc_file_path)

            # Save JSON-LD to temporary file in same directory
            marc_path = pathlib.Path(marc_file_path)
            jsonld_filename = marc_path.stem + ".jsonld"
            jsonld_path = marc_path.parent / jsonld_filename

            jsonld_file = save_jsonld_to_file(jsonld, str(jsonld_path))

            logger.info(f"Conversion complete: {jsonld_file}")
            return jsonld_file

        except Exception as e:
            logger.error(f"MARC conversion failed: {e}")
            raise

    @task()
    def bluecore_db_info(**kwargs) -> str:
        """
        Get Blue Core database connection string.

        Returns:
            SQLAlchemy database URL
        """
        return get_bluecore_db()

    @task()
    def cleanup_files(marc_file_path: str, jsonld_file_path: str):
        """
        Clean up uploaded MARC file and generated JSON-LD file.

        Removes:
        - Original MARC file
        - Generated JSON-LD file
        - Empty parent directory if applicable

        Args:
            marc_file_path: Path to original MARC file
            jsonld_file_path: Path to generated JSON-LD file
        """
        logger.info("Cleaning up temporary files")

        # Delete JSON-LD file
        try:
            jsonld_path = pathlib.Path(jsonld_file_path)
            if jsonld_path.exists():
                jsonld_path.unlink()
                logger.info(f"Deleted JSON-LD file: {jsonld_file_path}")
        except Exception as e:
            logger.error(f"Failed to delete JSON-LD file: {e}")

        # Delete MARC file and parent directory if empty
        try:
            marc_path = pathlib.Path(marc_file_path)
            parent_dir = marc_path.parent
            remove_empty_parent = parent_dir.name != "uploads"

            delete_upload(
                upload=marc_file_path,
                remove_empty_parent=remove_empty_parent
            )
            logger.info(f"Deleted MARC file: {marc_file_path}")
        except Exception as e:
            logger.error(f"Failed to delete MARC file: {e}")

    # Define task dependencies
    marc_file = get_marc_file_path()
    user_uid = get_keycloak_user_uid()
    jsonld_file = convert_marc_to_jsonld(marc_file)
    bluecore_db = bluecore_db_info()

    # Load to database, then cleanup
    load(jsonld_file, user_uid, bluecore_db) >> cleanup_files(marc_file, jsonld_file)


# Instantiate the DAG
marc21_loader_dag = marc21_loader()
