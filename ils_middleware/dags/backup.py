"""Blue Core database backup DAG."""

import logging
from datetime import datetime

from airflow.decorators import dag, task

from ils_middleware.tasks.amazon.bluecore_records_s3 import upload_db_backup

logger = logging.getLogger(__name__)


@dag(
    schedule="@weekly",
    start_date=datetime(2026, 2, 20),
    catchup=False,
    tags=["backup", "database"],
    default_args={"owner": "airflow"},
)
def bluecore_db_backup():
    """Backs up the Blue Core PostgreSQL database to AWS S3."""

    @task()
    def dump_database() -> str:
        """
        Dumps the Blue Core PostgreSQL database to a local temp file.

        Uses the `bluecore_db` Airflow connection (PostgresHook) to
        retrieve connection info, then shells out to pg_dump. Incremental
        zips the pg_dump file.

        Returns the local path of the zipped dump file.
        """
        raise NotImplementedError

    @task()
    def upload_to_s3(dump_path: str) -> str:
        """
        Uploads the database dump file to S3.

        Uses the `aws_bluecore_s3` Airflow connection (S3Hook) and the
        `bluecore_backup_s3_bucket` Airflow Variable for the bucket name.
        Key pattern: backups/bluecore_db/<YYYY-MM-DD>.dump.zip

        Returns the S3 key of the uploaded file.
        """
        s3_key = upload_db_backup(dump_path=dump_path)
        logger.info(f"{dump_path} uploaded to S3 with key: {s3_key}")
        return s3_key

    @task()
    def cleanup(dump_path: str):
        """Removes the local dump file after a successful upload."""
        raise NotImplementedError

    dump_path = dump_database()
    s3_key = upload_to_s3(dump_path)
    cleanup(dump_path)


bluecore_db_backup_dag = bluecore_db_backup()
