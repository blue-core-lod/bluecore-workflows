"""Record Loader Workflow for a single file."""
import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

@dag(schedule_interval=None, 
     start_date=datetime(2025, 2, 20), 
     catchup=False,
     tags=['ingest', 'record'],
     default_args={"owner": "airflow"},)
def loader():
    @task()
    def ingest():
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("Missing params")
        file_path = params.get("file")
        if file_path is None:
            raise ValueError("Missing file path")
        logger.info(f"Ingesting {file_path}")
        return file_path

    @task()
    def process(*args, **kwargs):
        file_path = kwargs.get("file")
        logger.info(f"Processing data {file_path}")
        return []

    @task()
    def store(*args, **kwargs):
        record=kwargs.get("record")
        logger.info(f"Storing data {record}")

    file_path = ingest()
    records = process(file=file_path)
    store.expand(record=records)
loader()