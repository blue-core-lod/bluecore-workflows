"""Record Loader Workflow for a single file."""
import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from ils_middleware.tasks.amazon.bluecore_records_s3 import get_file
from ils_middleware.tasks.bluecore.batch import is_zip, parse_file_to_graph

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
        local_file_path = get_file(file=file_path)
        return local_file_path

    @task.branch(task_id="process_choice")
    def choose_processing(**kwargs):
        file_path = kwargs.get("file")
        if is_zip(file_path):
            return ['process_zip']
        return ['process']

    @task(trigger_rule="one_success")
    def process(*args, **kwargs):
        file_path = kwargs.get("file")
        logger.info(f"Processing data {file_path}")
        resources = parse_file_to_graph(file_path)
        logger.info(f"{len(resources)} Resources to process")
        return resources

    @task
    def process_zip(**kwargs):
        logger.info(f"Process zip file")

    @task(trigger_rule="one_success")
    def store(*args, **kwargs):
        record=kwargs.get("record")
        logger.info(f"Storing data {record}")

    file_path = ingest()
    next_task = choose_processing(file=file_path) 
    records = process(file=file_path)
    process_zip_task = process_zip(file=file_path)

    next_task >> [records, process_zip_task]
    process_zip_task >> records
    store.expand(record=records)
loader()