import logging
from urllib.parse import urlparse
import os
from os import path

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader

logger = logging.getLogger(__name__)


def get_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")
    conversion_failures = task_instance.xcom_pull(
        key="conversion_failures", task_ids="process_symphony.rdf2marc"
    )
    for instance_uri in resources:
        if conversion_failures and instance_uri in conversion_failures:
            logger.debug(f"{instance_uri} in {conversion_failures.keys()}")
            continue
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        temp_file = s3_hook.download_file(
            key=f"marc/airflow/{instance_id}/record.mar",
            bucket_name=Variable.get("marc_s3_bucket"),
        )
        instance_uuid = instance_uri.split("/")[-1]
        task_instance.xcom_push(key=instance_uuid, value=temp_file)


def send_to_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")
    conversion_failures = task_instance.xcom_pull(
        key="conversion_failures", task_ids="process_symphony.rdf2marc"
    )
    for instance_uri in resources:
        if conversion_failures and instance_uri in conversion_failures:
            logger.debug(f"{instance_uri} in {conversion_failures.keys()}")
            continue
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]
        temp_file = task_instance.xcom_pull(
            key=instance_id, task_ids="process_symphony.download_marc"
        )
        marc_record = marc_record_from_temp_file(instance_id, temp_file)
        s3_hook.load_string(
            marc_record.as_json(),
            f"marc/airflow/{instance_id}/record.json",
            Variable.get("marc_s3_bucket"),
            replace=True,
        )
        task_instance.xcom_push(key=instance_id, value=marc_record.as_json())


def marc_record_from_temp_file(instance_id, temp_file):
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
