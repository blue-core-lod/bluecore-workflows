import logging
import pathlib

import httpx
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


def get_file(**kwargs):
    aws_conn_id = kwargs.get("aws_conn_id", "aws_bluecore_s3")
    file_str = kwargs.get("file")
    if file_str.startswith("s3"):
        bucket_name = kwargs["bucket"]
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        local_file_path = s3_hook.download_file(
            key=file_str,
            bucket_name=bucket_name,
            preseve_file_name=True,
            use_autogenerated_subdir=True,
        )
    elif file_str.startswith("http"):
        airflow_path = pathlib.Path(kwargs.get("airflow", "/opt/airflow/"))
        file_name = file_str.split("/")[-1]
        local_file_path = airflow_path / f"uploads/{file_name}"
        get_result = httpx.get(file_str)
        get_result.raise_for_status()
        with local_file_path.open("wb+") as fo:
            fo.write(get_result.content)
    elif file_str.startswith("/opt/airflow"):
        local_file_path = pathlib.Path(file_str)
    return str(local_file_path)
