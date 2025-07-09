import logging
import json
from urllib.parse import urlparse
from os import path

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

logger = logging.getLogger(__name__)


def Rdf2Marc(**kwargs):
    """Runs rdf2marc on a BF Instance URL"""
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    conversion_failures = {}

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        rdf2marc_lambda = kwargs.get("rdf2marc_lambda")

        s3_bucket = kwargs.get("s3_bucket")
        s3_record_path = f"airflow/{instance_id}/record"
        marc_path = f"{s3_record_path}.mar"
        marc_text_path = f"{s3_record_path}.txt"
        marc_err_path = f"{s3_record_path}.err"

        lambda_hook = LambdaHook(
            aws_conn_id="aws_lambda_connection",
        )

        params = {
            "instance_uri": instance_uri,
            "bucket": s3_bucket,
            "marc_path": marc_path,
            "marc_txt_path": marc_text_path,
            "error_path": marc_err_path,
        }

        result = lambda_hook.invoke_lambda(
            function=rdf2marc_lambda,
            log_type="None",
            qualifier="$LATEST",
            invocation_type="RequestResponse",
            payload=json.dumps(params),
        )

        if "x-amz-function-error" in result["ResponseMetadata"].get("HTTPHeaders"):
            payload = json.loads(result["Payload"].read())
            msg = f"RDF2MARC conversion failed for {instance_uri}, error: {payload.get('errorMessage')}"
            conversion_failures[instance_uri] = msg
        elif result["StatusCode"] == 200:
            instance_uuid = instance_uri.split("/")[-1]
            task_instance.xcom_push(key=instance_uuid, value=marc_path)
        else:
            msg = f"RDF2MARC conversion failed for {instance_uri}: {result['FunctionError']}"
            conversion_failures[instance_uri] = msg

    task_instance.xcom_push(key="conversion_failures", value=conversion_failures)
