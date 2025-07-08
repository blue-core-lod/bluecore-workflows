"""Sinopia Operators and Functions for email notifications."""

from airflow.providers.amazon.aws.hooks.ses import SesHook

import logging


logger = logging.getLogger(__name__)


def send_notification_emails(**kwargs) -> None:
    # Send success emails first
    send_update_success_emails(**kwargs)

    # send failure notifications
    send_task_failure_notifications(**kwargs)


def send_update_success_emails(**kwargs) -> None:
    task_instance = kwargs["task_instance"]
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    ses_hook = SesHook(aws_conn_id="aws_ses_connection")
    for resource_uri in resources:
        resource_uuid = resource_uri.split("/")[-1]
        message = task_instance.xcom_pull(
            key=resource_uuid, task_ids="api-message-parse"
        )
        email_attributes = _email_on_success_attributes(message)
        ses_hook.send_email(**email_attributes)


def send_task_failure_notifications(**kwargs) -> None:
    parent_task_ids = list(kwargs["task"].upstream_task_ids)
    err_msg_context = {"parent_task_ids": parent_task_ids, "kwargs": kwargs}
    ses_hook = SesHook(aws_conn_id="aws_ses_connection")
    task_instance = kwargs["task_instance"]
    bad_resources = task_instance.xcom_pull(
        key="bad_resources", task_ids="api-message-parse"
    )

    for resource_uri in bad_resources or []:
        logger.error(  # type: ignore
            f"Unable to determine user to notify for resource: {resource_uri}",
            context=err_msg_context,
        )

    # Conversion Failures conversion_failures in process_symphony.rdf2marc
    failed_resources = task_instance.xcom_pull(
        key="conversion_failures", task_ids="process_symphony.rdf2marc"
    )
    for resource_uri in failed_resources or []:
        resource_uuid = resource_uri.split("/")[-1]
        message = task_instance.xcom_pull(
            key=resource_uuid, task_ids="api-message-parse"
        )
        email_attributes = _email_on_failure_attributes(message)
        ses_hook.send_email(**email_attributes)
        notify_and_log(
            f"Error executing upstream task for {resource_uri}", err_msg_context
        )


def notify_and_log(err_msg: str, err_msg_context: dict) -> None:
    logger.error(f"{err_msg}: err_msg_context={err_msg_context}")


def _email_on_failure_attributes(message: dict) -> dict:
    email_addr = message["email"]
    resource_uri = message["resource_uri"]
    group = message["group"]
    return {
        "mail_from": "sinopia-devs@lists.stanford.edu",
        "to": email_addr,
        "subject": "Error executing Sinopia to ILS task on your behalf",
        "html_content": f"Error processing resource_uri (if available): {resource_uri} / group (if available): {group}",
    }


def _email_on_success_attributes(message: dict) -> dict:
    email_addr = message["email"]
    resource_uri = message["resource_uri"]
    group = message["group"]
    target = message["target"]
    return {
        "mail_from": "sinopia-devs@lists.stanford.edu",
        "to": email_addr,
        "subject": f"successfully published {resource_uri}",
        "html_content": f"You have successfully published {resource_uri} from Sinopia to {group} {target}",
    }
