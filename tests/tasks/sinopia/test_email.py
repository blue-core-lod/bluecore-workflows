"""Test Sinopia email notification sending."""

import pytest
from pytest_mock import MockerFixture
from datetime import datetime

from airflow import DAG

from airflow.providers.amazon.aws.hooks.ses import SesHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.email import (
    send_notification_emails,
    send_update_success_emails,
    send_task_failure_notifications,
    logger,  # for spying on logging
)

from tasks import test_task_instance, mock_task_instance  # noqa: F401

mock_messages_list = [
    {
        "MessageId": "1234ab4f-52ab-4b54-a6cd-098b4321f02a",
        "ReceiptHandle": "real+long+string",
        "MD5OfBody": "8bf1ff18a27bba7cc54fc68dc3ca9d5a",
        "Body": '{"resource":{"uri":"http://exmpl/resource/foo"},"user":{"email":"abc@yale.edu"},"group":"stanford","target":"ils"}',  # noqa: E501 line too long
    },
    {
        "MessageId": "5678ab4f-52ab-4b54-a6cd-765b9876f08a",
        "ReceiptHandle": "real+long+string2",
        "MD5OfBody": "388e4db0aacfbcbe93e6072dd7cda916",
        "Body": '{"resource":{"uri":"http://exmpl/resource/bar"},"user":{"email":"abc@yale.edu"},"group":"yale","target":"ils"}',  # noqa: E501 line too long
    },
]


def test_task():
    start_date = datetime(2021, 9, 20)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


def test_send_notification_emails(mocker: MockerFixture):
    patched_send_update_success_emails = mocker.patch(
        "ils_middleware.tasks.sinopia.email.send_update_success_emails",
        return_value=True,
    )
    patched_send_task_failure_notifications = mocker.patch(
        "ils_middleware.tasks.sinopia.email.send_task_failure_notifications",
        return_value=True,
    )
    send_notification_emails(task_instance=test_task_instance())
    patched_send_update_success_emails.assert_called_once
    patched_send_task_failure_notifications.assert_called_once


@pytest.fixture
def mock_failure_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs) -> list:
        if kwargs["key"] == "email":
            return ["user@institution.edu"]
        if kwargs["key"] == "resource_uri":
            return [
                "https://api.sinopia.io/resource/9d3b525e-2d8f-4192-8456-0fb804d34fd1"
            ]

        return [None]

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


@pytest.fixture
def mock_failure_no_user_available_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs) -> list:
        return []

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_send_update_success_emails(
    mock_task_instance, mocker: MockerFixture  # noqa: F811
) -> None:
    task_instance = test_task_instance()

    mock_ses_hook_obj = mocker.Mock(SesHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SesHook", return_value=mock_ses_hook_obj
    )
    send_update_success_emails(task_instance=task_instance)

    patched_ses_hook_class.assert_called_once_with(aws_conn_id="aws_ses_connection")
    mock_ses_hook_obj.send_email.assert_any_call(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": "dscully@stanford.edu",
            "subject": "successfully published https://api.development.sinopia.io/resource/0000-1111-2222-3333",
            "html_content": "You have successfully published https://api.development.sinopia.io/resource/0000-1111-2222-3333 from Sinopia to stanford ils",  # noqa: E501
        }
    )
    mock_ses_hook_obj.send_email.assert_any_call(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": "fmulder@stanford.edu",
            "subject": "successfully published https://api.development.sinopia.io/resource/4444-5555-6666-7777",
            "html_content": "You have successfully published https://api.development.sinopia.io/resource/4444-5555-6666-7777 from Sinopia to yale ils",  # noqa: E501
        }
    )
    assert mock_ses_hook_obj.send_email.call_count == 2


def test_send_task_failure_notifications(
    mock_task_instance, mocker: MockerFixture  # noqa: F811
) -> None:
    execution_date = datetime(2021, 9, 21)
    task_instance = test_task_instance()

    logger_spy = mocker.spy(logger, "error")

    mock_ses_hook_obj = mocker.Mock(SesHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SesHook", return_value=mock_ses_hook_obj
    )

    send_task_failure_notifications(
        execution_date=execution_date, task=test_task(), task_instance=task_instance
    )

    expected_kwargs = {
        "execution_date": execution_date,
        "task": test_task(),
        "task_instance": task_instance,
    }
    expected_err_context = {"parent_task_ids": [], "kwargs": expected_kwargs}
    logger_spy.assert_called_with(
        f"Error executing upstream task for https://api.development.sinopia.io/resource/8888-9999-0000-1111: err_msg_context={expected_err_context}"  # noqa: E501
    )

    patched_ses_hook_class.assert_called_with(aws_conn_id="aws_ses_connection")
    expected_uri = "https://api.development.sinopia.io/resource/8888-9999-0000-1111"
    mock_ses_hook_obj.send_email.assert_called_once_with(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": "fmulder@stanford.edu",
            "subject": "Error executing Sinopia to ILS task on your behalf",
            "html_content": f"Error processing resource_uri (if available): {expected_uri} / group (if available): yale",  # noqa: E501 line too long
        }
    )
