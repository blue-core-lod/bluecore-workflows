"""Test Symphony Operators and functions."""

import io
import pytest


from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc

from tasks import test_task_instance, mock_task_instance  # noqa: F401

mock_200_response = {
    "Payload": io.StringIO("{}"),
    "ResponseMetadata": {"HTTPHeaders": {}},
    "StatusCode": 200,
}

task_instance = test_task_instance()


@pytest.fixture
def mock_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return mock_200_response

    monkeypatch.setattr(LambdaHook, "invoke_lambda", mock_invoke_lambda)


def test_Rdf2Marc(mock_task_instance, mock_lambda):  # noqa: F811
    Rdf2Marc(task_instance=task_instance)
    assert (
        task_instance.xcom_pull(
            key="0000-1111-2222-3333"
        )
        == "airflow/0000-1111-2222-3333/record.mar"
    )
    assert (
        task_instance.xcom_pull(
            key="4444-5555-6666-7777"
        )
        == "airflow/4444-5555-6666-7777/record.mar"
    )


@pytest.fixture
def mock_failed_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return {
            "StatusCode": 200,
            "Payload": io.StringIO(
                """{ "errorMessage": "AdminMetadata (bf:adminMetadata) not specified for Instance"}"""
            ),
            "ResponseMetadata": {"HTTPHeaders": {"x-amz-function-error": "Unhandled"}},
        }

    monkeypatch.setattr(LambdaHook, "invoke_lambda", mock_invoke_lambda)


def test_Rdf2Marc_LambdaError(mock_task_instance, mock_failed_lambda):  # noqa: F811
    Rdf2Marc(task_instance=task_instance)
    conversion_failures = task_instance.xcom_pull(key="conversion_failures")
    assert len(conversion_failures) == 2
    assert (
        "https://api.development.sinopia.io/resource/0000-1111-2222-3333"
        in conversion_failures
    )
    assert (
        "https://api.development.sinopia.io/resource/4444-5555-6666-7777"
        in conversion_failures
    )
