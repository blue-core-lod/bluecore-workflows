"""Test the AWS S3 tasks properly name and load files."""

import json
import pytest
from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3

from tasks import test_task_instance, mock_task_instance, marc_as_json


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "sinopia-marc-test")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "tests/fixtures/record.mar"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)


@pytest.fixture
def mock_s3_load_string():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string"
    ) as mocked:
        yield mocked


def test_get_from_s3(mock_env_vars, mock_s3_hook, mock_task_instance):
    """Test downloading a file from S3 into a temp file"""
    get_from_s3(task_instance=test_task_instance())
    assert (
        test_task_instance().xcom_pull(
            key="0000-1111-2222-3333"
        )
        == "tests/fixtures/record.mar"
    )
    assert (
        test_task_instance().xcom_pull(
            key="4444-5555-6666-7777"
        )
        == "tests/fixtures/record.mar"
    )


def test_send_to_s3(mock_env_vars, mock_s3_load_string, mock_task_instance):
    """Test sending a file to s3"""

    send_to_s3(task_instance=test_task_instance())
    mock_s3_load_string.call_count == 2
    marc_json = marc_as_json()
    assert (
        json.loads(
            test_task_instance().xcom_pull(
                key="0000-1111-2222-3333"
            )
        )
        == marc_json
    )
    assert (
        json.loads(
            test_task_instance().xcom_pull(
                key="4444-5555-6666-7777"
            )
        )
        == marc_json
    )


@pytest.fixture
def failed_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key.startswith("resources"):
            return ["https://api.development.sinopia.io/resource/rdf2marc-no-work"]
        if key.endswith("conversion_failures"):
            return {
                "https://api.development.sinopia.io/resource/rdf2marc-no-work": "Error message from rdf2marc"
            }

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_get_from_s3_error(failed_task_instance, mock_env_vars):
    """Tests skipping an upstream rdf2marc failure"""
    get_from_s3(task_instance=test_task_instance())

    assert (
        test_task_instance().xcom_pull(
            key="https://api.development.sinopia.io/resource/rdf2marc-no-work"
        )
        is None
    )


def test_send_to_s3_error(mock_env_vars, failed_task_instance):
    send_to_s3(task_instance=test_task_instance())

    assert (
        test_task_instance().xcom_pull(
            key="https://api.development.sinopia.io/resource/rdf2marc-no-work"
        )
        is None
    )
