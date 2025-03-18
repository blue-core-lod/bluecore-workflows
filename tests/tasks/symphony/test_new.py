"""Tests new MARC record in Symphony task."""

import pytest
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.new import NewMARCtoSymphony

from tasks import test_task_instance, mock_task_instance  # noqa: F401

task_instance = test_task_instance()


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_new_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.token = "234566"
        new_result.status_code = 200
        new_result.text = "Successful creation"
        new_result.json = lambda: {"@key": "45678"}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


def test_NewMARCtoSymphony(
    mock_connection,
    mock_new_request,
    mock_task_instance,  # noqa: F811
):  # noqa: F811
    """Tests NewMARCtoSymphony"""
    NewMARCtoSymphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        library_key="GREEN",
        marc_json={"leader": "11222999   adf", "fields": [{"tag": "245"}]},
    )
    assert task_instance.xcom_pull(key="0000-1111-2222-3333") == "45678"


@pytest.fixture
def mock_failed_request(monkeypatch, mocker: MockerFixture):
    def mock_failed_post(*args, **kwargs):
        failed_result = mocker.stub(name="post_result")
        failed_result.status_code = 401
        failed_result.text = "Unauthorized"
        return failed_result

    monkeypatch.setattr(requests, "post", mock_failed_post)
