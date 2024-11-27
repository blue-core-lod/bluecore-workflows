import pytest
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.overlay import overlay_marc_in_symphony

from tasks import test_task_instance, mock_task_instance  # noqa: F401

task_instance = test_task_instance()

CATKEY = "320011"
MARC_JSON = {
    "leader": "11222999   adf",
    "fields": [{"tag": "245"}],
    "SIRSI": [
        CATKEY,
    ],
}
MARC_JSON_NO_CAT_KEY = {"leader": "11222999   adf", "fields": [{"tag": "245"}]}


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_new_request(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        new_result = mocker.stub(name="put_result")
        new_result.token = "234566"
        new_result.status_code = 201
        new_result.text = "Successful modified"
        new_result.json = lambda: {"@key": CATKEY}
        return new_result

    monkeypatch.setattr(requests, "put", mock_put)


def test_overlay_marc_in_symphony(
    mock_new_request, mock_connection, mock_task_instance  # noqa: F811
):
    overlay_marc_in_symphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        catkey=CATKEY,
        marc_json=MARC_JSON,
    )
    assert task_instance.xcom_pull(key="0000-1111-2222-3333").startswith(CATKEY)


def test_missing_catkey(
    mock_new_request, mock_connection, mock_task_instance  # noqa: F811
):  # noqa: F811
    overlay_marc_in_symphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
    )
    assert len(task_instance.xcom_pull(key="missing_catkeys")) > 0
