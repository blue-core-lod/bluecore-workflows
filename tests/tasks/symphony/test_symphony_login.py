"""Test Symphony Login."""

import pytest
import requests  # type: ignore

from airflow.hooks.base import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.login import SymphonyLogin


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_login_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.status_code = 200
        new_result.text = "Successful login"
        new_result.json = lambda: {"sessionToken": "234566"}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


def test_subscribe_operator(mock_connection, mock_login_request):
    """Test with typical kwargs for SymphonyLogin."""
    task_login = SymphonyLogin(
        conn_id="symphony_dev_login",
        login="DEVSYS",
        password="APASSWord",
    )
    assert task_login == "234566"
