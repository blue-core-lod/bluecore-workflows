"""Tests Symphony Request"""

import pytest
import requests  # type: ignore

from airflow.hooks.base import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.request import SymphonyRequest


@pytest.fixture
def mock_sinopia_env(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SINOPIA_ENV", "test")


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.status_code = 200
        new_result.text = "Successful request"
        new_result.json = lambda: {}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


def test_missing_request_filter(mock_sinopia_env, mock_connection, mock_request):
    result = SymphonyRequest(token="34567")
    assert result.startswith("Successful request")


def test_unknown_http_verb(mock_sinopia_env, mock_connection, mock_request):
    with pytest.raises(ValueError, match="get not available"):
        SymphonyRequest(token="45567", http_verb="get", endpoint="catalog/bib/3455")
