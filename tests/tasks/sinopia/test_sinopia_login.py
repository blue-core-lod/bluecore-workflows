"""Tests Sinopia Login Function."""

import pytest

from airflow.models import Variable

from ils_middleware.tasks.sinopia.login import httpx, sinopia_login


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, default=None):
        value = None
        match key.lower():

            case "sinopia_user":
                value = "ils_middleware"

            case "sinopia_password":
                value = "a_pwd_1234"

            case "keycloak_url":
                value = "http://bcld.info/keycloak"

        return value

    monkeypatch.setattr(Variable, "get", mock_get)

@pytest.fixture
def mock_keycloak_post(mocker, monkeypatch):
    def mock_post(*args, **kwargs):
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status = lambda : None
        mock_response.json = lambda : { "access_token": "abc12345"}
        return mock_response

    monkeypatch.setattr(httpx, "post", mock_post)

def test_sinopia_login(mock_variable, mock_keycloak_post):

    jwt = sinopia_login()
    assert jwt == "abc12345"
