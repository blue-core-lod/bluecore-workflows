"""Test FOLIO Operators and functions."""

import pytest
import requests

from pytest_mock import MockerFixture

from ils_middleware.tasks.folio.login import FolioLogin


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "some_jwt_token"}

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


# <Response [201]>
def test_valid_login(mock_request):
    assert FolioLogin(username="DEVSYS", password="APASSWord") == "some_jwt_token"


def test_missing_username():
    with pytest.raises(KeyError, match="username"):
        FolioLogin()


def test_missing_password():
    with pytest.raises(KeyError, match="password"):
        FolioLogin(username="DEVSYS")
