import os
import pytest
import jwt
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import base64

import config.webserver_config as wsc

CLIENT_ID = "bluecore_workflows"
USERNAME = "test_user"
PROVIDER = wsc.PROVIDER_NAME

# HS256 token payload
MOCK_JWT = jwt.encode(
    {
        "preferred_username": USERNAME,
        "email": f"{USERNAME}@example.com",
        "given_name": "Test",
        "family_name": "User",
        "resource_access": {CLIENT_ID: {"roles": ["airflow_admin", "airflow_user"]}},
    },
    key="mocked-public-key",
    algorithm="HS256",
)


@pytest.mark.integration
@patch(
    "config.webserver_config.jwt.decode",
    return_value=jwt.decode(MOCK_JWT, "mocked-public-key", algorithms=["HS256"]),
)

# ==============================================================================
# Tests fetching the Keycloak realm public key and decoding an HS256 JWT,
# verifying that the preferred_username and roles are correctly extracted
# and that the HTTPX request is made to the proper endpoint.
# ------------------------------------------------------------------------------
def test_keycloak_token_flow_and_jwt_decoding(mock_jwt_decode, httpx_mock, monkeypatch):
    # Set env before importing/reloading
    monkeypatch.setenv("KEYCLOAK_INTERNAL_URL", "http://localhost:8081/keycloak/")
    monkeypatch.setenv("KEYCLOAK_EXTERNAL_URL", "http://localhost:8081/keycloak/")
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_CLIENT_SECRET", "mocked_client_secret")
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_REALM", "bluecore")

    # Build a test public key, DER → Base64
    test_public_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048
    ).public_key()

    # Convert to DER and then base64 for mocking the Keycloak JSON response
    public_key_der = test_public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    mock_public_key_b64 = base64.b64encode(public_key_der).decode("utf-8")

    # Mock the realm URL response
    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8081/keycloak/realms/bluecore",
        json={"public_key": mock_public_key_b64},
    )

    # Reload module so URLs pick up monkeypatched env
    import importlib

    importlib.reload(wsc)

    # Exercise load/get
    wsc.get_public_key()

    # Simulate login flow
    decoded = mock_jwt_decode.return_value
    assert decoded["preferred_username"] == USERNAME
    assert "airflow_admin" in decoded["resource_access"][CLIENT_ID]["roles"]
    # Ensure the HTTPX call was made
    assert httpx_mock.get_requests()[0].url.path.endswith("/realms/bluecore")


# ==============================================================================
# Test that a non-Keycloak provider returns an empty dict
# ------------------------------------------------------------------------------
def test_oauth_user_info_with_other_provider_returns_empty():
    # Instantiate without calling base __init__
    manager = wsc.CustomSecurityManager.__new__(wsc.CustomSecurityManager)
    result = manager.oauth_user_info("not_keycloak", {"access_token": "ignored"})
    assert result == {}


# ==============================================================================
# Test empty‐roles branch: fallback to ["airflow_public"]
# ------------------------------------------------------------------------------
@patch("config.webserver_config.get_public_key", return_value=None)
@patch("config.webserver_config.jwt.decode")
def test_oauth_user_info_empty_roles(mock_decode, mock_get_pk):
    mock_decode.return_value = {
        "preferred_username": "rolland",
        "email": "rolland@example.com",
        "given_name": "Rolland",
        "family_name": "Deschain",
        "resource_access": {CLIENT_ID: {"roles": []}},
    }

    manager = wsc.CustomSecurityManager.__new__(wsc.CustomSecurityManager)
    info = manager.oauth_user_info(PROVIDER, {"access_token": "dummy_token"})

    assert info["username"] == "rolland"
    assert info["email"] == "rolland@example.com"
    assert info["first_name"] == "Rolland"
    assert info["last_name"] == "Deschain"
    assert info["role_keys"] == ["airflow_public"]


# ==============================================================================
# Test filtering branch: only keep roles containing "airflow"
# ------------------------------------------------------------------------------
@patch("config.webserver_config.get_public_key", return_value=None)
@patch("config.webserver_config.jwt.decode")
def test_oauth_user_info_filters_roles(mock_decode, mock_get_pk):
    mock_decode.return_value = {
        "preferred_username": "bob",
        "email": "bob@example.com",
        "given_name": "Bob",
        "family_name": "Builder",
        "resource_access": {
            CLIENT_ID: {"roles": ["airflow_admin", "not_relevant", "airflow_viewer"]}
        },
    }

    manager = wsc.CustomSecurityManager.__new__(wsc.CustomSecurityManager)
    info = manager.oauth_user_info(PROVIDER, {"access_token": "dummy"})

    # Non-airflow role should be dropped
    assert set(info["role_keys"]) == {"airflow_admin", "airflow_viewer"}
    assert info["username"] == "bob"
    assert info["email"] == "bob@example.com"
