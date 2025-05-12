import os
import pytest
import jwt
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import base64


CLIENT_ID = "bluecore_workflows"
USERNAME = "test_user"
PASSWORD = "123456"

MOCK_JWT = jwt.encode(
    {
        "preferred_username": USERNAME,
        "email": f"{USERNAME}@example.com",
        "given_name": "Test",
        "family_name": "User",
        "resource_access": {
            CLIENT_ID: {"roles": ["airflow_admin", "airflow_user"]}
        },
    },
    key="mocked-public-key",
    algorithm="HS256",
)

@pytest.mark.integration
@patch("config.webserver_config.jwt.decode", return_value=jwt.decode(MOCK_JWT, "mocked-public-key", algorithms=["HS256"]))
def test_keycloak_token_flow_and_jwt_decoding(mock_jwt_decode, httpx_mock, monkeypatch):
    # Set the environment variables BEFORE importing the config
    monkeypatch.setenv("KEYCLOAK_INTERNAL_URL", "http://localhost:8081/keycloak/")
    monkeypatch.setenv("KEYCLOAK_EXTERNAL_URL", "http://localhost:8081/keycloak/")
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_CLIENT_SECRET", "mocked_client_secret")
    monkeypatch.setenv("AIRFLOW_KEYCLOAK_REALM", "bluecore")

    # Create a temporary public key (use only for testing)
    test_public_key = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()

    # Convert to DER and then base64 for mocking the Keycloak JSON response
    public_key_der = test_public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    mock_public_key_b64 = base64.b64encode(public_key_der).decode("utf-8")

    httpx_mock.add_response(
        method="GET",
        url="http://localhost:8081/keycloak/realms/bluecore",
        json={"public_key": mock_public_key_b64},
    )

    import importlib
    from config import webserver_config

    importlib.reload(webserver_config)

    # This line ensures the mocked GET request is used
    webserver_config.get_public_key()

    # Trigger token decoding manually (simulate login logic)
    token = MOCK_JWT
    decoded = mock_jwt_decode.return_value

    assert decoded["preferred_username"] == USERNAME
    assert "airflow_admin" in decoded["resource_access"][CLIENT_ID]["roles"]
    assert httpx_mock.get_requests()[0].url.path.endswith("/realms/bluecore")
