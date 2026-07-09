import pytest

from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import (
    KeycloakAuthManager,
)

CLIENT_ID = "bluecore_workflows"
CLIENT_SECRET = "mocked_client_secret"
REALM = "bluecore"
SERVER_URL = "http://keycloak:8080/keycloak/"


@pytest.fixture
def keycloak_auth_manager_config(monkeypatch):
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET", CLIENT_SECRET)
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__REALM", REALM)
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__SERVER_URL", SERVER_URL)


# ==============================================================================
# Verifies our AIRFLOW__KEYCLOAK_AUTH_MANAGER__* config wires up correctly into
# the client python-keycloak uses to talk to the Keycloak server.
# ------------------------------------------------------------------------------
def test_get_keycloak_client_uses_airflow_config(keycloak_auth_manager_config):
    client = KeycloakAuthManager.get_keycloak_client()

    assert client.client_id == CLIENT_ID
    assert client.realm_name == REALM
    assert client.connection.base_url == SERVER_URL


# ==============================================================================
# Explicit client_id/client_secret (used for service accounts) override config,
# but realm/server_url still come from Airflow config.
# ------------------------------------------------------------------------------
def test_get_keycloak_client_with_explicit_credentials(keycloak_auth_manager_config):
    client = KeycloakAuthManager.get_keycloak_client(
        client_id="other_client", client_secret="other_secret"
    )

    assert client.client_id == "other_client"
    assert client.realm_name == REALM
    assert client.connection.base_url == SERVER_URL


# ==============================================================================
# client_id and client_secret must be provided together or not at all.
# ------------------------------------------------------------------------------
def test_get_keycloak_client_requires_both_or_neither_credential(
    keycloak_auth_manager_config,
):
    with pytest.raises(ValueError):
        KeycloakAuthManager.get_keycloak_client(client_id="other_client")

    with pytest.raises(ValueError):
        KeycloakAuthManager.get_keycloak_client(client_secret="other_secret")
