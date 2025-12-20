"""Blue Core Login"""

import httpx

from airflow.models import Variable


def sinopia_login(**kwargs):
    """Log into Blue Core using Airflow Variables and retrieves JWT."""
    sinopia_user = Variable.get("sinopia_user")
    sinopia_password = Variable.get("sinopia_password")
    keycloak_url = Variable.get("keycloak_url")

    login_response = httpx.post(
        f"{keycloak_url}/keycloak/realms/bluecore/protocol/openid-connect/token",
        data={
            "client_id": "bluecore_api",
            "username": sinopia_user,
            "password": sinopia_password,
            "grant_type": "password",
        },
    )

    login_response.raise_for_status()

    return login_response.json().get("access_token")
