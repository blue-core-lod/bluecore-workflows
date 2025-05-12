import os
import logging

from base64 import b64decode

import httpx
import jwt

from cryptography.hazmat.primitives import serialization
from tokenize import Exponent

from airflow.providers.fab.auth_manager.security_manager.override import (
    AUTH_OAUTH,
    FabAirflowSecurityManagerOverride,
)

from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthOAuthView

basedir = os.path.abspath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

AUTH_ROLE_ADMIN = "Admin"
AUTH_TYPE = AUTH_OAUTH

AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"

# ==============================================================================
# Defines Keycloak roles mapped to Airflow access roles.
# ------------------------------------------------------------------------------
# Each Keycloak role listed here must be created in the corresponding realm
# and assigned to users. When a user is assigned one of these Keycloak roles,
# they will inherit the mapped Airflow role.
#
# Example: Assigning the "airflow_admin" role to a user in Keycloak grants
# them "Admin" access in Airflow.
# ==============================================================================
AUTH_ROLES_MAPPING = {
    "airflow_admin": ["Admin"],
    "airflow_op": ["Op"],
    "airflow_user": ["User"],
    "airflow_viewer": ["Viewer"],
    "airflow_public": ["Public"],
}

# Pull .env constants ==========================================================
CLIENT_ID = os.getenv("AIRFLOW_KEYCLOAK_CLIENT_ID")
CLIENT_SECRET = os.getenv("AIRFLOW_KEYCLOAK_CLIENT_SECRET")
REALM = os.getenv("AIRFLOW_KEYCLOAK_REALM")
KEYCLOAK_INTERNAL_URL = os.getenv("KEYCLOAK_INTERNAL_URL")
KEYCLOAK_EXTERNAL_URL = os.getenv("KEYCLOAK_EXTERNAL_URL")

# Build URLs ===================================================================
### External URLs (for outside docker environment) ###
OIDC_ISSUER_EXTERNAL = (
    f"{KEYCLOAK_EXTERNAL_URL}realms/{REALM}"  # For redirect_uri in browser
)
OIDC_BASE_EXTERNAL_URL = f"{OIDC_ISSUER_EXTERNAL}/protocol/openid-connect"

### Internal URLs (for inside docker environment) ###
OIDC_ISSUER_INTERNAL = f"{KEYCLOAK_INTERNAL_URL}realms/{REALM}"  # For airflow-to-keycloak communication (httpx, etc.)
OIDC_BASE_INTERNAL_URL = f"{OIDC_ISSUER_INTERNAL}/protocol/openid-connect"

### Authorization URLs ###
OIDC_TOKEN_URL = f"{OIDC_BASE_INTERNAL_URL}/token"
OIDC_AUTH_URL = f"{OIDC_BASE_EXTERNAL_URL}/auth"

# Debug lines to ensure proper env variables are set and urls are correct
RESET = "\033[0m"
BOLD = "\033[1m"
MAGENTA = "\033[95m"
print()
print(f"{MAGENTA}{'#' * 71}{RESET}")
print(f"{BOLD}{MAGENTA}OIDC_ISSUER_INTERNAL:{RESET} {OIDC_ISSUER_INTERNAL}")
print(f"{BOLD}{MAGENTA}OIDC_ISSUER_EXTERNAL:{RESET} {OIDC_ISSUER_EXTERNAL}")
print(f"{BOLD}{MAGENTA}OIDC_BASE_INTERNAL_URL:{RESET} {OIDC_BASE_INTERNAL_URL}")
print(f"{BOLD}{MAGENTA}OIDC_BASE_EXTERNAL_URL:{RESET} {OIDC_BASE_EXTERNAL_URL}")
print(f"{BOLD}{MAGENTA}OIDC_TOKEN_URL:{RESET} {OIDC_TOKEN_URL}")
print(f"{BOLD}{MAGENTA}OIDC_AUTH_URL:{RESET} {OIDC_AUTH_URL}")
print(f"{BOLD}{MAGENTA}CLIENT_SECRET:{RESET} {CLIENT_SECRET}")
print(f"{MAGENTA}{'#' * 71}{RESET}")
print()

PROVIDER_NAME = "keycloak"
OAUTH_PROVIDERS = [
    {
        "name": PROVIDER_NAME,
        "token_key": "access_token",
        "icon": "fa-sign-in",
        "remote_app": {
            "api_base_url": f"{OIDC_BASE_INTERNAL_URL}/",
            "access_token_url": OIDC_TOKEN_URL,
            "authorize_url": OIDC_AUTH_URL,
            "request_token_url": None,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "client_kwargs": {"scope": "email profile"},
        },
    }
]


def load_keycloak_public_key():
    req = httpx.get(OIDC_ISSUER_INTERNAL)
    key_der_base64 = req.json()["public_key"]
    key_def = b64decode(key_der_base64)
    print()
    print(f"{MAGENTA}{'#' * 71}{RESET}")
    print(f"{BOLD}{MAGENTA}req status:{RESET} {req}")
    print(f"{MAGENTA}{'#' * 71}{RESET}")
    print()
    return serialization.load_der_public_key(key_def)


def get_public_key():
    return load_keycloak_public_key()


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    def oauth_user_info(self, provider, response):
        user_info = {}
        if provider == PROVIDER_NAME:
            token = response.get("access_token")
            decoded_jwt = jwt.decode(
                token,
                get_public_key(),
                algorithms=["HS256", "RS256"],
                audience=CLIENT_ID,
            )
            roles = decoded_jwt["resource_access"][CLIENT_ID]["roles"]
            if not roles:
                roles = ["airflow_public"]
            else:
                roles = [role for role in roles if "airflow" in role]
            user_info["username"] = decoded_jwt.get("preferred_username")
            user_info["email"] = decoded_jwt.get("email")
            user_info["first_name"] = decoded_jwt.get("given_name")
            user_info["last_name"] = decoded_jwt.get("family_name")
            user_info["role_keys"] = roles
            logger.info(f"User is logging in with info: {user_info}")
        return user_info


SECURITY_MANAGER_CLASS = CustomSecurityManager
