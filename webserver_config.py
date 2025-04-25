import os
import logging

from base64 import b64decode

import httpx
import jwt

from cryptography.hazmat.primitives import serialization
from tokenize import Exponent

from airflow.providers.fab.auth_manager.security_manager.override import (
    AUTH_OAUTH,
    FabAirflowSecurityManagerOverride
)

from flask_appbuilder import expose
from flask_appbuilder.security.views import AuthOAuthView

basedir = os.path.abspath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)

WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

AUTH_ROLE_ADMIN = 'Admin'
AUTH_TYPE = AUTH_OAUTH

AUTH_USER_REGISTRATION_ROLE = "Public"

AUTH_ROLES_MAPPING = {
    "airflow_admin": ["Admin"],
    "airflow_op": ["Op"],
    "airflow_user": ["User"],
    "airflow_viewer": ["Viewer"],
    "airflow_public": ["Public"],
}

PROVIDER_NAME = "keycloak"
CLIENT_ID = os.getenv("AIRFLOW_KEYCLOAK_CLIENT_ID")
CLIENT_SECRET = os.getenv("AIRFLOW_KEYCLOAK_CLIENT_SECRET")

OIDC_ISSURER = os.getenv("OIDC_ISSUER", "http://keycloak.3206bluecoreterraform.orb.local/keycloak/realms/bluecore")
OIDC_BASE_URL = f"{OIDC_ISSURER}/protocol/openid-connect"
OIDC_TOKEN_URL = f"{OIDC_BASE_URL}/token"
OIDC_AUTH_URL = f"{OIDC_BASE_URL}/auth"

OAUTH_PROVIDERS = [
    {
        "name": PROVIDER_NAME,
        "token_key": "access_token",
        "icon": "fa-sign-in",
        "remote_app": {
            "api_base_url": f"{OIDC_BASE_URL}/",
            "access_token_url": OIDC_TOKEN_URL,
            "authorize_url": OIDC_AUTH_URL,
            "request_token_url": None,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "client_kwargs": {
                "scope": "email profile"
            }
        }
    }        
]

req = httpx.get(OIDC_ISSURER)
key_der_base64 = req.json()["public_key"]
key_def = b64decode(key_der_base64)
public_key = serialization.load_der_public_key(key_def)

class CustomSecurityManager(FabAirflowSecurityManagerOverride):


    def oauth_user_info(self, provider, response):
        user_info = {}
        if provider == PROVIDER_NAME:
            token = response.get("access_token")
            decoded_jwt = jwt.decode(
                token,
                public_key,
                algorithms=["HS256", "RS256"],
                audience=CLIENT_ID
            )
            roles = decoded_jwt["resource_access"][CLIENT_ID]["roles"]
            if not roles:
                roles = ["airflow_public"]
            else:
                roles = [role for role in roles if "airflow" in role]
            user_info["username"] = decoded_jwt.get("preferred_username")
            user_info["email"] = decoded_jwt.get("email")
            user_info["first_name"] = decoded_jwt.get("given_name")
            user_info["last_name"] =  decoded_jwt.get("family_name")
            user_info["role_keys"] = roles
            logger.info(f"User is logging in with info: {user_info}")
        return user_info
    
SECURITY_MANAGER_CLASS = CustomSecurityManager