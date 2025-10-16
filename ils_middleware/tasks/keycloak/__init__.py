import os

import httpx


KEYCLOAK_EXTERNAL_URL = os.environ.get(
    "KEYCLOAK_INTERNAL_URL", "http://localhost/keycloak"
).rstrip("/")
ADMIN_USERNAME = os.environ.get("KEYCLOAK_ADMIN", "admin")
ADMIN_PASSWORD = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")


def get_admin_token() -> str:
    token_response = httpx.post(
        f"{KEYCLOAK_EXTERNAL_URL}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "username": ADMIN_USERNAME,
            "password": ADMIN_PASSWORD,
            "client_id": "admin-cli",
        },
    )
    token_response.raise_for_status()
    return token_response.json()["access_token"]


def get_bluecore_members() -> dict:
    members: dict = {}
    all_groups_response = httpx.get(
        f"{KEYCLOAK_EXTERNAL_URL}/admin/realms/bluecore/groups",
        headers={"Authorization": f"Bearer {get_admin_token()}"},
    )
    all_groups_response.raise_for_status()
    for group in all_groups_response.json():
        group_id = group["id"]
        group_name = group["name"]
        members_request = httpx.get(
            f"{KEYCLOAK_EXTERNAL_URL}/admin/realms/bluecore/groups/{group_id}/members",
            headers={"Authorization": f"Bearer {get_admin_token()}"},
        )
        for member in members_request.json():
            if member["username"] in members:
                members[member["username"]]["groups"].append(group_name.casefold())
            else:
                members[member["username"]] = {
                    "email": member["email"],
                    "groups": [group_name.casefold()],
                }
    return members


def get_user_groups(username: str) -> list:
    user_profile_result = httpx.get(
        f"{KEYCLOAK_EXTERNAL_URL}/admin/realms/bluecore/users?username={username}",
        headers={"Authorization": f"Bearer {get_admin_token()}"},
    )

    user_profiles = user_profile_result.json()
    match len(user_profiles):
        case 0:
            raise ValueError(f"{username} not found")

        case 1:
            user_id = user_profiles[0]["id"]

        case _:
            raise ValueError(f"Multiple users found for username {username}")

    user_groups = httpx.get(
        f"{KEYCLOAK_EXTERNAL_URL}/admin/realms/bluecore/users/{user_id}/groups",
        headers={"Authorization": f"Bearer {get_admin_token()}"},
    )
    return [group["name"].lower().replace(" ", "") for group in user_groups.json()]
