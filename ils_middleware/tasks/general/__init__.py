import httpx

from airflow.sdk import get_current_context

from ils_middleware.tasks.keycloak import get_bluecore_members


def get_user(username: str) -> dict:
    """Retrieves user email and groups from Keycloak"""
    bluecore_members = get_bluecore_members()
    return bluecore_members[username]


def get_resource(resource_uri: str) -> dict:
    """Retrieves the Resource from Bluecore API"""
    result = httpx.get(resource_uri)
    result.raise_for_status()
    return result.json()


def message_from_context(**kwargs):
    """
    Retrieves messages from context
    """
    context = kwargs.get("context")

    if context is None:
        context = get_current_context()

    params = context.get("params")

    message = params.get("message")

    if message is None:
        raise ValueError("Cannot initialize DAG, no message present")

    return message


def parse_messages(**kwargs) -> str:
    """Parses and checks for existing resource in message."""
    task_instance = kwargs["task_instance"]
    message = kwargs["message"]

    resources = []
    resources_with_errors = []

    resource_payload = get_resource(message["resource"])
    resource_uri = resource_payload["uri"]
    user_payload = get_user(message["user"])
    if message["group"] not in user_payload["groups"]:
        raise ValueError(
            f"Cannot export: user {message['user']} not in group {message['group']}"
        )
    try:
        task_instance.xcom_push(
            key=resource_payload["uuid"],
            value={
                "email": user_payload["email"],
                "group": message["group"],
                "resource_uri": resource_uri,
                "resource": resource_payload["data"],
            },
        )
        resources.append(resource_uri)
    except KeyError:
        resources_with_errors.append(resource_uri)

    task_instance.xcom_push(key="resources", value=resources)
    task_instance.xcom_push(key="bad_resources", value=resources_with_errors)
    return "completed_parse"
