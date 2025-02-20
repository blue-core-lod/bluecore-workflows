import httpx

from airflow.sdk import get_current_context


def get_resource(resource_uri: str) -> dict:
    """Retrieves the Resource from Bluecore API"""
    result = httpx.get(resource_uri)
    if result.status_code < 400:
        return result.json()
    return {}


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
