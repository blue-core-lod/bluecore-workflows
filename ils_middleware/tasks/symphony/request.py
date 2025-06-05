"""Request function for Symphony Web Services"""

import logging
import requests  # type: ignore

from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def symphony_requests(http_verb, uri, data, headers):
    if http_verb.startswith("post"):
        result = requests.post(uri, data=data, headers=headers)
    elif http_verb.startswith("put"):
        result = requests.put(uri, data=data, headers=headers)
    else:
        msg = f"{http_verb} not available for {uri}"
        logger.error(msg)
        raise ValueError(msg)
    if result.status_code > 399:
        msg = f"Symphony Web Service Call to {uri} Failed with {result.status_code}\n{result.text}"
        logger.error(msg)
        raise Exception(msg)
    return result


def SymphonyRequest(**kwargs) -> str:
    app_id = kwargs.get("app_id")
    client_id = kwargs.get("client_id")
    conn_id = kwargs.get("conn_id", "")
    data = kwargs.get("data")
    endpoint = kwargs.get("endpoint", "")
    http_verb = kwargs.get("http_verb", "post")
    override_headers = kwargs.get("headers", {})
    response_filter = kwargs.get("filter")
    session_token = kwargs.get("token")

    headers = {
        "Content-Type": "application/vnd.sirsidynix.roa.resource.v2+json",
        "Accept": "application/vnd.sirsidynix.roa.resource.v2+json",
        "sd-originating-app-id": app_id,
        "x-sirs-clientID": client_id,
    }

    if session_token:
        headers["x-sirs-sessionToken"] = session_token

    # Incoming headers either override existing or add new header
    for key, value in override_headers.items():
        headers[key] = value

    logger.info(f"Headers {headers}")
    # Generate Symphony URL based on the Airflow Connection and endpoint
    symphony_conn = BaseHook.get_connection(conn_id)

    symphony_uri = symphony_conn.host + endpoint

    symphony_result = symphony_requests(http_verb, symphony_uri, data, headers)

    logger.debug(
        f"Symphony Results symphony_result {symphony_result.status_code}\n{symphony_result.text}"
    )
    if response_filter:
        return response_filter(symphony_result)
    return symphony_result.text
