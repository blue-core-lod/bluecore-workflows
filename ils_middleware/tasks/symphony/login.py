"""Symphony Login."""

import json

from ils_middleware.tasks.symphony.request import SymphonyRequest


def SymphonyLogin(**kwargs) -> str:
    login = kwargs.get("login")
    password = kwargs.get("password")

    return SymphonyRequest(
        **kwargs,
        task_id="login_symphony",
        data=json.dumps({"login": login, "password": password}),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        endpoint="user/staff/login",
        filter=lambda response: response.json().get("sessionToken"),
    )
