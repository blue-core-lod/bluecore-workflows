import pytest

from ils_middleware.tasks.keycloak import httpx  # type: ignore


@pytest.fixture
def mock_keycloak(monkeypatch, mocker):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub("get_result")
        get_response.raise_for_status = lambda: None
        payload = None
        if args[0].endswith("bluecore/groups"):
            payload = [
                {"id": "5e52bd26-8440-4506-b625-e1712fe52777", "name": "Stanford"},
                {"id": "eba4f859-fb61-47d5-801f-3756b8126114", "name": "LOC"},
            ]
        if args[0].endswith("e1712fe52777/members"):
            payload = [{"username": "dev_op", "email": "dev_op@bluecore.edu"}]
        if args[0].endswith("3756b8126114/members"):
            payload = [{"username": "dev_user", "email": "dev_user@bluecore.edu"}]
        if args[0].endswith("users?username=dev_op"):
            payload = [
                {
                    "username": "dev_op",
                    "email": "dev_op@bluecore.org",
                    "id": "48bac4b4-a4f1-4008-9f16-7dc0b51fd85e",
                }
            ]
        if args[0].endswith("users?username=poly_user"):
            payload = [
                {"username": "poly_user", "id": "8a1b50b7-6638-4cfc-9869-0d84b407598b"},
                {"username": "poly_user", "id": "60db4f45-8e66-4a61-9269-493ac850f90a"},
            ]
        if args[0].endswith("users?username=aearhart"):
            payload = []
        if args[0].endswith("7dc0b51fd85e/groups"):
            payload = [{"name": "UC Davis"}]
        # Used in general/test_init.py::test_parse_messages
        if args[0].endswith("7922d096-9b45-4235-be9a-a89d390bee83"):
            payload = {
                "uuid": "7922d096-9b45-4235-be9a-a89d390bee83",
                "uri": "https://bcld.info/instance/7922d096-9b45-4235-be9a-a89d390bee83",
                "data": {
                    "@id": "https://bcld.info/instance/7922d096-9b45-4235-be9a-a89d390bee83",
                    "@type": ["http://id.loc.gov/ontologies/bibframe/Work"],
                },
            }
        get_response.json = lambda: payload
        return get_response

    def mock_post(*args, **kwargs):
        post_response = mocker.stub("post-response")
        post_response.raise_for_status = lambda: None
        post_response.json = lambda: {"access_token": "yJhbGciOi"}
        return post_response

    monkeypatch.setattr(httpx, "get", mock_get)
    monkeypatch.setattr(httpx, "post", mock_post)
