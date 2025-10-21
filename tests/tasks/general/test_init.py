import pytest

from ils_middleware.tasks.general import message_from_context, parse_messages
from tests.keycloak.server_mocks import mock_keycloak  # type: ignore # noqa: F401


def test_message_from_context():
    mock_context = {
        "params": {
            "message": {"resource": {"http://sinopia.io/resources/1111-2222-3333-0000"}}
        }
    }

    message = message_from_context(context=mock_context)

    assert message == {"resource": {"http://sinopia.io/resources/1111-2222-3333-0000"}}


def test_message_from_context_no_message():
    mock_context = {"params": {"message": None}}
    with pytest.raises(ValueError, match="Cannot initialize DAG, no message present"):
        message_from_context(context=mock_context)


def test_parse_messages(mocker, mock_keycloak):  # noqa: F811
    mock_task_instance = mocker
    xcoms = []
    mock_task_instance.xcom_push = lambda key, value: xcoms.append(
        {"key": key, "value": value}
    )

    message = {
        "user": {
            "username": "dev_op",
            "email": "dev_op@bluecore.org",
            "id": "48bac4b4-a4f1-4008-9f16-7dc0b51fd85e",
        },
        "resource": "https://bcld.info/instance/7922d096-9b45-4235-be9a-a89d390bee83",
        "group": "stanford",
    }

    result = parse_messages(task_instance=mock_task_instance, message=message)

    assert result.startswith("completed_parse")
    assert xcoms[0]["key"].endswith("7922d096-9b45-4235-be9a-a89d390bee83")
    assert xcoms[1]["value"][0].startswith("https://bcld.info/instance/7922d096")
    assert xcoms[2]["value"] == []
