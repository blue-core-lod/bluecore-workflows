import pytest

from ils_middleware.tasks.general import message_from_context


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
