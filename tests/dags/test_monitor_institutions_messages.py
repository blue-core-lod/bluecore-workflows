import pytest


messages = [
    {"group": "stanford"},
    {"group": "colorado"},
]


def test_parse_institutional_msgs(mocker):
    from ils_middleware.dags.api_monitor import _parse_institutional_msgs

    institutional_messages = _parse_institutional_msgs(messages)
    assert len(institutional_messages) == 1
    assert institutional_messages[0]["group"] == "stanford"


def test_trigger_dags(mocker, caplog):
    mock_trigger_operator = mocker.patch(
        "ils_middleware.dags.api_monitor.TriggerDagRunOperator"
    )
    from ils_middleware.dags.api_monitor import (
        _parse_institutional_msgs,
        _trigger_dags,
    )

    institutional_messages = _parse_institutional_msgs([{"group": "Stanford"}])
    _trigger_dags(messages=institutional_messages)

    assert mock_trigger_operator.called
    assert "Trigger DAG for Stanford" in caplog.text
