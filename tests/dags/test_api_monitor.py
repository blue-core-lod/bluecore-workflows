import pytest


def test_check_available_institutions(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    institutional_messages = _check_available_institutions({"group": "stanford"})
    assert len(institutional_messages) == 1
    assert institutional_messages["group"] == "stanford"


def test_check_available_institutions_unknown_group(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    with pytest.raises(ValueError, match="cc isn't available for export"):
        _check_available_institutions({"group": "cc"})


def test_trigger_dags(mocker, caplog):
    mocker.patch("airflow.models.Variable.get", return_value="test")

    mock_trigger_operator = mocker.patch(
        "ils_middleware.dags.api_monitor.TriggerDagRunOperator"
    )

    from ils_middleware.dags.api_monitor import (
        _check_available_institutions,
        _trigger_dags,
    )

    institutional_messages = _check_available_institutions({"group": "Stanford"})
    _trigger_dags(payload=institutional_messages)

    assert mock_trigger_operator.called
    assert "Trigger DAG for Stanford" in caplog.text
