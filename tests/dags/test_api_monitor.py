def test_build_payload_without_local_id(mocker):
    mocker.patch("airflow.sdk.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _build_payload

    payload = _build_payload(
        {"resource": "https://bcld.info/instance/1111-2222", "user": "uid-1"}
    )

    assert payload == {
        "resource": "https://bcld.info/instance/1111-2222",
        "local_id": None,
        "user": "uid-1",
    }


def test_build_payload_with_local_id(mocker):
    mocker.patch("airflow.sdk.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _build_payload

    payload = _build_payload(
        {
            "resource": "https://bcld.info/instance/1111-2222",
            "user": "uid-1",
            "local_id": "a123456",
        }
    )

    assert payload == {
        "resource": "https://bcld.info/instance/1111-2222",
        "user": "uid-1",
        "local_id": "a123456",
    }


def test_check_available_institutions(mocker):
    mocker.patch("airflow.sdk.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    assert _check_available_institutions("stanford")


def test_check_available_institutions_unknown_group(mocker):
    mocker.patch("airflow.sdk.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    assert not _check_available_institutions("cc")


def test_trigger_dags(mocker, caplog):
    mocker.patch("airflow.sdk.Variable.get", return_value="test")

    mock_trigger_operator = mocker.patch(
        "ils_middleware.dags.api_monitor.TriggerDagRunOperator"
    )

    from ils_middleware.dags.api_monitor import (
        _check_available_institutions,
        _trigger_dags,
    )

    group = "stanford"
    assert _check_available_institutions(group)

    _trigger_dags(
        payload={
            "user": {
                "groups": [
                    group,
                ]
            }
        }
    )

    assert mock_trigger_operator.called
    assert "Trigger DAG for stanford" in caplog.text
