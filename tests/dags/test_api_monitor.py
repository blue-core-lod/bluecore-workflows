import pytest


def test_check_available_institutions(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    assert _check_available_institutions("stanford")


def test_check_available_institutions_unknown_group(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    from ils_middleware.dags.api_monitor import _check_available_institutions

    assert not _check_available_institutions("cc")


def test_get_user_info(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    mocker.patch(
        "ils_middleware.dags.api_monitor.get_user_groups", return_value=["cornell"]
    )
    from ils_middleware.dags.api_monitor import _get_user_group

    group = _get_user_group("dev_op")
    assert group.startswith("cornell")


def test_get_user_group_multiple_groups(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    mocker.patch(
        "ils_middleware.dags.api_monitor.get_user_groups",
        return_value=["cornell", "ucdavis"],
    )
    from ils_middleware.dags.api_monitor import _get_user_group

    with pytest.raises(ValueError, match="dev_op can only be in one group for export"):
        _get_user_group("dev_op")


def test_get_user_group_no_groups(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")
    mocker.patch("ils_middleware.dags.api_monitor.get_user_groups", return_value=[])
    from ils_middleware.dags.api_monitor import _get_user_group

    with pytest.raises(ValueError, match="dev_op not in any groups"):
        _get_user_group("dev_op")


def test_trigger_dags(mocker, caplog):
    mocker.patch("airflow.models.Variable.get", return_value="test")

    mock_trigger_operator = mocker.patch(
        "ils_middleware.dags.api_monitor.TriggerDagRunOperator"
    )

    from ils_middleware.dags.api_monitor import (
        _check_available_institutions,
        _trigger_dags,
    )

    group = "stanford"
    assert _check_available_institutions(group)

    _trigger_dags(payload={"group": group})

    assert mock_trigger_operator.called
    assert "Trigger DAG for stanford" in caplog.text
