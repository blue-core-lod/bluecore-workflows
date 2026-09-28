def test_resource_loader_conf_sets_bfdb_agent_user_uid(mocker):
    mocker.patch("airflow.models.Variable.get", return_value="test")

    from ils_middleware.dags.bfdb import _resource_loader_conf

    assert _resource_loader_conf("/opt/airflow/uploads/run/works/123.json") == {
        "file": "/opt/airflow/uploads/run/works/123.json",
        "user_uid": "BFDB Agent",
    }
