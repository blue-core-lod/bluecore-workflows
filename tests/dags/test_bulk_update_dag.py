"""
Tests for the parts of the bulk_update DAG that hold logic of their own. The
work itself lives in ils_middleware.tasks.bulk_update, which is tested
directly; these cover the wiring the DAG adds.
"""

import types

import pytest
from airflow.exceptions import AirflowException

from ils_middleware.dags.bulk_update import bulk_update_dag
from ils_middleware.tasks.bulk_update import new_report

WORK_URI = "https://bcld.info/works/1234"


def callable_for(task_id):
    return bulk_update_dag.get_task(task_id).python_callable


@pytest.fixture
def context(mocker):
    """Stand in for the Airflow task context, with params a test can set."""
    context = {
        "params": {},
        "dag_run": types.SimpleNamespace(
            dag_id="bulk_update", run_id="manual__2026-09-08", conf={}
        ),
    }
    mocker.patch(
        "ils_middleware.dags.bulk_update.get_current_context", return_value=context
    )
    return context


def test_dag_structure():
    assert bulk_update_dag.dag_id == "bulk_update"
    assert sorted(task.task_id for task in bulk_update_dag.tasks) == [
        "bluecore_db_info",
        "get_keycloak_user_uid",
        "plan",
        "report",
        "update_batch",
    ]
    # a run only writes if someone asks it to
    assert bulk_update_dag.params["dry_run"] is True


def test_plan(context, tmp_path):
    csv_file = tmp_path / "uris.csv"
    csv_file.write_text(f"{WORK_URI}\nhttps://bcld.info/works/5678\n")
    context["params"] = {
        "file": str(csv_file),
        "query": "DELETE WHERE { ?resource <http://id.loc.gov/ontologies/bibframe/note> ?n }",
        "batch_size": 1,
        "dry_run": True,
    }

    assert callable_for("plan")() == [[WORK_URI], ["https://bcld.info/works/5678"]]


def test_plan_checks_the_query_before_the_file(context, tmp_path):
    """A bad query fails the run before a resource is touched."""
    context["params"] = {"file": str(tmp_path / "nope.csv"), "query": "DROP ALL"}

    with pytest.raises(Exception, match="Drop is not allowed"):
        callable_for("plan")()


def test_get_keycloak_user_uid(context):
    context["dag_run"].conf = {"user_uid": "uid-1"}

    assert callable_for("get_keycloak_user_uid")() == "uid-1"


def test_report(context, mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", str(tmp_path))
    reports = [new_report(dry_run=True) | {"processed": 1, "updated": [WORK_URI]}]

    path = callable_for("report")(reports)

    assert path == str(tmp_path / "bulk_update" / "manual__2026-09-08" / "index.html")
    assert (
        WORK_URI
        in (tmp_path / "bulk_update" / "manual__2026-09-08" / "report.json").read_text()
    )


def test_report_fails_the_run_when_a_resource_errored(context, tmp_path, monkeypatch):
    """The report is still written; the run is not called a success."""
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", str(tmp_path))
    reports = [
        new_report(dry_run=False)
        | {"processed": 1, "errors": [{"uri": WORK_URI, "error": "deadlock"}]}
    ]

    with pytest.raises(AirflowException, match="1 resources errored"):
        callable_for("report")(reports)

    assert (tmp_path / "bulk_update" / "manual__2026-09-08" / "index.html").exists()


def test_report_does_not_fail_the_run_for_skipped_resources(
    context, tmp_path, monkeypatch
):
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", str(tmp_path))
    reports = [
        new_report(dry_run=False)
        | {"processed": 1, "skipped": [{"uri": WORK_URI, "reason": "not found"}]}
    ]

    assert callable_for("report")(reports)
