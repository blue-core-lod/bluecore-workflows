import pathlib

import pytest
from airflow.sdk import DAG, Variable

DAG_PATHS = [
    p
    for p in (pathlib.Path(__file__).parents[2] / "ils_middleware/dags").glob(
        "[!_]*.py"
    )
]


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "sqs_url":
            return "http://aws.com/12345/"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path, mock_variable):
    module = _import_file(dag_path.name, dag_path)

    dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]
    assert dag_objects

    # For every DAG object, test for cycles
    for dag in dag_objects:
        dag.check_cycle()


@pytest.mark.parametrize(
    "dag_file, prefix",
    [("stanford.py", "process_folio."), ("cornell.py", "")],
)
def test_record_instance_exports_follows_folio(dag_file, prefix, mock_variable):
    dag_path = pathlib.Path(__file__).parents[2] / "ils_middleware/dags" / dag_file
    module = _import_file(dag_file, dag_path)

    record_exports = module.dag.get_task(f"{prefix}record-instance-exports")
    assert record_exports.upstream_task_ids == {f"{prefix}new-or-upsert-folio-records"}


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
