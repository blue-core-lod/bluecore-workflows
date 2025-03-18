import pathlib
import pytest

from airflow import models
from airflow.utils.dag_cycle_tester import check_cycle

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

    monkeypatch.setattr(models.Variable, "get", mock_get)


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path, mock_variable):
    module = _import_file(dag_path.name, dag_path)

    dag_objects = [var for var in vars(module).values() if isinstance(var, models.DAG)]
    assert dag_objects

    # For every DAG object, test for cycles
    for dag in dag_objects:
        check_cycle(dag)


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
