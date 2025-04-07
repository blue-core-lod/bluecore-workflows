"""Test FOLIO Mapping from Sinopia."""

import pytest
import rdflib

from airflow.models.taskinstance import TaskInstance
from ils_middleware.tasks.folio.map import map_to_folio

from tasks import test_task_instance

instance_uri = (
    "https://api.stage.sinopia.io/resource/b0319047-acd0-4f30-bd8b-98e6c1bac6b0"
)

work_uri = "https://api.stage.sinopia.io/resource/c96d8b55-e0ac-48a5-9a9b-b0684758c99e"

mock_push_store: dict = {}


@pytest.fixture
def mock_task_instance(monkeypatch, test_graph: rdflib.Graph):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids", "")
        if key == "b0319047-acd0-4f30-bd8b-98e6c1bac6b0":
            if task_ids.startswith("bf-graph"):
                return {
                    "graph": test_graph.serialize(format="json-ld"),
                    "work_uri": work_uri,
                }
            # Would be the title_task
            return mock_push_store[key]
        if key.startswith("resources"):
            return [
                instance_uri,
            ]

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = [str(value[0][0]), str(value[0][1])]
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


def test_folio(mock_task_instance, test_graph: rdflib.Graph):  # noqa: F811
    """Test map_to_folio."""
    map_to_folio(
        task_instance=test_task_instance(),
        folio_field="title",
        task_groups_ids=[""],
    )

    instance_uuid = instance_uri.split("/")[-1]
    title_list = test_task_instance().xcom_pull(key=instance_uuid)

    # Main Title
    assert title_list[0].startswith("Scrivere di Islam")

    # Subtitle
    assert title_list[1].startswith("raccontere la diaspora")


def test_folio_work(mock_task_instance, test_graph: rdflib.Graph):  # noqa: F811
    map_to_folio(
        task_instance=test_task_instance(),
        folio_field="contributor.primary.Person",
        task_groups_ids=[""],
    )

    instance_uuid = instance_uri.split("/")[-1]
    contributors_list = test_task_instance().xcom_pull(key=instance_uuid)

    # First Contributor Name (tests for both options as SPARQL query is not
    # explictly defining an order
    assert contributors_list[0].startswith("Blow, C. Joe") or contributors_list[
        0
    ].startswith("Brioni, Simone")

    # First Contributor Role
    assert contributors_list[1].startswith("Author")
