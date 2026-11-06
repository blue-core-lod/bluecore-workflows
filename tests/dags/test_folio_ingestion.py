import pytest
from airflow.utils.trigger_rule import TriggerRule

from ils_middleware.dags import folio_ingestion

DAG = folio_ingestion.folio_instance_to_bluecore_dag


@pytest.mark.parametrize(
    "params,chosen",
    [
        ({"use_marc": True}, "retrieve_marc_record"),
        ({"use_marc": False}, "generate_bibframe"),
        ({}, "generate_bibframe"),
    ],
)
def test_choose_mapping(mocker, params, chosen):
    mocker.patch.object(
        folio_ingestion, "get_current_context", return_value={"params": params}
    )

    assert DAG.get_task("choose_mapping").python_callable() == chosen


def test_branches_lead_to_their_tasks():
    branch = DAG.get_task("choose_mapping")

    assert branch.downstream_task_ids == {"generate_bibframe", "retrieve_marc_record"}
    assert DAG.get_task("retrieve_marc_record").downstream_task_ids == {
        "marc_to_bibframe"
    }


def test_branches_merge_at_ingest_then_update_source_uri():
    ingest = DAG.get_task("ingest_into_bluecore")

    assert {"generate_bibframe", "marc_to_bibframe"} <= ingest.upstream_task_ids
    assert ingest.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    assert "update_source_uri" in ingest.downstream_task_ids


def test_use_marc_defaults_to_false():
    assert DAG.params["use_marc"] is False
