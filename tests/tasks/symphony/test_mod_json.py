"""Tests functions for modifying JSON to Symphony JSON."""

from ils_middleware.tasks.symphony.mod_json import to_symphony_json

from tasks import test_task_instance, mock_task_instance  # noqa: F401

task_instance = test_task_instance()


def test_to_symphony_json(mock_task_instance):  # noqa: F811
    to_symphony_json(task_instance=task_instance)
    symphony_json = task_instance.xcom_pull(
        key="0000-1111-2222-3333"
    )
    assert symphony_json["standard"].startswith("MARC21")
    assert symphony_json["leader"].startswith("01498nam a2200265 u 4500")
