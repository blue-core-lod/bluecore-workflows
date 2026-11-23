import pytest

from ils_middleware.dags.bfdb import process_activity_streams_dag as dag


def test_cursor_commit_follows_load_and_precedes_cleanup():
    commit = dag.get_task("commit_cursors")
    cleanup = dag.get_task("delete_file_path")
    assert "bfdb_file_loader" in commit.upstream_task_ids
    assert "commit_cursors" in cleanup.upstream_task_ids
    assert commit.trigger_rule == "none_failed"
    assert cleanup.trigger_rule == "none_failed"
    assert dag.max_active_runs == 1


@pytest.mark.parametrize("errors", [[], [[]], [[], []]])
def test_cleanup_removes_successful_downloads(tmp_path, errors):
    run_path = tmp_path / "run"
    run_path.mkdir()
    path = run_path / "1234.hub"
    path.write_text("{}")

    dag.get_task("delete_file_path").python_callable([str(path)], errors)

    assert not run_path.exists()


def test_cleanup_keeps_downloads_when_any_batch_has_errors(tmp_path):
    path = tmp_path / "1234.hub"
    path.write_text("{}")

    dag.get_task("delete_file_path").python_callable([str(path)], [[], [str(path)], []])

    assert path.exists()


def test_loader_passes_files(mocker):
    load = mocker.patch("ils_middleware.dags.bfdb.load_files", return_value=[])
    assert (
        dag.get_task("bfdb_file_loader").python_callable(
            bluecore_db="db", user_uid="BFDB Agent", files=["1234.hub"]
        )
        == []
    )
    load.assert_called_once_with(
        bluecore_db="db",
        user_uid="BFDB Agent",
        files=["1234.hub"],
    )
