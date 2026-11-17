"""
Tests for the archived_file_loader DAG's cleanup step. delete_file_path gets
the collected output of the mapped cbd_file_loader task: one list of failed
names per batch, so a clean run of two batches arrives as [[], []].
"""

import pytest

from ils_middleware.dags.archive_loader import archived_file_loader_dag


def delete_file_path(**kwargs):
    callable_ = archived_file_loader_dag.get_task("delete_file_path").python_callable
    return callable_(**kwargs)


@pytest.fixture
def upload(tmp_path):
    batch_dir = tmp_path / "uploads" / "batch-1"
    batch_dir.mkdir(parents=True)
    upload = batch_dir / "hubs.jsonld.gz"
    upload.write_bytes(b"data")
    return upload


@pytest.mark.parametrize("errors", [[[]], [[], [], []]])
def test_deletes_upload_when_no_batch_has_errors(upload, errors):
    delete_file_path(archive_file_path=str(upload), errors=errors)

    assert not upload.exists()
    # the per-upload directory is emptied, so it goes too
    assert not upload.parent.exists()


def test_keeps_upload_when_a_batch_has_errors(upload, caplog):
    delete_file_path(archive_file_path=str(upload), errors=[[], ["bad.rdf"]])

    assert upload.exists()
    assert "bad.rdf" in caplog.text


def test_keeps_uploads_directory(tmp_path):
    """A file at the top of uploads/ is deleted, but uploads/ itself stays."""
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    upload = uploads / "hubs.jsonld.gz"
    upload.write_bytes(b"data")

    delete_file_path(archive_file_path=str(upload), errors=[[]])

    assert not upload.exists()
    assert uploads.exists()
