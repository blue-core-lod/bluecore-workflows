import os
import tarfile
import zipfile

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from ils_middleware.tasks.bluecore import (
    batch_archived_files,
    delete_upload,
    is_zip,
    get_bluecore_db,
    load_cbd_files,
    zip_to_tar_gz,
)


def test_batch_archived_files_no_file():
    with pytest.raises(FileNotFoundError, match="cbd.tar.gz does not exist"):
        batch_archived_files("cbd.tar.gz")


def test_batch_archived_files(tmp_path):
    test_cbd_files_dir = tmp_path / "data"
    test_cbd_files_dir.mkdir()
    for i in range(0, 10_000):
        test_cbd_file = test_cbd_files_dir / f"20{i}.cbd.jsonld"
        test_cbd_file.write_text("{}")
    archived_file_path = tmp_path / "cbd.tar.gz"
    with tarfile.open(archived_file_path, "x:gz") as tar_file:
        tar_file.add(test_cbd_files_dir)

    test_batches = batch_archived_files(str(archived_file_path))
    assert len(test_batches) == 5
    assert len(test_batches[0]) == 2_000


def test_delete_upload(tmp_path):
    upload_path = tmp_path / "sub_dir"
    os.mkdir(upload_path)
    upload_file = upload_path / "bf-record.jsonld"
    upload_file.touch()
    upload_file2 = upload_path / "bf-record2.jsonld"
    upload_file2.touch()

    assert upload_path.exists()
    assert upload_file.exists()
    assert upload_file2.exists()

    # It should delete the parent directory only if it's empty and remove_empty_parent is True
    delete_upload(str(upload_file))
    assert upload_path.exists()
    assert not upload_file.exists()

    # It should keep the parent directory as remove_empty_parent is not set
    delete_upload(str(upload_file2))
    assert upload_path.exists()
    assert not upload_file2.exists()

    # Delete the second file again with the remove_empty_parent flag set to True
    upload_file2.touch()
    delete_upload(str(upload_file2), remove_empty_parent=True)
    assert not upload_path.exists()


def test_is_zip():
    assert is_zip("test.zip")
    assert is_zip("test.tar.gz")
    assert not is_zip("test.txt")


class MockPostgresHook(object):
    def __init__(self, *args):
        self.sqlalchemy_url = make_url(
            "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore"
        )


@pytest.fixture
def mock_postgres_hook(mocker):
    mocker.patch(
        "ils_middleware.tasks.bluecore.PostgresHook",
        return_value=MockPostgresHook(),
    )
    return mocker


def test_get_bluecore_db(mock_postgres_hook):
    db_string = get_bluecore_db()

    assert db_string.startswith("postgresql://bluecore_admin")


def _build_cbd_archive(tmp_path):
    cbd_file = tmp_path / "022.cbd.jsonld"
    cbd_file.write_text("{}")  # empty but valid JSON-LD
    archive_path = tmp_path / "cbd.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(cbd_file, arcname="022.cbd.jsonld")
    return archive_path


def test_load_cbd_files_with_deadlocks(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    sleep_mock = mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    deadlock = OperationalError("deadlock detected", None, Exception("deadlock"))
    save_graph_mock = mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        # raise twice, then succeed on the 3rd attempt
        side_effect=[deadlock, deadlock, None],
    )

    errors = load_cbd_files(
        ["022.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    assert errors == []
    assert save_graph_mock.call_count == 3  # retried until success
    assert sleep_mock.call_count == 2  # backoff between the 3 attempts


def test_load_cbd_files_persistent_deadlock(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    deadlock = OperationalError("deadlock detected", None, Exception("deadlock"))
    mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        side_effect=deadlock,  # always deadlocks
    )

    # after 3 failed attempts the loop re-raises
    with pytest.raises(OperationalError):
        load_cbd_files(
            ["022.cbd.jsonld"],
            "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
            "user-uid",
            str(archive_path),
        )


def test_load_cbd_files_non_deadlock_error(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    sleep_mock = mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    save_graph_mock = mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        side_effect=ValueError("bad graph"),
    )

    errors = load_cbd_files(
        ["022.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    # a non-deadlock error is recorded once and does not retry
    assert errors == ["022.cbd.jsonld"]
    assert save_graph_mock.call_count == 1
    assert sleep_mock.call_count == 0


def test_zip_to_tar_gz(tmp_path):
    cbd_file_path = tmp_path / "022.cbd.jsonld"
    cbd_file_path.write_text("{}")

    zip_file_path = tmp_path / "cbd.zip"
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        zip_file.write(cbd_file_path)

    tar_file_name = zip_to_tar_gz(str(zip_file_path))
    assert not zip_file_path.exists()
    assert tar_file_name.endswith("cbd.tar.gz")
