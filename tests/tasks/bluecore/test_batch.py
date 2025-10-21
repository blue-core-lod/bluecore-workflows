import os

from ils_middleware.tasks.bluecore.batch import (
    delete_upload,
    is_zip,
)


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
