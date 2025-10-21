import logging
import os
import pathlib

BLUECORE_URL = os.environ.get("BLUECORE_URL", "https://bcld.info/")

logger = logging.getLogger(__name__)


def delete_upload(upload: str, remove_empty_parent: bool = False) -> None:
    """
    Deletes upload file
    """
    upload_path = pathlib.Path(upload)
    upload_path.unlink()
    if remove_empty_parent:
        parent_dir = upload_path.parent
        if parent_dir.exists() and not any(parent_dir.iterdir()):
            parent_dir.rmdir()


def is_zip(file_name: str) -> bool:
    """Determines if file is a zip file"""
    if file_name.endswith(".zip") or file_name.endswith(".gz"):
        return True
    return False
