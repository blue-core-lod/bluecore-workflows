import pytest  # noqa

from ils_middleware.tasks.amazon.bluecore_records_s3 import get_file, httpx


class MockHTTPXResponse(object):

    def __init__(self, url: str):
        self.content = b"""{ "uri": "https://bluecore-core.lod.io/works/123456" }"""

    def raise_for_status(self):
        return


class MockS3Hook(object):

    def download_file(self, **kwargs):
        return "/tmp/bluecore_12275150.jsonld"


def test_get_file_s3(mocker):
    mocker.patch(
        "ils_middleware.tasks.amazon.bluecore_records_s3.S3Hook",
        return_value=MockS3Hook()
    )

    file_path = get_file(
        file="s3://bluecore-development/bluecore_12275150.jsonld",
        bucket="bluecore-development"
    )

    assert file_path.startswith("/tmp/bluecore_12275150")


def test_get_file_http(mocker, tmp_path):
    mocker.patch.object(
        httpx,
        "get",
        MockHTTPXResponse
    )

    airflow_path = tmp_path / "airflow"
    uploads_path = airflow_path / "uploads"
    uploads_path.mkdir(parents=True, exist_ok=True)

    file_path = get_file(file="https://api.sinopia.io/resources/111abc333",
                         airflow=str(airflow_path))
    
    assert file_path.startswith(f"{airflow_path}/uploads/111abc333")
    assert (uploads_path / "111abc333").exists()


def test_get_file_local():

    file_path = get_file(file="/opt/airflow/uploads/bluecore_88991.jsonld")
    assert file_path.endswith("uploads/bluecore_88991.jsonld")