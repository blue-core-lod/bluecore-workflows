import httpx
import pytest

from pytest_mock import MockerFixture

from unittest.mock import MagicMock

from airflow.models import Connection

from tasks import test_task_instance  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401


from ils_middleware.tasks.folio.new import (
    post_folio_records,  # noqa
    _check_for_existance,  # noqa
    _put_to_okapi,  # noqa
)
import ils_middleware.tasks.folio.new as new_folio

okapi_uri = "https://okapi-folio.dev.edu"
instance_uri = "https://api.development.sinopia.io/resource/0000-1111-2222-3333"


@pytest.fixture
def mock_airflow_connection():
    return Connection(
        conn_id="stanford_folio",
        conn_type="http",
        host=okapi_uri,
        login="folio_user",
        password="pass",
        extra={"tenant": "sul "},
    )


def mock_httpx_client(*args, **kwargs):
    mock_client = MagicMock()

    def mock__enter__(*args):
        return args[0]

    def mock__exit__(*args):
        pass

    def mock_get(*args, **kwargs):
        post_response = MagicMock()
        post_response.status_code = 404
        if args[0].endswith("f85ea17b-4861-426d-a681-e24f0b44b57f"):
            post_response.status_code = 200
            post_response.json = lambda: {"hrid": "in00031000", "_version": "2"}

        return post_response

    mock_client.__enter__ = mock__enter__
    mock_client.__exit__ = mock__exit__
    mock_client.get = mock_get
    return mock_client


class MockFolioClient(object):
    def __init__(self, *args, **kwargs):
        self.okapi_url = okapi_uri
        self.okapi_headers = {}

    def folio_post(self, *args, **kwargs):
        if args[0].endswith("instances?upsert=true"):
            raise httpx.HTTPStatusError(
                "Internal server error",
                request=httpx.Request("POST", args[0]),
                response=httpx.Response(500),
            )
        return {"hrid": "in000780"}

    def folio_put(self, *args, **kwargs):
        if args[0].endswith("0e076a6f-156d-4735-98dd-4d876edeab37"):
            raise httpx.HTTPStatusError(
                "422 Error",
                request=httpx.Request("PUT", args[0]),
                response=httpx.Response(422),
            )

    def get_folio_http_client(self):
        return mock_httpx_client()


@pytest.fixture
def mock_folio_client(monkeypatch):
    monkeypatch.setattr(new_folio, "FolioClient", MockFolioClient)


def test_happypath_post_folio_record(
    mocker,
    mock_airflow_connection,
    mock_folio_client,  # noqa: F811
    mock_task_instance,  # noqa: F811
    mock_requests_okapi,  # noqa: F811
):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get_connection_from_secrets",
        return_value=mock_airflow_connection,
    )

    post_folio_records(
        task_instance=test_task_instance(),
        tenant="sul",
        endpoint="/instance-storage/instances",
        folio_connection_id="stanford_folio",
        task_groups_ids=[""],
    )

    instance_uuid = instance_uri.split("/")[-1]
    assert (
        test_task_instance().xcom_pull(key=instance_uuid)
    ) == "147b1171-740e-513e-84d5-b63a9642792c"


def test_raised_error(
    mock_airflow_connection,
    mock_folio_client,
    mock_task_instance,  # noqa: F811
    caplog,
    mocker: MockerFixture,  # noqa: F811
):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get_connection_from_secrets",
        return_value=mock_airflow_connection,
    )

    post_folio_records(
        task_instance=test_task_instance(),
        folio_connection_id="stanford_folio",
        endpoint="/instance-storage/batch/instances?upsert=true",
        task_groups_ids=["folio"],
    )

    assert "Internal server error" in caplog.text


def test_check_for_existance_existing_record(mocker, mock_task_instance):  # noqa: F811
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get_connection_from_secrets",
        return_value=mock_airflow_connection,
    )
    records = [{"id": "f85ea17b-4861-426d-a681-e24f0b44b57f"}]
    _, existing_records = _check_for_existance(
        records=records, folio_client=MockFolioClient()
    )

    assert existing_records[0]["hrid"] == "in00031000"


class MockTaskInstance(object):
    def xcom_push(self, *args, **kwargs):
        pass


def test_put_to_okapi_exception(mocker, mock_task_instance, caplog):  # noqa: F811
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get_connection_from_secrets",
        return_value=mock_airflow_connection,
    )

    _put_to_okapi(
        task_instance=MockTaskInstance(),
        records=[
            {
                "id": "0e076a6f-156d-4735-98dd-4d876edeab37",
                "electronicAccess": [
                    {
                        "uri": "https://api.stage.sinopia.io/resource/f6d4b9e2-08f4-4a42-8bb4-ca6103c33237"
                    }
                ],
            }
        ],
        folio_client=MockFolioClient(),
    )

    assert "422 Error" in caplog.text
