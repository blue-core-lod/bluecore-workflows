import json
from unittest.mock import MagicMock

import httpx
import pytest
from airflow.sdk import Connection
from pytest_mock import MockerFixture
from tasks import (
    mock_requests_okapi,  # noqa: F401
    mock_task_instance,  # noqa: F401
    test_task_instance,
)

import ils_middleware.tasks.folio.new as new_folio
from ils_middleware.tasks.folio.new import (
    _check_for_existance,
    _notify_local_id_not_found,
    _put_to_okapi,
    post_folio_records,
)

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
        # Airflow stores connection `extra` as a JSON string, not a dict.
        extra=json.dumps({"tenant": "sul "}),
    )


def mock_httpx_client(*args, **kwargs):
    mock_client = MagicMock()

    def mock__enter__(*args):
        return args[0]

    def mock__exit__(*args):
        pass

    def mock_get(*args, **kwargs):
        if args[0].endswith("/inventory/instances"):
            query_response = MagicMock()
            query = kwargs.get("params", {}).get("query", "")
            query_response.status_code = 200
            if 'hrid=="in000555"' in query:
                query_response.json = lambda: {
                    "instances": [
                        {
                            "id": "aaaa1111-2222-3333-4444-555566667777",
                            "hrid": "in000555",
                            "_version": "3",
                        }
                    ]
                }
            else:
                query_response.json = lambda: {"instances": []}
            return query_response

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


class MockFolioClient:
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
    mock_folio_client,
    mock_task_instance,  # noqa: F811
    mock_requests_okapi,  # noqa: F811
):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get",
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
    mocker: MockerFixture,
):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get",
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
        "ils_middleware.tasks.folio.new.Connection.get",
        return_value=mock_airflow_connection,
    )
    records = [{"id": "f85ea17b-4861-426d-a681-e24f0b44b57f"}]
    _, existing_records, unmatched_local_ids = _check_for_existance(
        records=records, folio_client=MockFolioClient()
    )

    assert existing_records[0]["hrid"] == "in00031000"
    assert not unmatched_local_ids


def test_check_for_existance_local_id_overlay(mocker):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get",
        return_value=mock_airflow_connection,
    )
    records = [
        {
            "id": "computed-uuid-not-used",
            "sourceUri": "https://bcld.info/instance/aaaa-bbbb",
        }
    ]
    new_records, existing_records, unmatched_local_ids = _check_for_existance(
        records=records,
        folio_client=MockFolioClient(),
        local_ids={"aaaa-bbbb": "in000555"},
    )

    assert not new_records
    assert not unmatched_local_ids
    assert existing_records[0]["id"] == "aaaa1111-2222-3333-4444-555566667777"
    assert existing_records[0]["hrid"] == "in000555"
    assert existing_records[0]["_version"] == "3"


def test_check_for_existance_local_id_not_found(mocker):
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get",
        return_value=mock_airflow_connection,
    )
    records = [
        {
            "id": "computed-uuid-not-used",
            "sourceUri": "https://bcld.info/instance/cccc-dddd",
        }
    ]
    new_records, existing_records, unmatched_local_ids = _check_for_existance(
        records=records,
        folio_client=MockFolioClient(),
        local_ids={"cccc-dddd": "unknown-local-id"},
    )

    assert not existing_records
    assert not new_records
    assert unmatched_local_ids[0]["record"]["id"] == "computed-uuid-not-used"
    assert unmatched_local_ids[0]["local_id"] == "unknown-local-id"


def test_notify_local_id_not_found_sends_email(mocker):
    mock_send_email = mocker.patch(
        "ils_middleware.tasks.folio.new.send_local_id_not_found_email"
    )
    task_instance = mocker.Mock()
    task_instance.xcom_pull.return_value = {
        "email": "researcher@example.edu",
        "resource_uri": "https://bcld.info/instance/cccc-dddd",
    }

    _notify_local_id_not_found(
        [
            {
                "record": {"sourceUri": "https://bcld.info/instance/cccc-dddd"},
                "local_id": "unknown-local-id",
            }
        ],
        task_instance,
    )

    mock_send_email.assert_called_once_with(
        task_instance.xcom_pull.return_value, "unknown-local-id"
    )


def test_notify_local_id_not_found_skips_when_no_message(mocker):
    mock_send_email = mocker.patch(
        "ils_middleware.tasks.folio.new.send_local_id_not_found_email"
    )
    task_instance = mocker.Mock()
    task_instance.xcom_pull.return_value = None

    _notify_local_id_not_found(
        [
            {
                "record": {"sourceUri": "https://bcld.info/instance/cccc-dddd"},
                "local_id": "unknown-local-id",
            }
        ],
        task_instance,
    )

    mock_send_email.assert_not_called()


class MockTaskInstance:
    def xcom_push(self, *args, **kwargs):
        pass


def test_put_to_okapi_exception(mocker, mock_task_instance, caplog):  # noqa: F811
    mocker.patch(
        "ils_middleware.tasks.folio.new.Connection.get",
        return_value=mock_airflow_connection,
    )

    _put_to_okapi(
        task_instance=MockTaskInstance(),
        records=[
            {
                "id": "0e076a6f-156d-4735-98dd-4d876edeab37",
                "sourceUri": "https://bcld.info/instance/f6d4b9e2-08f4-4a42-8bb4-ca6103c33237",
            }
        ],
        folio_client=MockFolioClient(),
    )

    assert "422 Error" in caplog.text
