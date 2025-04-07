"""Tests alma Post BF Work"""

import pytest
from tasks import (
    mock_task_instance,
)

from unittest import mock
from unittest.mock import patch, Mock
from pytest_mock import MockerFixture
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from ils_middleware.tasks.alma.post_bfwork import (
    get_env_vars,
    NewWorktoAlma,
    parse_400,
    putWorkToAlma,
)

# Define the mock variable
mock_variable_getter = Mock()
mock_variable_getter.side_effect = lambda x: (
    "your_value"
    if x in ["alma_uri_region_penn", "uri_region", "alma_api_key"]
    else None
)

os.environ["alma_uri_region_penn"] = "https://api-na.hosted.exlibrisgroup.com"
os.environ["uri_region"] = "https://api-na.hosted.exlibrisgroup.com"
os.environ["institution"] = "penn"

task_instance = mock_task_instance
alma_uri = (
    "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs?from_nz_mms_id=&from_cz_mms_id=&\
            normalization=&validate=false&override_warning=true&check_match=false&import_profile=&apikey=\
            12ab34c56789101112131415161718192021"
)
MockDag = Mock()
MockDag.dag_id = "penn"
actual_dag = MockDag


def test_get_env_vars():
    # Mock the Variable.get method
    mock_variable_getter = Mock()
    mock_variable_getter.side_effect = ["uri_region", "alma_api_key"]

    with patch("airflow.models.Variable.get", new=mock_variable_getter):
        # Call the function with mock arguments
        uri_region, alma_api_key = get_env_vars(institution="penn")

        # Assert that the Variable.get method was called with the correct arguments
        mock_variable_getter.assert_any_call("alma_uri_region_penn")
        mock_variable_getter.assert_any_call("alma_api_key_penn")

        # Assert that the function returned the correct values
        assert uri_region == "uri_region"
        assert alma_api_key == "alma_api_key"


@pytest.fixture
def data():
    return "dummy_value"


@pytest.fixture
def instance_uri():
    return "dummy_value"


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://alma.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "sinopia-marc-test")
    monkeypatch.setenv(
        "AIRFLOW_VAR_ALMA_API_KEY_PENN", "12ab34c56789101112131415161718192021"
    )
    monkeypatch.setenv(
        "AIRFLOW_VAR_ALMA_URI_REGION_PENN", "https://api-na.hosted.exlibrisgroup.com"
    )


@pytest.fixture
def mock_hook(mocker: mock.Mock) -> mock.Mock:
    return mocker.patch("airflow.hooks.base_hook.BaseHook")


@pytest.fixture
def mock_s3_load_bytes():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_bytes"
    ) as mocked:
        yield mocked


@pytest.fixture
def mock_s3_hook(mocker):
    mock = mocker.Mock()
    mocker.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook", return_value=mock)
    return mock


def get_s3_hook():
    return S3Hook(aws_conn_id="aws_lambda_connection")


# Mock the boto3 client and its head_object method
mock_boto3_client = Mock()
mock_boto3_client.head_object.return_value = {}


def test_NewWorktoAlma_post_request(mocker: MockerFixture):
    # Mock the requests.post method
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = "<mms_id>12345</mms_id>"
    mocker.patch("requests.post", return_value=mock_response)

    # Mock the get_env_vars function
    mock_get_env_vars = Mock()
    mock_get_env_vars.side_effect = lambda x: (
        ("uri_region", "alma_api_key") if x == "penn" else (None, None)
    )
    mocker.patch(
        "ils_middleware.tasks.alma.post_bfwork.get_env_vars", new=mock_get_env_vars
    )

    # Mock the ET.fromstring method
    mock_xml = Mock()
    mock_xml.xpath.return_value = ["12345"]
    mocker.patch("xml.etree.ElementTree.fromstring", return_value=mock_xml)

    # Mock the task_instance.xcom_pull method
    mock_task_instance = Mock()
    mock_task_instance.xcom_pull.return_value = ["resource1", "resource2"]
    mocker.patch(
        "airflow.models.TaskInstance.xcom_pull",
        return_value=mock_task_instance.xcom_pull,
    )

    # Mock the Variable.get method
    mock_variable_getter = Mock()
    mock_variable_getter.side_effect = lambda x: (
        "bucket_name" if x == "marc_s3_bucket" else None
    )
    mocker.patch("airflow.models.Variable.get", new=mock_variable_getter)

    # Mock the s3_hook.read_key method
    mock_s3_hook = Mock()
    mock_s3_hook.read_key.return_value = b"file_content"
    mocker.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.read_key",
        return_value=mock_s3_hook.read_key,
    )

    # Call the function with mock arguments
    NewWorktoAlma(
        dag=Mock(dag_id="penn"),
        task_instance=mock_task_instance,
    )


def test_parse_400():
    # Create a mock result
    result = "<root><error><errorMessage1>Test Error</errorMessage1></error></root>"
    # Call the function
    put_mms_id = parse_400(result)
    # Assert that the function returns the expected value
    assert put_mms_id == "No text found in brackets"


def test_putWorkToAlma_success():
    # Mock the requests module and return a mock response with a 200 status code
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<bib><mms_id>12345</mms_id></bib>"
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments
        task_instance = Mock()
        putWorkToAlma(
            alma_update_uri="https://example.com",
            data="<root></root>",
            task_instance=task_instance,
            instance_uri="test_instance_uri",
            put_mms_id_str="12345",
        )
        # Assert that the xcom_push method is called with the expected arguments
        task_instance.xcom_push.assert_called_once_with(
            key="test_instance_uri", value="12345"
        )


def test_putWorkToAlma_failure():
    # Mock the requests module and return a mock response with a non-200 status code
    mock_response = Mock()
    mock_response.status_code = 400
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments and expect it to raise an exception
        task_instance = Mock()
        with pytest.raises(Exception):
            putWorkToAlma(
                alma_update_uri="https://example.com",
                data="<root></root>",
                task_instance=task_instance,
                instance_uri="test_instance_uri",
                put_mms_id_str="12345",
            )


def test_putWorkToAlma_internal_server_error():
    # Mock the requests module and return a mock response with a 500 status code
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "<root>1234567</root>"
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments and expect it to raise an exception
        task_instance = Mock()
        with pytest.raises(Exception, match="Internal server error from Alma API: 500"):
            putWorkToAlma(
                alma_update_uri="https://example.com",
                data="<root>1234567</root>",
                task_instance=task_instance,
                instance_uri="test_instance_uri",
                put_mms_id_str="12345",
            )


def test_putWorkToAlma_unexpected_status_code():
    # Mock the requests module and return a mock response with a 400 status code
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.text = "<root>1234567</root>"
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments and expect it to raise an exception
        task_instance = Mock()
        with pytest.raises(
            Exception,
            match=f"Unexpected status code from Alma API: {mock_response.status_code}",
        ):
            putWorkToAlma(
                alma_update_uri="https://example.com",
                data="<root>1234567 </root>",
                task_instance=task_instance,
                instance_uri="test_instance_uri",
                put_mms_id_str="12345",
            )
