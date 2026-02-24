import logging
from unittest.mock import Mock, patch
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa
from ils_middleware.tasks.amazon.alma_instance_s3 import send_instance_to_alma_s3
import ssl

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def mock_httpx_get():
    mock_httpx = Mock()
    mock_httpx.json = lambda: {"data": []}
    return mock_httpx


@patch(
    "ils_middleware.tasks.amazon.alma_instance_s3.httpx.get",
    return_value=mock_httpx_get(),
)
@patch.dict(
    "os.environ", {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}
)
@patch("airflow.models.Variable.get")
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
@patch("rdflib.Graph")
@patch("lxml.etree")
@patch.object(S3Hook, "load_bytes", return_value=None)
def test_send_instance_to_alma_s3(
    mock_load_bytes,
    mock_etree,
    mock_graph,
    mock_s3_hook,
    mock_get_connection,
    mock_variable,
    mock_httpx_get,
):
    # Arrange
    mock_task_instance = Mock()
    ssl._create_default_https_context = ssl._create_unverified_context
    mock_task_instance.xcom_pull.return_value = [
        "https://api.stage.sinopia.io/resource/2b09bb49-549d-4851-9a52-5feb0abc0843"
    ]
    mock_graph_instance = mock_graph.return_value
    mock_graph_instance.serialize.return_value = b"<test></test>"
    mock_graph_instance.parse.return_value = (
        None  # Mock the parse method on the instance
    )
    mock_etree.XSLT.return_value = Mock()  # Mock the XSLT transformation
    mock_etree.fromstring.return_value = Mock()  # Mock the fromstring method
    mock_etree.tostring.return_value = b"<test></test>"  # Mock the tostring method
    mock_variable.return_value = "test_bucket"
    mock_connection = Mock()
    mock_connection.login = "test_access_key_id"
    mock_connection.password = "test_secret_access_key"
    mock_get_connection.return_value = mock_connection
    mock_s3_hook_instance = Mock()
    mock_s3_hook.return_value = mock_s3_hook_instance
    mock_s3_hook.get_connection = Mock(return_value=mock_connection)

    # Act
    send_instance_to_alma_s3(task_instance=mock_task_instance)

    # Log the calls to verify behavior
    logger.debug(f"xcom_pull calls: {mock_task_instance.xcom_pull.call_args_list}")
    logger.debug(f"parse calls: {mock_graph_instance.parse.call_args_list}")
    logger.debug(f"serialize calls: {mock_graph_instance.serialize.call_args_list}")
    logger.debug(f"fromstring calls: {mock_etree.fromstring.call_args_list}")
    logger.debug(f"XSLT calls: {mock_etree.XSLT.call_args_list}")
    logger.debug(f"tostring calls: {mock_etree.tostring.call_args_list}")
    logger.debug(f"load_bytes calls: {mock_s3_hook_instance.load_bytes.call_args_list}")
    logger.debug(f"xcom_push calls: {mock_task_instance.xcom_push.call_args_list}")


# Run the test
test_send_instance_to_alma_s3()
