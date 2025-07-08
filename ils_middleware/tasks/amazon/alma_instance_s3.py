import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdflib import Graph, URIRef, Namespace
from lxml import etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

logger = logging.getLogger(__name__)


def send_instance_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    for instance_uri in resources:
        instance_uri = URIRef(instance_uri)
        work_uri = None
        instance_graph = Graph()
        instance_graph.parse(instance_uri)

        # Define the bf namespace
        bf = Namespace("http://id.loc.gov/ontologies/bibframe/")

        # Bind the namespaces to the instance graph
        for prefix, url in alma_namespaces:
            instance_graph.bind(prefix, url)

        # Get the work URI from the instance graph
        work_uri = instance_graph.value(subject=instance_uri, predicate=bf.instanceOf)

        # Check if work_uri is none
        if work_uri is None:
            logger.error(f"No work URI found for instance {instance_uri}")
            continue

        # Ensure work_uri is a URIRef
        work_uri = URIRef(work_uri)

        # Remove any triples where work_uri is the subject
        instance_graph.remove((work_uri, None, None))

        # Serialize the instance graph
        instance_alma_xml = instance_graph.serialize(
            format="pretty-xml", encoding="utf-8"
        )

        # Parse the serialized XML
        tree = ET.fromstring(instance_alma_xml)

        # Log the XML before XSLT transformation
        logger.debug(
            f"XML before XSLT transformation: {ET.tostring(tree, pretty_print=True, encoding='utf-8').decode('utf-8')}"
        )

        # Apply XSLT to normalize instance
        xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-instance.xsl")
        transform = ET.XSLT(xslt)
        instance_alma_xml = transform(tree)
        instance_alma_xml = ET.tostring(
            instance_alma_xml, pretty_print=True, encoding="utf-8"
        )
        # post to s3 as bytes
        s3_hook.load_bytes(
            instance_alma_xml,
            f"alma/{instance_uri}/bfinstance_alma.xml",
            Variable.get("marc_s3_bucket"),
            replace=True,
        )
        # save to xcom
        instance_uuid = instance_uri.split("/")[-1]
        task_instance.xcom_push(
            key=instance_uuid, value=instance_alma_xml.decode("utf-8")
        )
        logger.info(f"Saved BFInstance description for {instance_uri} to alma.")
        logger.info(f"bf_instance_alma_xml: {instance_alma_xml.decode('utf-8')}")
