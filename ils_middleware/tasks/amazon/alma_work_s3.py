import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF
from copy import deepcopy
from lxml import etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

logger = logging.getLogger(__name__)


def get_work_uri(instance_graph, instance_uri, bf):
    return instance_graph.value(subject=URIRef(instance_uri), predicate=bf.instanceOf)


def parse_graph(uri):
    graph = Graph()
    graph.parse(uri)
    return graph


def serialize_work_graph(work_graph):
    return work_graph.serialize(format="pretty-xml", encoding="utf-8")


def find_related_to_elements(tree, work_about, namespaces):
    return tree.xpath(
        f'//bf:relatedTo[@rdf:resource="{work_about}"]',
        namespaces=namespaces,
    )


def process_related_to_elements(related_to_elements, work, namespaces):
    cloned = False
    for related_to in related_to_elements:
        relationship = related_to.getparent()
        if (
            relationship.tag
            in (
                "{http://id.loc.gov/ontologies/bflc/}Relationship",
                "{http://id.loc.gov/ontologies/bibframe/}Relationship",
            )
            and relationship.find("bf:relatedTo/bf:Work", namespaces=namespaces)
            is not None
        ):
            logger.info(
                "bf:relatedTo element is within a bflc:Relationship that contains a bf:Work element"
            )
            related_to.attrib.pop(
                "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", None
            )
            existing_work = related_to.find("bf:Work", namespaces=namespaces)
            if existing_work is None:
                cloned_work = deepcopy(work)
                related_to.append(cloned_work)
                cloned = True
    return cloned


def process_work_elements(tree, namespaces):
    works_to_remove = []
    works = tree.xpath("//bf:Work", namespaces=namespaces)
    logger.info(f"Found {len(works)} bf:Work elements")

    for work in works:
        work_about = work.attrib.get(
            "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
        )
        logger.info(f"Processing bf:Work with rdf:about={work_about}")

        if work_about:
            related_to_elements = find_related_to_elements(tree, work_about, namespaces)

            if process_related_to_elements(related_to_elements, work, namespaces):
                works_to_remove.append(work)

    for work in works_to_remove:
        work.getparent().remove(work)
        logger.info(
            f"Removed original bf:Work element with rdf:about= \
            {work.attrib.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about')}"
        )


def apply_xslt(tree):
    xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-work.xsl")
    transform = ET.XSLT(xslt)
    return transform(tree)


def load_to_s3(s3_hook, bfwork_alma_xml, instance_uri):
    s3_hook.load_bytes(
        bfwork_alma_xml,
        f"alma/{instance_uri}/bfwork_alma.xml",
        Variable.get("marc_s3_bucket"),
        replace=True,
    )


def push_to_xcom(task_instance, instance_uri, bfwork_alma_xml):
    instance_uuid = instance_uri.split("/")[-1]
    task_instance.xcom_push(key=instance_uuid, value=bfwork_alma_xml.decode("utf-8"))


def send_work_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="api-message-parse")

    for instance_uri in resources:
        instance_uri = URIRef(instance_uri)
        bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
        namespaces = {
            "bf": "http://id.loc.gov/ontologies/bibframe/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "bflc": "http://id.loc.gov/ontologies/bflc/",
        }

        instance_graph = parse_graph(instance_uri)
        work_uri = get_work_uri(instance_graph, instance_uri, bf)
        work_uri = URIRef(work_uri)
        work_graph = parse_graph(work_uri)
        work_graph.add((work_uri, RDF.type, bf.Work))

        for prefix, url in alma_namespaces:
            work_graph.bind(prefix, Namespace(url))

        bfwork_alma_xml = serialize_work_graph(work_graph)
        tree = ET.fromstring(bfwork_alma_xml)

        process_work_elements(tree, namespaces)
        bfwork_alma_xml = apply_xslt(tree)
        bfwork_alma_xml = ET.tostring(
            bfwork_alma_xml, pretty_print=True, encoding="utf-8"
        )

        logger.info(f"Normalized BFWork description for {work_uri}.")
        load_to_s3(s3_hook, bfwork_alma_xml, instance_uri)
        push_to_xcom(task_instance, instance_uri, bfwork_alma_xml)
        logger.info(f"bf_work_alma_xml: {bfwork_alma_xml.decode('utf-8')}")
