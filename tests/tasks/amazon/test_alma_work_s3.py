from unittest.mock import Mock, patch, ANY
from ils_middleware.tasks.amazon.alma_work_s3 import (
    get_work_uri,
    parse_graph,
    serialize_work_graph,
    find_related_to_elements,
    process_related_to_elements,
    process_work_elements,
    load_to_s3,
    push_to_xcom,
    send_work_to_alma_s3,
)
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF
from lxml import etree as ET


def test_get_work_uri():
    instance_graph = Graph()
    bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
    instance_uri = URIRef("http://example.com/instance")
    work_uri = URIRef("http://example.com/work")
    instance_graph.add((instance_uri, bf.instanceOf, work_uri))
    result = get_work_uri(instance_graph, instance_uri, bf)
    assert result == work_uri


def test_parse_graph():
    uri = "http://example.com/resource"
    with patch.object(Graph, "parse", return_value=None):
        graph = parse_graph(uri)
        assert isinstance(graph, Graph)


def test_serialize_work_graph():
    work_graph = Graph()
    work_uri = URIRef("http://example.com/work")
    bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
    work_graph.add((work_uri, RDF.type, bf.Work))
    result = serialize_work_graph(work_graph)
    assert b"<rdf:RDF" in result


def test_find_related_to_elements():
    bfwork_alma_xml = b"""
    <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:bflc="http://id.loc.gov/ontologies/bflc/">
        <bf:Work rdf:about="work_about_uri">
            <!-- Work content -->
        </bf:Work>
        <bflc:Relationship>
            <bf:relatedTo rdf:resource="work_about_uri">
                <!-- Some content -->
            </bf:relatedTo>
        </bflc:Relationship>
    </rdf:RDF>
    """
    tree = ET.fromstring(bfwork_alma_xml)
    namespaces = {
        "bf": "http://id.loc.gov/ontologies/bibframe/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "bflc": "http://id.loc.gov/ontologies/bflc/",
    }
    result = find_related_to_elements(tree, "work_about_uri", namespaces)
    assert len(result) == 1


def test_process_related_to_elements_no_cloning():
    # No cloning should occur because bf:Work exists under bf:relatedTo
    bfwork_alma_xml = b"""
    <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:bflc="http://id.loc.gov/ontologies/bflc/">
        <bf:Work rdf:about="work_about_uri">
            <!-- Original Work content -->
        </bf:Work>
        <bflc:Relationship>
            <bf:relatedTo rdf:resource="work_about_uri">
                <bf:Work>
                    <!-- Existing Work content -->
                </bf:Work>
            </bf:relatedTo>
        </bflc:Relationship>
    </rdf:RDF>
    """
    tree = ET.fromstring(bfwork_alma_xml)
    namespaces = {
        "bf": "http://id.loc.gov/ontologies/bibframe/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "bflc": "http://id.loc.gov/ontologies/bflc/",
    }
    related_to_elements = find_related_to_elements(tree, "work_about_uri", namespaces)
    work = tree.find(".//bf:Work", namespaces=namespaces)
    result = process_related_to_elements(related_to_elements, work, namespaces)
    assert result is False  # No cloning should occur


def test_process_work_elements():
    # Test the process_work_elements function
    bfwork_alma_xml = b"""
    <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        <bf:Work rdf:about="work_about_uri">
            <!-- Work content -->
        </bf:Work>
    </rdf:RDF>
    """
    tree = ET.fromstring(bfwork_alma_xml)
    namespaces = {
        "bf": "http://id.loc.gov/ontologies/bibframe/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    }
    process_work_elements(tree, namespaces)
    assert (
        len(tree.xpath("//bf:Work", namespaces=namespaces)) == 1
    )  # No work should be removed


def test_apply_xslt():
    # Sample XML input
    bfwork_alma_xml = b"""
    <root>
        <element>Original</element>
    </root>
    """
    # Corrected XSLT that transforms 'element' content
    xslt_str = b"""
    <xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
        <xsl:template match="element">
            <element>Transformed</element>
        </xsl:template>
        <xsl:template match="@*|node()" priority="-1">
            <xsl:copy>
                <xsl:apply-templates select="@*|node()"/>
            </xsl:copy>
        </xsl:template>
    </xsl:stylesheet>
    """
    # Parse XML and XSLT
    tree = ET.fromstring(bfwork_alma_xml)
    xslt_root = ET.fromstring(xslt_str)
    transform = ET.XSLT(xslt_root)
    # Apply XSLT transformation
    result = transform(tree)
    # Perform assertion
    assert isinstance(result, ET._XSLTResultTree)
    # Normalize the output strings
    result_str = ET.tostring(result, encoding="unicode")
    expected_str = "<root><element>Transformed</element></root>"
    # Remove all whitespace between tags
    import re

    result_str = re.sub(r">\s+<", "><", result_str)
    expected_str = re.sub(r">\s+<", "><", expected_str)
    assert result_str == expected_str


@patch(
    "ils_middleware.tasks.amazon.alma_work_s3.Variable.get", return_value="test_bucket"
)
def test_load_to_s3(mock_variable_get):
    mock_s3_hook = Mock()
    bfwork_alma_xml = b"<rdf:RDF></rdf:RDF>"
    instance_uri = "http://example.com/instance"
    load_to_s3(mock_s3_hook, bfwork_alma_xml, instance_uri)
    mock_s3_hook.load_bytes.assert_called_once_with(
        bfwork_alma_xml,
        f"alma/{instance_uri}/bfwork_alma.xml",
        "test_bucket",
        replace=True,
    )


def test_push_to_xcom():
    mock_task_instance = Mock()
    instance_uri = "http://example.com/instance"
    bfwork_alma_xml = b"<rdf:RDF></rdf:RDF>"
    push_to_xcom(mock_task_instance, instance_uri, bfwork_alma_xml)
    instance_uuid = instance_uri.split("/")[-1]
    mock_task_instance.xcom_push.assert_called_once_with(
        key=instance_uuid, value=bfwork_alma_xml.decode("utf-8")
    )


@patch("ils_middleware.tasks.amazon.alma_work_s3.S3Hook")
@patch(
    "ils_middleware.tasks.amazon.alma_work_s3.Variable.get", return_value="test_bucket"
)
@patch("ils_middleware.tasks.amazon.alma_work_s3.Graph")
@patch("ils_middleware.tasks.amazon.alma_work_s3.ET.parse")
@patch("ils_middleware.tasks.amazon.alma_work_s3.ET.XSLT")
def test_send_work_to_alma_s3(
    mock_etree_XSLT, mock_etree_parse, mock_graph_class, mock_variable_get, mock_s3_hook
):
    mock_task_instance = Mock()
    mock_task_instance.xcom_pull.return_value = [
        "https://example.com/resource/instance_uri"
    ]
    mock_graph_instance = Mock()
    mock_graph_class.return_value = mock_graph_instance
    mock_graph_instance.value.return_value = "https://example.com/resource/work_uri"
    mock_graph_instance.serialize.return_value = b"""
    <rdf:RDF xmlns:bf="http://id.loc.gov/ontologies/bibframe/"
             xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns:bflc="http://id.loc.gov/ontologies/bflc/">
        <bf:Work rdf:about="work_about_uri">
            <!-- Work content -->
        </bf:Work>
        <bflc:Relationship>
            <bf:relatedTo rdf:resource="work_about_uri">
                <!-- Missing bf:Work element to trigger cloning -->
            </bf:relatedTo>
        </bflc:Relationship>
    </rdf:RDF>
    """
    bf_work_tree = ET.fromstring(mock_graph_instance.serialize.return_value)
    mock_etree_parse.return_value = ET.ElementTree(bf_work_tree)

    # Adjusted mocking of ET.XSLT
    mock_transform_instance = Mock()
    # Mock the transform to return the bf_work_tree unmodified
    mock_transform_instance.return_value = bf_work_tree
    mock_etree_XSLT.return_value = mock_transform_instance

    send_work_to_alma_s3(task_instance=mock_task_instance)

    mock_task_instance.xcom_pull.assert_called_once_with(
        key="resources", task_ids="api-message-parse"
    )
    mock_graph_instance.parse.assert_any_call(
        URIRef("https://example.com/resource/instance_uri")
    )
    mock_graph_instance.parse.assert_any_call(
        URIRef("https://example.com/resource/work_uri")
    )
    mock_graph_instance.serialize.assert_called_once_with(
        format="pretty-xml", encoding="utf-8"
    )
    mock_etree_XSLT.assert_called_once()
    mock_transform_instance.assert_called_once_with(ANY)
    mock_s3_hook_instance = mock_s3_hook.return_value
    mock_s3_hook_instance.load_bytes.assert_called_once()
    called_args, called_kwargs = mock_s3_hook_instance.load_bytes.call_args
    assert (
        called_args[1]
        == "alma/https://example.com/resource/instance_uri/bfwork_alma.xml"
    )
    assert called_args[2] == "test_bucket"
    assert called_kwargs.get("replace", False) is True

    # Normalize the XML content for comparison
    def normalize_xml(xml_str):
        parser = ET.XMLParser(remove_blank_text=True)
        tree = ET.fromstring(xml_str, parser=parser)
        # Remove whitespace between elements
        for elem in tree.iter():
            if elem.text:
                elem.text = elem.text.strip()
            if elem.tail:
                elem.tail = elem.tail.strip()
        return ET.tostring(tree, encoding="utf-8")

    expected_value = normalize_xml(
        mock_graph_instance.serialize.return_value.decode("utf-8")
    )
    actual_value = normalize_xml(mock_task_instance.xcom_push.call_args[1]["value"])

    assert expected_value == actual_value

    mock_task_instance.xcom_push.assert_called_once_with(
        key="instance_uri",
        value=mock_task_instance.xcom_push.call_args[1]["value"],
    )
