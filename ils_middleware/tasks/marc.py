"""MARC21 to BIBFRAME conversion tasks for Airflow workflows."""

import io
import logging
import pathlib
import tempfile
from typing import BinaryIO

import pymarc
import rdflib
from lxml import etree

logger = logging.getLogger(__name__)

# XSLT file path - relative to the Airflow DAG directory
XSLT_BASE_PATH = pathlib.Path("/opt/airflow/plugins/toolbox/xslt")


def marc21_to_marcxml(marc_file_path: str) -> bytes:
    """
    Convert MARC21 binary format to MARC XML.

    Args:
        marc_file_path: Path to MARC21 (.mrc or .marc) file

    Returns:
        MARC XML as bytes

    Raises:
        ValueError: If file cannot be read or parsed
    """
    try:
        file_path = pathlib.Path(marc_file_path)
        logger.info(f"Converting MARC21 file to XML: {file_path}")

        with open(file_path, "rb") as f:
            raw_marc = f.read()

        # Parse MARC21 binary
        marc_reader = pymarc.MARCReader(raw_marc, to_unicode=True, force_utf8=True)
        marc_record = next(marc_reader)

        # Convert to MARC XML
        marc_bytes = io.BytesIO()
        writer = pymarc.XMLWriter(marc_bytes)
        writer.write(marc_record)
        writer.close(close_fh=False)

        marc_xml = marc_bytes.getvalue()
        logger.info(f"Successfully converted MARC21 to XML ({len(marc_xml)} bytes)")
        return marc_xml

    except StopIteration:
        raise ValueError(f"No MARC records found in file: {marc_file_path}")
    except Exception as e:
        logger.error(f"Failed to convert MARC21 to XML: {e}")
        raise ValueError(f"Failed to parse MARC21 file: {e}")


def marcxml_to_bibframe(marc_xml: bytes) -> str:
    """
    Convert MARC XML to BIBFRAME RDF/XML using marc2bibframe2 XSLT.

    Args:
        marc_xml: MARC XML as bytes

    Returns:
        BIBFRAME RDF/XML as string

    Raises:
        ValueError: If XSLT transformation fails
    """
    try:
        logger.info("Converting MARC XML to BIBFRAME")

        # Load XSLT transformation
        xslt_path = XSLT_BASE_PATH / "marc2bf" / "marc2bibframe2.xsl"
        if not xslt_path.exists():
            raise ValueError(f"XSLT file not found: {xslt_path}")

        xslt_root = etree.parse(str(xslt_path))
        marc2bf_xslt = etree.XSLT(xslt_root)

        # Parse MARC XML
        marc_doc = etree.XML(marc_xml)

        # Apply XSLT transformation
        bf_xml = marc2bf_xslt(marc_doc)
        bf_xml_str = str(bf_xml)

        logger.info(f"Successfully converted to BIBFRAME ({len(bf_xml_str)} bytes)")
        return bf_xml_str

    except Exception as e:
        logger.error(f"Failed to convert MARC XML to BIBFRAME: {e}")
        raise ValueError(f"XSLT transformation failed: {e}")


def bibframe_to_jsonld(bibframe_xml: str) -> str:
    """
    Convert BIBFRAME RDF/XML to JSON-LD.

    Args:
        bibframe_xml: BIBFRAME RDF/XML as string

    Returns:
        JSON-LD as string

    Raises:
        ValueError: If parsing or serialization fails
    """
    try:
        logger.info("Converting BIBFRAME to JSON-LD")

        # Parse BIBFRAME RDF/XML into graph
        graph = rdflib.Graph()
        graph.parse(data=bibframe_xml, format="xml")

        logger.info(f"Parsed BIBFRAME graph with {len(graph)} triples")

        # Serialize to JSON-LD
        jsonld = graph.serialize(format="json-ld", indent=2)

        logger.info(f"Successfully converted to JSON-LD ({len(jsonld)} bytes)")
        return jsonld

    except Exception as e:
        logger.error(f"Failed to convert BIBFRAME to JSON-LD: {e}")
        raise ValueError(f"Failed to serialize to JSON-LD: {e}")


def marc21_to_jsonld(marc_file_path: str) -> str:
    """
    Convert MARC21 file to JSON-LD (complete pipeline).

    This is the main entry point that combines all conversion steps:
    1. MARC21 binary → MARC XML
    2. MARC XML → BIBFRAME RDF/XML
    3. BIBFRAME → JSON-LD

    Args:
        marc_file_path: Path to MARC21 file (.mrc, .marc, or .xml)

    Returns:
        JSON-LD as string, ready for load() task

    Raises:
        ValueError: If any conversion step fails
    """
    file_path = pathlib.Path(marc_file_path)
    logger.info(f"Starting MARC21 to JSON-LD conversion for: {file_path}")

    # Determine file type and handle accordingly
    ext = file_path.suffix.lower()

    if ext in [".mrc", ".marc"]:
        # Binary MARC21 - needs conversion to XML first
        marc_xml = marc21_to_marcxml(marc_file_path)
    elif ext == ".xml":
        # Already MARC XML - read directly
        logger.info("File is already MARC XML")
        with open(file_path, "rb") as f:
            marc_xml = f.read()
    else:
        raise ValueError(
            f"Unsupported file extension: {ext}. "
            f"Expected .mrc, .marc, or .xml"
        )

    # Convert MARC XML to BIBFRAME
    bibframe_xml = marcxml_to_bibframe(marc_xml)

    # Convert BIBFRAME to JSON-LD
    jsonld = bibframe_to_jsonld(bibframe_xml)

    logger.info(f"Conversion complete: {file_path} → JSON-LD")
    return jsonld


def save_jsonld_to_file(jsonld: str, output_path: str) -> str:
    """
    Save JSON-LD string to file for processing by load() task.

    Args:
        jsonld: JSON-LD as string
        output_path: Path where to save the file

    Returns:
        Path to saved file
    """
    output_file = pathlib.Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(jsonld)

    logger.info(f"Saved JSON-LD to: {output_file}")
    return str(output_file)
