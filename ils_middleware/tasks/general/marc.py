import io
import pathlib

import lxml.etree as ET
import pymarc

MARC2BIBFRAME2_XSL = "ils_middleware/tasks/general/xslt/marc2bibframe2/xsl/marc2bibframe2.xsl"

def convert_to_xml(marc_file: str) -> str:
    """
    Convert MARC21 to MARC XML
    """
    marc_path = pathlib.Path(marc_file)
    with marc_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(fo)
        marc_records = [r for r in marc_reader]
        if len(marc_records) != 1:
            raise ValueError(f"Number of MARC records {len(marc_records)} should only be 1")
        marc_record = marc_records[0]
    memory = io.BytesIO()
    writer = pymarc.XMLWriter(memory)
    writer.write(marc_record)
    writer.close(close_fh=False)
    return memory.getvalue().decode("utf-8")

def xslt_marc_to_bf(marc_xml: str, source_base_uri: str) -> str:
    """
    Takes MARC XML and transforms into BIBFRAME RDF XML using the Library of
    Congress marc2bibframe2 at https://github.com/lcnetdev/marc2bibframe2/
    """
    marc_record = ET.fromstring(marc_xml.encode("utf-8"))
    xslt = ET.parse(MARC2BIBFRAME2_XSL)
    transform = ET.XSLT(xslt)
    bf_rdf_xml = transform(marc_record, baseuri=ET.XSLT.strparam(source_base_uri))
    return ET.tostring(bf_rdf_xml, pretty_print=True, encoding="unicode")