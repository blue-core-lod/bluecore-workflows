import pytest

from ils_middleware.tasks.general.marc import convert_to_xml, xslt_marc_to_bf

RECORD_MAR = "tests/fixtures/record.mar"
RECORD_XML = "tests/fixtures/record.xml"


def test_convert_to_xml_returns_str():
    result = convert_to_xml(RECORD_MAR)

    assert isinstance(result, str)
    assert "Vikingasilver" in result
    assert result.startswith("<?xml")


def test_convert_to_xml_no_records(tmp_path):
    empty_marc = tmp_path / "empty.mrc"
    empty_marc.touch()

    with pytest.raises(ValueError, match="Number of MARC records 0 should only be 1"):
        convert_to_xml(str(empty_marc))


def test_convert_to_xml_multiple_records(tmp_path):
    single_record = open(RECORD_MAR, "rb").read()
    two_records = tmp_path / "two.mrc"
    two_records.write_bytes(single_record + single_record)

    with pytest.raises(ValueError, match="Number of MARC records 2 should only be 1"):
        convert_to_xml(str(two_records))


def test_xslt_marc_to_bf_returns_str():
    marc_xml = open(RECORD_XML).read()

    result = xslt_marc_to_bf(marc_xml, "http://example.org/base/")

    assert isinstance(result, str)
    assert "<rdf:RDF" in result
    assert "http://example.org/base/" in result


def test_xslt_marc_to_bf_accepts_xml_declaration():
    # Regression test: convert_to_xml's output (and MARC XML fixtures) include an
    # XML declaration, which lxml.etree.fromstring rejects when given a str. This
    # exercises the fix that re-encodes to bytes before parsing.
    marc_xml = open(RECORD_XML).read()
    assert marc_xml.startswith("<?xml")

    result = xslt_marc_to_bf(marc_xml, "http://example.org/base/")

    assert "bf:Work" in result or "bf:Instance" in result


def test_convert_to_xml_then_xslt_marc_to_bf_roundtrip():
    marc_xml = convert_to_xml(RECORD_MAR)

    bf_rdf_xml = xslt_marc_to_bf(marc_xml, "http://id.loc.gov/resources/")

    assert isinstance(bf_rdf_xml, str)
    assert "<rdf:RDF" in bf_rdf_xml
