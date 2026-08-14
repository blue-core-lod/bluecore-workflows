import pytest

from ils_middleware.tasks.general.marc import (
    convert_to_xml,
    replace_dlc_assigner,
    xslt_marc_to_bf,
)

DLC_URI = "http://id.loc.gov/vocabulary/organizations/dlc"
CBC_URI = "http://id.loc.gov/vocabulary/organizations/cbc"

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
    with open(RECORD_MAR, "rb") as fo:
        single_record = fo.read()
    two_records = tmp_path / "two.mrc"
    two_records.write_bytes(single_record + single_record)

    with pytest.raises(ValueError, match="Number of MARC records 2 should only be 1"):
        convert_to_xml(str(two_records))


def test_xslt_marc_to_bf_returns_str():
    with open(RECORD_XML) as fo:
        marc_xml = fo.read()

    result = xslt_marc_to_bf(marc_xml, "http://example.org/base/")

    assert isinstance(result, str)
    assert "<rdf:RDF" in result
    assert "http://example.org/base/" in result


def test_xslt_marc_to_bf_accepts_xml_declaration():
    # Regression test: convert_to_xml's output (and MARC XML fixtures) include an
    # XML declaration, which lxml.etree.fromstring rejects when given a str. This
    # exercises the fix that re-encodes to bytes before parsing.
    with open(RECORD_XML) as fo:
        marc_xml = fo.read()
    assert marc_xml.startswith("<?xml")

    result = xslt_marc_to_bf(marc_xml, "http://example.org/base/")

    assert "bf:Work" in result or "bf:Instance" in result


def test_convert_to_xml_then_xslt_marc_to_bf_roundtrip():
    marc_xml = convert_to_xml(RECORD_MAR)

    bf_rdf_xml = xslt_marc_to_bf(marc_xml, "http://id.loc.gov/resources/")

    assert isinstance(bf_rdf_xml, str)
    assert "<rdf:RDF" in bf_rdf_xml


def test_replace_dlc_assigner_replaces_dlc_with_cbc():
    rdf_xml = f'<bf:assigner rdf:resource="{DLC_URI}"/>'

    result = replace_dlc_assigner(rdf_xml)

    assert DLC_URI not in result
    assert CBC_URI in result


def test_replace_dlc_assigner_preserves_other_assigners():
    other_uri = "http://id.loc.gov/vocabulary/organizations/cst"
    rdf_xml = f'<bf:assigner rdf:resource="{other_uri}"/>'

    result = replace_dlc_assigner(rdf_xml)

    assert other_uri in result
    assert CBC_URI not in result


def test_replace_dlc_assigner_replaces_all_occurrences():
    rdf_xml = (
        f'<bf:assigner rdf:resource="{DLC_URI}"/>\n'
        f'<bf:Agent rdf:about="{DLC_URI}"/>\n'
        f'<bf:assigner rdf:resource="http://id.loc.gov/vocabulary/organizations/pu"/>'
    )

    result = replace_dlc_assigner(rdf_xml)

    assert DLC_URI not in result
    assert result.count(CBC_URI) == 2
    assert "http://id.loc.gov/vocabulary/organizations/pu" in result
