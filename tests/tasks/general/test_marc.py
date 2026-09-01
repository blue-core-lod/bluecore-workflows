import pytest

from ils_middleware.tasks.general.marc import convert_to_xml

RECORD_MAR = "tests/fixtures/record.mar"


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
    two_records.write_bytes(single_record * 2)

    with pytest.raises(ValueError, match="Number of MARC records 2 should only be 1"):
        convert_to_xml(str(two_records))
