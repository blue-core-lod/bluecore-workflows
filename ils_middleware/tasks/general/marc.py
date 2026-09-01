import pathlib

from marc_bibframe import marc_to_marcxml


def convert_to_xml(marc_file: str) -> str:
    """
    Convert MARC21 to MARC XML
    """
    marc_path = pathlib.Path(marc_file)
    marcxml = marc_to_marcxml(marc_path.read_bytes())
    # The upload is a single record; more than one means the caller sent
    # something we are not set up to handle downstream.
    count = marcxml.count(b"<record>")
    if count != 1:
        raise ValueError(f"Number of MARC records {count} should only be 1")
    return marcxml.decode("utf-8")
