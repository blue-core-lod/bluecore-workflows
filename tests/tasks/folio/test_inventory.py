import pytest

from ils_middleware.tasks.folio.inventory import cql_quote, lookup_by_local_id


@pytest.mark.parametrize(
    "value,quoted",
    [
        ("a123", "a123"),
        ('a"123', 'a\\"123'),
        ("a\\123", "a\\\\123"),
        ("a*1?2^3", "a\\*1\\?2\\^3"),
    ],
)
def test_cql_quote(value, quoted):
    assert cql_quote(value) == quoted


def test_lookup_by_local_id_quotes_hrid(mocker):
    folio_client = mocker.MagicMock()
    folio_client.folio_get.return_value = []

    assert lookup_by_local_id(' a"1* ', folio_client) is None
    folio_client.folio_get.assert_called_once_with(
        "/inventory/instances", key="instances", query='hrid=="a\\"1\\*"'
    )
