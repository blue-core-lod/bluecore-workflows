import pytest

from ils_middleware.tasks.bluecore.storage import get_bluecore_db


class MockPostgresHook(object):
    def __init__(self, *args):
        self.sqlalchemy_url = (
            "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore"
        )


@pytest.fixture
def mock_postgres_hook(mocker):
    mocker.patch(
        "ils_middleware.tasks.bluecore.storage.PostgresHook",
        return_value=MockPostgresHook(),
    )
    return mocker


def test_get_bluecore_db(mock_postgres_hook):
    db_string = get_bluecore_db()

    assert db_string.startswith("postgresql://bluecore_admin")
