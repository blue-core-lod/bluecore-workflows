from ils_middleware.tasks.keycloak import get_bluecore_members
from tests.keycloak.server_mocks import mock_keycloak  # type: ignore #noqa: F401


def test_get_bluecore_members(mock_keycloak):  # noqa: F811
    user_membership = get_bluecore_members()

    assert user_membership["dev_op"]["email"].startswith("dev_op@bluecore.edu")
    assert user_membership["dev_user"]["groups"] == ["loc"]
