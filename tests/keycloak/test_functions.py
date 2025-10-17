from ils_middleware.tasks.keycloak import get_bluecore_members, get_user_profile
from tests.keycloak.server_mocks import mock_keycloak  # type: ignore #noqa: F401


def test_get_bluecore_members(mock_keycloak):  # noqa: F811
    user_membership = get_bluecore_members()

    assert user_membership["dev_op"]["email"].startswith("dev_op@bluecore.edu")
    assert user_membership["dev_user"]["groups"] == ["loc"]


def test_get_user_profile(mock_keycloak):  # noqa: F811
    user = get_user_profile("48bac4b4-a4f1-4008-9f16-7dc0b51fd85e")

    assert user["email"] == "dev_op@bluecore.org"
    assert user["groups"] == ["ucdavis"]
    assert user["username"] == "dev_op"
