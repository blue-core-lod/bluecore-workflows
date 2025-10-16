import pytest

from ils_middleware.tasks.keycloak import get_bluecore_members, get_user_groups
from tests.keycloak.server_mocks import mock_keycloak  # type: ignore #noqa: F401


def test_get_bluecore_members(mock_keycloak):  # noqa: F811
    user_membership = get_bluecore_members()

    assert user_membership["dev_op"]["email"].startswith("dev_op@bluecore.edu")
    assert user_membership["dev_user"]["groups"] == ["loc"]


def test_get_user_groups(mock_keycloak):  # noqa: F811
    groups = get_user_groups("dev_op")

    assert groups == ["ucdavis"]


def test_get_user_groups_multiple_users(mock_keycloak):  # noqa: F811
    with pytest.raises(ValueError, match="Multiple users found for username poly_user"):
        get_user_groups("poly_user")


def test_get_user_groups_no_user(mock_keycloak):  # noqa: F811
    with pytest.raises(ValueError, match="aearhart not found"):
        get_user_groups("aearhart")
