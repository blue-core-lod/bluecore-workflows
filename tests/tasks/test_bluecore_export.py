import logging

import pytest

from ils_middleware.tasks import bluecore_export
from ils_middleware.tasks.bluecore_export import record_instance_exports

INSTANCE_URI = "https://bcld.info/instances/7922d096-9b45-4235-be9a-a89d390bee83"
UUID = "7922d096-9b45-4235-be9a-a89d390bee83"

XCOMS = {
    ("exported", "process_folio.new-or-upsert-folio-records"): [UUID],
    (UUID, "api-message-parse"): {
        "resource_uri": INSTANCE_URI,
        "user_id": "48bac4b4-a4f1-4008-9f16-7dc0b51fd85e",
        "username": "dev_op",
        "updated_at": "2026-10-05T12:30:00",
    },
    (f"{UUID}-export", "process_folio.new-or-upsert-folio-records"): {
        "folio_id": "147b1171",
        "hrid": "in000780",
    },
}


@pytest.fixture
def task_instance(mocker):
    task_instance = mocker.Mock()
    task_instance.xcom_pull.side_effect = lambda key, task_ids: XCOMS.get(
        (key, task_ids)
    )
    return task_instance


@pytest.fixture
def session(mocker):
    mocker.patch.object(bluecore_export, "get_bluecore_db", return_value="db-url")
    mocker.patch.object(bluecore_export, "get_engine")
    session = mocker.MagicMock()
    session_maker = mocker.patch.object(bluecore_export, "sessionmaker")
    session_maker.return_value.return_value.__enter__.return_value = session
    return session


def _scalars_return(session, mocker, *results):
    session.scalars.side_effect = [
        mocker.Mock(first=mocker.Mock(return_value=result)) for result in results
    ]


def test_record_instance_exports(mocker, task_instance, session):
    instance = mocker.Mock(id=42, uri=INSTANCE_URI)
    version = mocker.Mock()
    _scalars_return(session, mocker, instance, version)
    mock_add = mocker.patch.object(bluecore_export, "add_instance_export")

    record_instance_exports(
        task_instance=task_instance, group="stanford", task_groups_ids=["process_folio"]
    )

    mock_add.assert_called_once_with(
        session,
        instance,
        target_identifier="in000780",
        version=version,
        keycloak_user_id="48bac4b4-a4f1-4008-9f16-7dc0b51fd85e",
        keycloak_username="dev_op",
        keycloak_group="stanford",
    )
    session.commit.assert_called_once()


def test_record_instance_exports_no_matching_version(
    mocker, task_instance, session, caplog
):
    instance = mocker.Mock(id=42, uri=INSTANCE_URI)
    _scalars_return(session, mocker, instance, None)
    mock_add = mocker.patch.object(bluecore_export, "add_instance_export")

    with caplog.at_level(logging.WARNING):
        record_instance_exports(
            task_instance=task_instance,
            group="stanford",
            task_groups_ids=["process_folio"],
        )

    assert mock_add.call_args.kwargs["version"] is None
    assert "recording the latest Version" in caplog.text


def test_record_instance_exports_missing_instance(mocker, task_instance, session):
    _scalars_return(session, mocker, None)
    mock_add = mocker.patch.object(bluecore_export, "add_instance_export")

    with pytest.raises(ValueError, match="No Instance found"):
        record_instance_exports(
            task_instance=task_instance,
            group="stanford",
            task_groups_ids=["process_folio"],
        )

    mock_add.assert_not_called()
    session.commit.assert_not_called()


def test_record_instance_exports_add_fails(mocker, task_instance, session):
    _scalars_return(session, mocker, mocker.Mock(id=42, uri=INSTANCE_URI), None)
    mocker.patch.object(
        bluecore_export,
        "add_instance_export",
        side_effect=ValueError("without a Keycloak user id"),
    )

    with pytest.raises(ValueError, match="Keycloak user id"):
        record_instance_exports(
            task_instance=task_instance,
            group="stanford",
            task_groups_ids=["process_folio"],
        )

    session.commit.assert_not_called()


def test_record_instance_exports_nothing_exported(mocker):
    task_instance = mocker.Mock()
    task_instance.xcom_pull.return_value = []
    mock_engine = mocker.patch.object(bluecore_export, "get_engine")

    record_instance_exports(
        task_instance=task_instance, group="cornell", task_groups_ids=[]
    )

    task_instance.xcom_pull.assert_called_once_with(
        key="exported", task_ids="new-or-upsert-folio-records"
    )
    mock_engine.assert_not_called()
