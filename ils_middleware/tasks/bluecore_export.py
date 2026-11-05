"""Records each successful export to an institution's catalog in Blue Core's
instance_exports table.

Only FOLIO (the stanford and cornell DAGs) is wired up. Alma would plug in
after its post-to-Alma task, pushing the same "exported" and "{uuid}-export"
XComs with the MMS ID as the hrid.
"""

import logging
from datetime import UTC, datetime

from bluecore_models.models import Instance, Version
from bluecore_models.utils.db import add_instance_export
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ils_middleware.tasks.bluecore import get_bluecore_db, get_engine

logger = logging.getLogger(__name__)


def _exported_version(session, instance: Instance, updated_at: str | None):
    """
    Returns the Version that was exported. add_version sets Version.created_at
    from the resource's updated_at, so the updated_at the DAG retrieved
    identifies it. Returns None, for add_instance_export to use the latest
    Version, when there is no match.
    """
    if updated_at:
        exported_at = datetime.fromisoformat(updated_at)
        # created_at is a naive UTC column
        if exported_at.tzinfo is not None:
            exported_at = exported_at.astimezone(UTC).replace(tzinfo=None)
        version = session.scalars(
            select(Version).where(
                Version.resource_id == instance.id,
                Version.created_at == exported_at,
            )
        ).first()
        if version is not None:
            return version
    logger.warning(
        f"No Version of {instance.uri} matches updated_at {updated_at}; "
        "recording the latest Version"
    )
    return None


def record_instance_exports(**kwargs):
    """Adds an InstanceExport for each instance successfully sent to the catalog"""
    task_instance = kwargs["task_instance"]
    group = kwargs["group"]

    task_groups = ".".join(kwargs["task_groups_ids"])
    task_id = "new-or-upsert-folio-records"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"

    exported = task_instance.xcom_pull(key="exported", task_ids=task_id)
    if not exported:
        logger.info("No exported instances to record")
        return

    session_maker = sessionmaker(bind=get_engine(get_bluecore_db()))
    with session_maker() as session:
        for instance_uuid in exported:
            message = task_instance.xcom_pull(
                key=instance_uuid, task_ids="api-message-parse"
            )
            export = task_instance.xcom_pull(
                key=f"{instance_uuid}-export", task_ids=task_id
            )
            instance = session.scalars(
                select(Instance).where(Instance.uri == message["resource_uri"])
            ).first()
            if instance is None:
                raise ValueError(f"No Instance found for {message['resource_uri']}")
            add_instance_export(
                session,
                instance,
                target_identifier=export["hrid"],
                version=_exported_version(session, instance, message["updated_at"]),
                keycloak_user_id=message["user_id"],
                keycloak_username=message["username"],
                keycloak_group=group,
            )
            logger.info(f"Recorded export of {instance.uri} as {export['hrid']}")
        session.commit()
