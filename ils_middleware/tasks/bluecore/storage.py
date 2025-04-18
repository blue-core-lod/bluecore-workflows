import logging

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_bluecore_db():
    pg_hook = PostgresHook("bluecore_db")
    return str(pg_hook.sqlalchemy_url)


@task.virtualenv(
    requirements=["bluecore-models"],
    system_site_packages=False,
)
def store_bluecore_resources(**kwargs):
    """Stores Work or Instance in the Blue Core Database

    Note: Virtualenv is needed because bluecore.models uses SQLAlchemy 2.+
    and Airflow uses a 1.x version of SQLAlchemy
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from bluecore_models.models import (
        Instance,
        Work,
        OtherResource,
        ResourceBase,
        BibframeOtherResources,
    )

    records = kwargs["records"]
    bluecore_db_conn_string = kwargs["bluecore_db"]

    engine = create_engine(bluecore_db_conn_string)

    with Session(engine) as session:
        for payload in records:
            match payload["class"]:
                case "Instance":
                    instance = (
                        session.query(Instance)
                        .where(Instance.uri == payload["uri"])
                        .first()
                    )
                    if not instance:
                        instance = Instance(
                            uri=payload["uri"], data=payload["resource"]
                        )
                        if "work_uri" in payload:
                            db_work = (
                                session.query(Work)
                                .where(Work.uri == payload["work_uri"])
                                .first()
                            )
                            if db_work:
                                instance.work = db_work
                        session.add(instance)

                case "OtherResource":
                    other_resource = (
                        session.query(OtherResource)
                        .where(OtherResource.uri == payload["uri"])
                        .first()
                    )
                    if not other_resource:
                        other_resource = OtherResource(
                            uri=payload["uri"], data=payload["resource"]
                        )
                        session.add(other_resource)
                    bibframe_resource = (
                        session.query(ResourceBase)
                        .where(ResourceBase.uri == payload["bibframe_resource_uri"])
                        .first()
                    )
                    bf_other_resource = BibframeOtherResources(
                        other_resource=other_resource,
                        bibframe_resource=bibframe_resource,
                    )

                    session.add(bf_other_resource)

                case "Work":
                    work = session.query(Work).where(Work.uri == payload["uri"]).first()
                    if not work:
                        work = Work(uri=payload["uri"], data=payload["resource"])
                        session.add(work)

                case _:
                    logger.error(
                        f"Unknown class {payload['class']} for {payload['uri']}"
                    )
        session.commit()
