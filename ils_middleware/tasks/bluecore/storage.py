import logging

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def get_bluecore_db():
    pg_hook = PostgresHook("bluecore_db")
    return str(pg_hook.sqlalchemy_url)


@task.virtualenv(
    requirements=["blue-core-data-models"],
    system_site_packages=False,
)
def store_bluecore_resources(**kwargs):
    """Stores Work or Instance in the Blue Core Database
    
    Note: Virtualenv is needed because bluecore.models uses SQLAlchemy 2.+ 
    and Airflow uses a 1.x version of SQLAlchemy
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from bluecore.models import Instance, Work

    records = kwargs['records']
    bluecore_db_conn_string = kwargs["bluecore_db"]

    engine = create_engine(bluecore_db_conn_string)

    with Session(engine) as session:
        for payload in records:
            match payload["class"]:

                case "Instance":
                    instance = Instance(
                        uri=payload["uri"],
                        data=payload["resource"]
                    )
                    if "work_uri" in payload:
                        db_work = session.query(Work).where(Work.uri == payload["work_uri"]).first()
                        if db_work:
                            instance.work = db_work
                    session.add(instance)

                case "Work":             
                    work = Work(
                        uri=payload["uri"],
                        data=payload["resource"]
                    )
                    session.add(work)

                case _:
                    logger.error(f"Unknown class {payload['class']} for {payload['uri']}")
        session.commit()
