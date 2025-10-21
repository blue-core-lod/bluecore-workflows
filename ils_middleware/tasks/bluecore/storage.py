import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def get_bluecore_db() -> str:
    pg_hook = PostgresHook("bluecore_db")
    return str(pg_hook.sqlalchemy_url)
