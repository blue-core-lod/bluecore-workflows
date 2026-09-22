"""Publish Blue Core activity streams as static nginx files."""

import os
from datetime import datetime

from airflow.sdk import dag, task

from ils_middleware.tasks.bcdb_activity_streams import generate_feeds
from ils_middleware.tasks.bluecore import get_bluecore_db


# scheduled 2 hours after bfdb dag starts
@dag(
    schedule="0 2 * * *",
    start_date=datetime(2026, 9, 15),
    catchup=False,
    tags=["bcdb", "activity-streams"],
    default_args={"owner": "airflow"},
)
def publish_bluecore_activity_streams():
    @task
    def publish() -> int:
        return generate_feeds(
            get_bluecore_db(),
            output_directory=os.environ.get(
                "BLUECORE_ACTIVITY_STREAMS_DIR", "/opt/airflow/bcdb"
            ),
            base_url=os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info"),
            batch_size=10_000,
        )

    publish()


publish_bluecore_activity_streams_dag = publish_bluecore_activity_streams()
