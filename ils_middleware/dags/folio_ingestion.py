"""Adds a FOLIO Inventory Instance to Blue Core as a BIBFRAME Work and Instance.

Triggered from the FOLIO ingest plugin page with the Instance's HRID and the
Keycloak id of the user who submitted it, whose group (stanford, cornell)
decides which FOLIO the Instance is retrieved from.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from ils_middleware.tasks.folio.bibframe import (
    folio_institution,
    generate_bibframe,
    ingest_into_bluecore,
    retrieve_folio_instance,
    update_source_uri,
)


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 16),
    catchup=False,
    max_active_runs=2,
    tags=["ingest", "folio"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "hrid": Param(type="string", description="FOLIO Instance HRID"),
        "user_uid": Param(
            None, type=["null", "string"], description="Keycloak user id"
        ),
    },
)
def folio_instance_to_bluecore():
    user_uid = "{{ params.user_uid or '' }}"
    # The user's Keycloak group picks the FOLIO Connection, e.g. stanford_folio
    institution = folio_institution(user_uid)
    folio_instance = retrieve_folio_instance("{{ params.hrid }}", institution)
    bibframe = generate_bibframe(folio_instance, institution)
    bf_instance_uri = ingest_into_bluecore(bibframe, user_uid=user_uid)
    update_source_uri(folio_instance, bf_instance_uri, institution)


folio_instance_to_bluecore_dag = folio_instance_to_bluecore()
