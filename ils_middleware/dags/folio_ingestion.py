"""Adds a FOLIO Inventory Instance to Blue Core as a BIBFRAME Work and Instance.

Triggered from the FOLIO ingest plugin page with the Instance's HRID and the
Keycloak id of the user who submitted it, whose group (stanford, cornell)
decides which FOLIO the Instance is retrieved from.

The Work and Instance come from one of two branches, chosen by the use_marc
param: mapping the Inventory JSON (bibframe.py), or transforming the MARC
record from Source Record Storage with marc2bibframe (marc.py). Both merge
at ingest_into_bluecore, and then update_source_uri.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task

from ils_middleware.tasks.folio.bibframe import (
    folio_institution,
    generate_bibframe,
    ingest_into_bluecore,
    retrieve_folio_instance,
    update_source_uri,
)
from ils_middleware.tasks.folio.marc import marc_to_bibframe, retrieve_marc_record


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
        "use_marc": Param(
            False,
            type="boolean",
            description="Transform the MARC record with marc2bibframe instead "
            "of mapping the Inventory JSON",
        ),
    },
)
def folio_instance_to_bluecore():
    @task.branch
    def choose_mapping() -> str:
        # Read from params, not a template, which would render as a string
        params = get_current_context()["params"]
        if params.get("use_marc"):
            return "retrieve_marc_record"
        return "generate_bibframe"

    user_uid = "{{ params.user_uid or '' }}"
    # The user's Keycloak group picks the FOLIO Connection, e.g. stanford_folio
    institution = folio_institution(user_uid)
    folio_instance = retrieve_folio_instance("{{ params.hrid }}", institution)
    mapping = choose_mapping()

    # Inventory JSON branch
    bibframe = generate_bibframe(folio_instance, institution)

    # MARC branch
    marc_xml = retrieve_marc_record(folio_instance, institution)
    bibframe_rdf_xml = marc_to_bibframe(marc_xml, institution)

    folio_instance >> mapping >> [bibframe, marc_xml]

    # The branches merge here; the skipped one's output arrives as None
    bf_instance_uri = ingest_into_bluecore(
        bibframe, user_uid=user_uid, bibframe_rdf_xml=bibframe_rdf_xml
    )
    update_source_uri(folio_instance, bf_instance_uri, institution)


folio_instance_to_bluecore_dag = folio_instance_to_bluecore()
