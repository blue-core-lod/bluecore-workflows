"""Blue Core FOLIO ingest Airflow plugin.

Like the other plugins, this mounts a FastAPI app into the Airflow API server and
registers an `external view`_ so that a "FOLIO" item appears in the Airflow
navigation, rendered in an iframe at ``/plugin/folio-ingest``. The page takes a
FOLIO Instance HRID and triggers the ``folio_instance_to_bluecore`` DAG for it.

The DAG run is created in process with the same function Airflow's own
``POST /api/v2/dags/{dag_id}/dagRuns`` route uses, behind the same permission
check, so no API client or service account token is needed and the run is
attributed to the signed in user.

There is no CSRF token: Airflow's ``_token`` cookie is ``SameSite=Lax``, so a
cross-site form POST arrives unauthenticated and is rejected.

.. _external view: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html#external-views
"""

from __future__ import annotations

import logging
from urllib.parse import urlencode

from airflow.api.common.trigger_dag import trigger_dag
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagAccessEntity,
)
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.types import DagRunTriggeredByType
from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ils_middleware.plugins import shared

logger = logging.getLogger(__name__)

PLUGIN_NAME = "bluecore_folio_ingest"
TITLE = "FOLIO Inventory to Blue Core"
NAV_LABEL = "FOLIO"

FOLIO_TO_BLUECORE_DAG_ID = "folio_instance_to_bluecore"

# Where the FastAPI app is mounted on the Airflow API server, and the path the
# Airflow UI uses for the iframe that wraps it.
URL_PREFIX = "/folio-ingest"
URL_ROUTE = "folio-ingest"

app = FastAPI(title=TITLE)
shared.mount_static(app)

requires_trigger_access = requires_access_dag(
    "POST", DagAccessEntity.RUN, param_dag_id=FOLIO_TO_BLUECORE_DAG_ID
)


def _redirect_home(request: Request, message: str) -> RedirectResponse:
    # Path only, so the redirect keeps the browser's scheme and host behind a proxy
    home_url = request.url_for("folio_ingest").path
    return RedirectResponse(
        f"{home_url}?{urlencode({'message': message})}", status_code=303
    )


@app.get(
    "/", response_class=HTMLResponse, dependencies=[Depends(shared.requires_login)]
)
def folio_ingest(request: Request) -> HTMLResponse:
    """Render the HRID form."""
    return shared.TEMPLATES.TemplateResponse(
        request=request,
        name="folio_ingest.html",
        context=shared.context(
            URL_PREFIX, title=TITLE, message=request.query_params.get("message")
        ),
    )


@app.post("/create", dependencies=[Depends(requires_trigger_access)])
def create(
    request: Request,
    user: GetUserDep,
    hrid: str = Form(default=""),
) -> RedirectResponse:
    """
    Triggers the DAG that creates a BIBFRAME Work and Instance in Blue Core
    from the FOLIO Inventory record with this HRID
    """
    hrid = hrid.strip()
    if not hrid:
        return _redirect_home(request, "Required: HRID")
    try:
        dag_run = trigger_dag(
            FOLIO_TO_BLUECORE_DAG_ID,
            triggered_by=DagRunTriggeredByType.UI,
            triggering_user_name=user.get_name(),
            conf={"hrid": hrid, "user_uid": user.get_id()},
        )
    except Exception as e:  # noqa: BLE001 -- shown to the user rather than a 500
        logger.error(f"Failed to trigger {FOLIO_TO_BLUECORE_DAG_ID}: {e}")
        return _redirect_home(
            request, f"Error starting {FOLIO_TO_BLUECORE_DAG_ID} for {hrid}: {e}"
        )
    if dag_run is None:
        return _redirect_home(
            request, f"Error starting {FOLIO_TO_BLUECORE_DAG_ID} for {hrid}"
        )
    return _redirect_home(
        request,
        f"Started {FOLIO_TO_BLUECORE_DAG_ID} run {dag_run.run_id} for {hrid}",
    )


class FolioIngestPlugin(AirflowPlugin):
    name = PLUGIN_NAME

    fastapi_apps = [  # noqa: RUF012
        {
            "app": app,
            "url_prefix": URL_PREFIX,
            "name": TITLE,
        }
    ]

    external_views = [  # noqa: RUF012
        {
            "name": NAV_LABEL,
            "href": f"{shared.airflow_root_path()}{URL_PREFIX}/",
            "url_route": URL_ROUTE,
            "destination": "nav",
            # Rendered by the Airflow UI in an <img>, so the icon cannot pick up
            # the nav's text color and each color mode needs its own file.
            "icon": shared.static_url(URL_PREFIX, "folio.svg"),
            "icon_dark_mode": shared.static_url(URL_PREFIX, "folio-dark.svg"),
        }
    ]
