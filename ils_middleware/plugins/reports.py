"""Scaffolding for the Blue Core reports Airflow plugin.

Like the bulk operations plugin, this mounts a FastAPI app into the Airflow API
server and registers an `external view`_ so that a "Reports" item appears in the
Airflow navigation, rendered in an iframe at ``/plugin/reports``.

For now the only route renders a placeholder page. Building the actual reports
--- per DAG run HTML reports of the errors that ``resource_loader`` and
``archived_file_loader`` hit, an index linking to them, and emailing a link to
whoever kicked the run off --- is
https://github.com/blue-core-lod/bluecore-workflows/issues/191

.. _external view: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html#external-views
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse

from ils_middleware.plugins import shared

PLUGIN_NAME = "bluecore_reports"
TITLE = "Blue Core Reports"
NAV_LABEL = "Reports"

# Where the FastAPI app is mounted on the Airflow API server, and the path the
# Airflow UI uses for the iframe that wraps it.
URL_PREFIX = "/reports"
URL_ROUTE = "reports"

app = FastAPI(title=TITLE)
shared.mount_static(app)


@app.get(
    "/", response_class=HTMLResponse, dependencies=[Depends(shared.requires_login)]
)
def reports(request: Request) -> HTMLResponse:
    """Render the reports index."""
    return shared.TEMPLATES.TemplateResponse(
        request=request,
        name="reports.html",
        context=shared.context(URL_PREFIX, title=TITLE),
    )


class ReportsPlugin(AirflowPlugin):
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
            "icon": shared.static_url(URL_PREFIX, "report.svg"),
            "icon_dark_mode": shared.static_url(URL_PREFIX, "report-dark.svg"),
        }
    ]
