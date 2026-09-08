"""Scaffolding for the Blue Core bulk operations Airflow plugin.

The plugin registers two things with Airflow:

1. A FastAPI application that is mounted into the Airflow API server, so its
   routes sit alongside the Airflow REST API and share its authentication.
2. An `external view`_ that puts a "Blue Core Bulk Operations" item in the
   Airflow navigation. Because the view supplies a ``url_route`` the Airflow UI
   renders ``href`` in an iframe at ``/plugin/bulk-operations`` instead of
   sending the user off site.

For now the only route renders a placeholder page. The bulk operations UI and
the API calls behind it are built out in:

- https://github.com/blue-core-lod/bluecore-workflows/issues/118
- https://github.com/blue-core-lod/graph-toolbox/issues/33
- https://github.com/blue-core-lod/graph-toolbox/issues/23

.. _external view: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html#external-views
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse

from ils_middleware.plugins import shared

PLUGIN_NAME = "bluecore_bulk_operations"
TITLE = "Blue Core Bulk Operations"

# The Airflow nav is narrow enough that anything longer gets truncated with an
# ellipsis, so the nav item is labelled separately from the page title.
NAV_LABEL = "Bulk"

# Where the FastAPI app is mounted on the Airflow API server, and the path the
# Airflow UI uses for the iframe that wraps it.
URL_PREFIX = "/bulk-operations"
URL_ROUTE = "bulk-operations"


app = FastAPI(title=TITLE)
shared.mount_static(app)


@app.get(
    "/", response_class=HTMLResponse, dependencies=[Depends(shared.requires_login)]
)
def bulk_operations(request: Request) -> HTMLResponse:
    """Render the bulk operations page."""
    return shared.TEMPLATES.TemplateResponse(
        request=request,
        name="bulk_operations.html",
        context=shared.context(URL_PREFIX, title=TITLE),
    )


class BulkOperationsPlugin(AirflowPlugin):
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
            "icon": shared.static_url(URL_PREFIX, "layers.svg"),
            "icon_dark_mode": shared.static_url(URL_PREFIX, "layers-dark.svg"),
        }
    ]
