"""Version endpoint for bluecore-workflows.

The API, Sinopia and Marva each answer a health URL with ``{"version": ...}``,
which the stack's landing page and the dev dashboard read. This plugin gives
Workflows the same: a FastAPI app on the Airflow API server whose only route
reports the installed bluecore-workflows version and the Airflow version. The
stack's nginx serves it as ``/workflows/health``.

Unlike the other plugins it needs no login and registers no external view: it
reports only version numbers, and is meant for tools rather than people.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from airflow import __version__ as airflow_version
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI

PLUGIN_NAME = "bluecore_health"
TITLE = "Blue Core Workflows health"

# Where the FastAPI app is mounted on the Airflow API server
URL_PREFIX = "/bluecore-health"

app = FastAPI(title=TITLE)


def workflows_version() -> str | None:
    """The bluecore-workflows version installed in this image."""
    try:
        return version("bluecore-workflows")
    except PackageNotFoundError:
        return None


@app.get("/")
def health() -> dict:
    """Report the running versions."""
    return {"status": "ok", "version": workflows_version(), "airflow": airflow_version}


class HealthPlugin(AirflowPlugin):
    name = PLUGIN_NAME

    fastapi_apps = [  # noqa: RUF012
        {
            "app": app,
            "url_prefix": URL_PREFIX,
            "name": TITLE,
        }
    ]
