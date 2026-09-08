"""Shared support for Blue Core Airflow plugins.

Airflow gives each plugin its own FastAPI app mounted at its own ``url_prefix``,
so there is no single origin the plugins can hang assets off. Every plugin
therefore serves this one shared ``static`` directory under its own prefix and
builds its own URLs into it. Templates are shared outright: ``base.html`` holds
the page chrome and each plugin fills in ``{% block content %}``.

Airflow 3.2 groups every ``destination: "nav"`` external view into one "Plugins"
submenu, with no way to promote an item out of it. Airflow 3.3 adds a
``nav_top_level`` key that renders an item directly on the toolbar instead, so
add that to each plugin's external view once this repo moves off 3.2.
"""

from __future__ import annotations

import functools
import pathlib
from typing import Any
from urllib.parse import urlsplit

from airflow.api_fastapi.core_api.security import requires_authenticated
from airflow.configuration import conf
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

PLUGINS_DIR = pathlib.Path(__file__).parent
STATIC_DIR = PLUGINS_DIR / "static"
STATIC_ROUTE = "/static"

TEMPLATES = Jinja2Templates(directory=str(PLUGINS_DIR / "templates"))

# Authentication is delegated to Airflow: the iframe request carries Airflow's
# JWT cookie, which this resolves through the configured auth manager (Keycloak
# in a deployed Blue Core stack).
requires_login = requires_authenticated()


def airflow_root_path() -> str:
    """Return the path Airflow is served under, without a trailing slash.

    Airflow is at the root in local development, but the bluecore-stack proxies
    it under ``/workflows``, so links into a plugin have to be prefixed with
    whatever ``[api] base_url`` says.
    """
    return urlsplit(conf.get("api", "base_url", fallback="") or "/").path.rstrip("/")


def mount_static(app: FastAPI) -> None:
    """Serve the shared static directory under a plugin's own ``url_prefix``.

    Left unauthenticated: these are decorative assets that the browser requests
    as plain images and stylesheets.
    """
    app.mount(STATIC_ROUTE, StaticFiles(directory=STATIC_DIR), name="static")


def static_url(url_prefix: str, filename: str) -> str:
    """Return the URL a browser should use to fetch a shared static file."""
    return f"{airflow_root_path()}{url_prefix}{STATIC_ROUTE}/{filename}"


def context(url_prefix: str, **extra: Any) -> dict[str, Any]:
    """Build a template context, adding what every plugin page needs.

    Templates get a ``static_url`` function rather than a fixed path, so they
    keep working for pages nested under a plugin's prefix, where a relative URL
    would resolve against the wrong directory.
    """
    return {"static_url": functools.partial(static_url, url_prefix), **extra}
