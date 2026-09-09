import pytest
from airflow.configuration import conf
from fastapi import FastAPI
from fastapi.testclient import TestClient

from ils_middleware.plugins import shared


@pytest.mark.parametrize(
    "base_url,expected",
    [
        ("http://localhost:8080/", ""),
        # the bluecore-stack proxies Airflow under /workflows
        ("http://localhost/workflows", "/workflows"),
        ("", ""),
    ],
)
def test_airflow_root_path(monkeypatch, base_url, expected):
    monkeypatch.setattr(conf, "get", lambda *args, **kwargs: base_url)

    assert shared.airflow_root_path() == expected


def test_static_url_includes_the_plugin_prefix(monkeypatch):
    monkeypatch.setattr(
        conf, "get", lambda *args, **kwargs: "http://localhost/workflows"
    )

    assert (
        shared.static_url("/bulk-operations", "bluecore.css")
        == "/workflows/bulk-operations/static/bluecore.css"
    )


def test_context_gives_templates_a_static_url_helper():
    context = shared.context("/bulk-operations", title="Some Title")

    assert context["title"] == "Some Title"
    assert context["static_url"]("bluecore.css").endswith(
        "/bulk-operations/static/bluecore.css"
    )


@pytest.mark.parametrize(
    "filename",
    [
        "bluecore.css",
        "bluecore.js",
        "layers.svg",
        "layers-dark.svg",
        "report.svg",
        "report-dark.svg",
    ],
)
def test_shared_static_files_are_served(filename):
    # Every plugin serves the shared static directory under its own prefix.
    app = FastAPI()
    shared.mount_static(app)

    # No auth override: these are decorative assets served unauthenticated.
    response = TestClient(app).get(f"/static/{filename}")

    assert response.status_code == 200
