import pytest
from fastapi.testclient import TestClient

from ils_middleware.plugins import shared
from ils_middleware.plugins.reports import (
    NAV_LABEL,
    TITLE,
    URL_PREFIX,
    URL_ROUTE,
    ReportsPlugin,
    app,
)


@pytest.fixture
def client():
    app.dependency_overrides[shared.requires_login] = lambda: None
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_plugin_registers_fastapi_app():
    assert ReportsPlugin.name == "bluecore_reports"
    assert ReportsPlugin.fastapi_apps == [
        {"app": app, "url_prefix": URL_PREFIX, "name": TITLE}
    ]


def test_plugin_registers_external_view():
    (external_view,) = ReportsPlugin.external_views

    assert external_view["name"] == NAV_LABEL
    assert external_view["destination"] == "nav"
    assert external_view["url_route"] == URL_ROUTE
    assert external_view["href"].endswith(f"{URL_PREFIX}/")
    assert external_view["icon"] == shared.static_url(URL_PREFIX, "report.svg")
    assert external_view["icon_dark_mode"] == shared.static_url(
        URL_PREFIX, "report-dark.svg"
    )


def test_reports_page(client):
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert f"<h1>{TITLE}</h1>" in response.text


def test_reports_page_uses_the_shared_chrome(client):
    response = client.get("/")

    # Inherited from base.html, with URLs resolved for this plugin's prefix.
    assert shared.static_url(URL_PREFIX, "bluecore.css") in response.text
    assert shared.static_url(URL_PREFIX, "bluecore.js") in response.text


def test_reports_page_requires_login():
    response = TestClient(app).get("/")

    assert response.status_code == 401
