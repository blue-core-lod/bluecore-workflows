import pytest
from fastapi.testclient import TestClient

from ils_middleware.plugins import shared
from ils_middleware.plugins.bulk_operations import (
    NAV_LABEL,
    TITLE,
    URL_PREFIX,
    URL_ROUTE,
    BulkOperationsPlugin,
    app,
)


@pytest.fixture
def client():
    app.dependency_overrides[shared.requires_login] = lambda: None
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_plugin_registers_fastapi_app():
    assert BulkOperationsPlugin.name == "bluecore_bulk_operations"
    assert BulkOperationsPlugin.fastapi_apps == [
        {"app": app, "url_prefix": URL_PREFIX, "name": TITLE}
    ]


def test_plugin_registers_external_view():
    (external_view,) = BulkOperationsPlugin.external_views

    assert external_view["name"] == NAV_LABEL
    assert external_view["destination"] == "nav"
    # url_route makes the Airflow UI render href in an iframe rather than
    # linking away from Airflow
    assert external_view["url_route"] == URL_ROUTE
    assert external_view["href"].endswith(f"{URL_PREFIX}/")


def test_external_view_has_an_icon_per_color_mode():
    (external_view,) = BulkOperationsPlugin.external_views

    # The UI renders these in an <img>, which cannot inherit the nav's text
    # color, so a light and a dark variant are both needed.
    assert external_view["icon"] == shared.static_url(URL_PREFIX, "layers.svg")
    assert external_view["icon_dark_mode"] == shared.static_url(
        URL_PREFIX, "layers-dark.svg"
    )


def test_bulk_operations_page(client):
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert f"<h1>{TITLE}</h1>" in response.text


def test_bulk_operations_page_uses_the_shared_chrome(client):
    response = client.get("/")

    # Inherited from base.html, with URLs resolved for this plugin's prefix.
    assert shared.static_url(URL_PREFIX, "bluecore.css") in response.text
    assert shared.static_url(URL_PREFIX, "bluecore.js") in response.text
    assert 'class="centered"' in response.text


def test_bulk_operations_page_requires_login():
    response = TestClient(app).get("/")

    assert response.status_code == 401
