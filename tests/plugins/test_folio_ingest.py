from types import SimpleNamespace

import pytest
from airflow.api_fastapi.core_api.security import get_user
from fastapi.testclient import TestClient

from ils_middleware.plugins import folio_ingest, shared
from ils_middleware.plugins.folio_ingest import (
    FOLIO_TO_BLUECORE_DAG_ID,
    NAV_LABEL,
    TITLE,
    URL_PREFIX,
    URL_ROUTE,
    FolioIngestPlugin,
    app,
)


class User:
    def get_id(self):
        return "abc-123"

    def get_name(self):
        return "cataloger"


@pytest.fixture
def client():
    app.dependency_overrides[shared.requires_login] = lambda: None
    app.dependency_overrides[folio_ingest.requires_trigger_access] = lambda: None
    app.dependency_overrides[get_user] = User
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def mock_trigger_dag(mocker):
    return mocker.patch.object(
        folio_ingest,
        "trigger_dag",
        return_value=SimpleNamespace(run_id="manual__1"),
    )


def test_plugin_registers_fastapi_app():
    assert FolioIngestPlugin.name == "bluecore_folio_ingest"
    assert FolioIngestPlugin.fastapi_apps == [
        {"app": app, "url_prefix": URL_PREFIX, "name": TITLE}
    ]


def test_plugin_registers_external_view():
    (external_view,) = FolioIngestPlugin.external_views

    assert external_view["name"] == NAV_LABEL
    assert external_view["destination"] == "nav"
    assert external_view["url_route"] == URL_ROUTE
    assert external_view["href"].endswith(f"{URL_PREFIX}/")
    assert external_view["icon"] == shared.static_url(URL_PREFIX, "folio.svg")
    assert external_view["icon_dark_mode"] == shared.static_url(
        URL_PREFIX, "folio-dark.svg"
    )


def test_home_asks_for_hrid(client):
    response = client.get("/")

    assert response.status_code == 200
    assert f"<h1>{TITLE}</h1>" in response.text
    assert 'name="hrid"' in response.text
    assert shared.static_url(URL_PREFIX, "bluecore.css") in response.text


def test_home_shows_message(client):
    response = client.get("/", params={"message": "Hello from Blue Core"})

    assert "Hello from Blue Core" in response.text


def test_home_requires_login():
    assert TestClient(app).get("/").status_code == 401


def test_create_requires_login(mock_trigger_dag):
    response = TestClient(app).post("/create", data={"hrid": "a123"})

    assert response.status_code == 401
    mock_trigger_dag.assert_not_called()


def test_create_requires_hrid(client, mock_trigger_dag):
    response = client.post("/create", data={"hrid": "  "})

    assert response.status_code == 200
    assert "Required: HRID" in response.text
    mock_trigger_dag.assert_not_called()


def test_create_trigger_error(client, mock_trigger_dag):
    mock_trigger_dag.side_effect = Exception("Dag id not found")

    response = client.post("/create", data={"hrid": "a123"})

    assert (
        f"Error starting {FOLIO_TO_BLUECORE_DAG_ID} for a123: Dag id not found"
        in response.text
    )


def test_create_triggers_dag(client, mock_trigger_dag):
    response = client.post("/create", data={"hrid": "  a123\n"})

    assert response.status_code == 200
    assert f"Started {FOLIO_TO_BLUECORE_DAG_ID} run manual__1 for a123" in response.text

    mock_trigger_dag.assert_called_once()
    assert mock_trigger_dag.call_args.args == (FOLIO_TO_BLUECORE_DAG_ID,)
    kwargs = mock_trigger_dag.call_args.kwargs
    assert kwargs["triggering_user_name"] == "cataloger"
    assert kwargs["conf"] == {"hrid": "a123", "user_uid": "abc-123"}
