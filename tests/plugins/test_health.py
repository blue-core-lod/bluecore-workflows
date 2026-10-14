from importlib.metadata import PackageNotFoundError

from airflow import __version__ as airflow_version
from fastapi.testclient import TestClient

from ils_middleware.plugins import health
from ils_middleware.plugins.health import TITLE, URL_PREFIX, HealthPlugin, app


def test_plugin_registers_fastapi_app():
    assert HealthPlugin.name == "bluecore_health"
    assert HealthPlugin.fastapi_apps == [
        {"app": app, "url_prefix": URL_PREFIX, "name": TITLE}
    ]


def test_health_reports_versions(mocker):
    mocker.patch.object(health, "version", return_value="1.4.5")

    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "version": "1.4.5",
        "airflow": airflow_version,
    }


def test_health_needs_no_login():
    assert TestClient(app).get("/").status_code == 200


def test_health_without_the_package_installed(mocker):
    mocker.patch.object(
        health, "version", side_effect=PackageNotFoundError("bluecore-workflows")
    )

    assert TestClient(app).get("/").json()["version"] is None
