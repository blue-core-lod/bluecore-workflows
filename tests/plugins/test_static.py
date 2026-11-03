import pytest
from fastapi.testclient import TestClient

from ils_middleware.plugins import bulk_operations, reports


@pytest.mark.parametrize(
    "app,path",
    [
        # Shared styles and script, from bluecore-airflow-common
        (bulk_operations.app, "/static/bluecore/bluecore.css"),
        (bulk_operations.app, "/static/bluecore/bluecore.js"),
        (reports.app, "/static/bluecore/bluecore.css"),
        (reports.app, "/static/bluecore/bluecore.js"),
        # Each plugin's own nav icons
        (bulk_operations.app, "/static/layers.svg"),
        (bulk_operations.app, "/static/layers-dark.svg"),
        (reports.app, "/static/report.svg"),
        (reports.app, "/static/report-dark.svg"),
    ],
)
def test_static_files_are_served(app, path):
    # No auth override: these are decorative assets served unauthenticated.
    assert TestClient(app).get(path).status_code == 200
