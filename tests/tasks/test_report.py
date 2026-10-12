import json

from ils_middleware.tasks import report as report_module
from ils_middleware.tasks.report import reports_dir, run_dir, slug, write_report


def test_slug():
    assert (
        slug("manual__2026-09-08T12:00:00+00:00") == "manual__2026-09-08T12_00_00_00_00"
    )
    assert slug("bulk_update") == "bulk_update"


def test_reports_dir_default(monkeypatch):
    monkeypatch.delenv("BLUECORE_REPORTS_DIR", raising=False)

    assert str(reports_dir()) == "/opt/airflow/reports"


def test_reports_dir_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("BLUECORE_REPORTS_DIR", str(tmp_path))

    assert reports_dir() == tmp_path


def test_run_dir_is_created(tmp_path):
    directory = run_dir("bulk_update", "manual__2026-09-08T12:00:00+00:00", tmp_path)

    assert directory.exists()
    assert directory.parent == tmp_path / "bulk_update"


def test_write_report(tmp_path):
    path = write_report(
        dag_id="bulk_update",
        run_id="manual__2026-09-08T12:00:00+00:00",
        report={"updated": ["https://bcld.info/works/1234"], "dry_run": True},
        title="Blue Core Bulk Update",
        summary={"Resources processed": 1},
        sections={
            "Skipped": [{"uri": "https://bcld.info/works/5678", "reason": "not found"}],
            "Errors": [],
        },
        base_dir=tmp_path,
    )

    html = (
        tmp_path / "bulk_update" / "manual__2026-09-08T12_00_00_00_00" / "index.html"
    ).read_text()
    assert path.endswith("index.html")
    assert "Blue Core Bulk Update" in html
    assert "Resources processed" in html
    # a section with rows becomes a table, an empty one says so
    assert "https://bcld.info/works/5678" in html
    assert "not found" in html
    assert "<h2>Errors (0)</h2>\n<p>None.</p>" in html

    payload = json.loads(
        (
            tmp_path
            / "bulk_update"
            / "manual__2026-09-08T12_00_00_00_00"
            / "report.json"
        ).read_text()
    )
    assert payload["dag_id"] == "bulk_update"
    assert payload["run_id"] == "manual__2026-09-08T12:00:00+00:00"
    assert payload["created_at"]
    assert payload["summary"] == {"Resources processed": 1}
    assert payload["updated"] == ["https://bcld.info/works/1234"]


def test_write_report_escapes_html(tmp_path):
    write_report(
        dag_id="bulk_update",
        run_id="run-1",
        report={},
        title="Blue Core Bulk Update",
        summary={},
        sections={"Errors": [{"error": "<script>alert('x')</script>"}]},
        base_dir=tmp_path,
    )

    html = (tmp_path / "bulk_update" / "run-1" / "index.html").read_text()
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_write_report_renders_a_list_cell(tmp_path):
    write_report(
        dag_id="bulk_update",
        run_id="run-1",
        report={},
        title="Blue Core Bulk Update",
        summary={},
        sections={
            "Changes": [{"uri": "https://bcld.info/works/1", "removed": ["a", "b"]}]
        },
        base_dir=tmp_path,
    )

    html = (tmp_path / "bulk_update" / "run-1" / "index.html").read_text()
    assert "a<br>b" in html


def test_write_report_links_to_the_dag_run(tmp_path, mocker):
    mocker.patch(
        "ils_middleware.tasks.report.dag_run_url",
        return_value="/workflows/dags/bulk_update/runs/run-1",
    )

    write_report(
        dag_id="bulk_update",
        run_id="run-1",
        report={},
        title="Blue Core Bulk Update",
        summary={},
        sections={},
        base_dir=tmp_path,
    )

    html = (tmp_path / "bulk_update" / "run-1" / "index.html").read_text()
    assert '<a href="/workflows/dags/bulk_update/runs/run-1">run-1</a>' in html


def test_write_report_without_a_dag_run_url(tmp_path, mocker, caplog):
    """A report is worth having even when we can't work out the run's URL."""
    mocker.patch(
        "ils_middleware.tasks.report.dag_run_url", side_effect=RuntimeError("no config")
    )

    write_report(
        dag_id="bulk_update",
        run_id="run-1",
        report={},
        title="Blue Core Bulk Update",
        summary={},
        sections={},
        base_dir=tmp_path,
    )

    html = (tmp_path / "bulk_update" / "run-1" / "index.html").read_text()
    assert "<a href" not in html
    assert "run-1" in html


def test_dag_run_url(mocker):
    conf = mocker.MagicMock()
    conf.get.return_value = "http://localhost:8080/workflows/"
    mocker.patch.dict("sys.modules")
    mocker.patch("airflow.configuration.conf", conf)

    assert (
        report_module.dag_run_url("bulk_update", "run-1")
        == "/workflows/dags/bulk_update/runs/run-1"
    )
