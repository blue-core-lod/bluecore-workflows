"""
Write a DAG run's report to the reports volume.

Reports are files on a mounted volume rather than XCom values so that they
outlive the DAG run's metadata and can be served by the Blue Core reports
plugin. The layout is one directory per DAG, one directory per run inside it:

    /opt/airflow/reports/<dag_id>/<run_id>/report.json
    /opt/airflow/reports/<dag_id>/<run_id>/index.html

The JSON is the whole report as the DAG assembled it, for anything that wants
to read it back. The HTML is a plain rendering of the same thing for a person
who opens it now; the reports plugin
(https://github.com/blue-core-lod/bluecore-workflows/issues/191) is where the
presentation properly belongs, and it can style or replace this.
"""

import html
import json
import logging
import os
import pathlib
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

DEFAULT_REPORTS_DIR = "/opt/airflow/reports"

# Run ids contain characters (colons, plus signs) that make for awkward paths,
# e.g. manual__2026-09-08T12:00:00+00:00.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def reports_dir() -> pathlib.Path:
    """Where reports are written, overridable for local runs and tests."""
    return pathlib.Path(os.environ.get("BLUECORE_REPORTS_DIR", DEFAULT_REPORTS_DIR))


def slug(value: str) -> str:
    """Make a string safe to use as a single path component."""
    return UNSAFE.sub("_", value).strip("_")


def run_dir(
    dag_id: str, run_id: str, base_dir: pathlib.Path | None = None
) -> pathlib.Path:
    """The directory this run's report files belong in, created if needed."""
    directory = (base_dir or reports_dir()) / slug(dag_id) / slug(run_id)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def dag_run_url(dag_id: str, run_id: str) -> str:
    """
    The Airflow UI URL for a DAG run, so a report can link back to the run that
    produced it. Relative to whatever Airflow is served under -- the Blue Core
    stack proxies it beneath /workflows.
    """
    # Imported here so this module can be used (and tested) outside Airflow.
    from airflow.configuration import conf

    # The path only, as the plugins do: a report is opened from the same origin
    # Airflow is served from, and [api] base_url may name a host that isn't the
    # one the reader is actually using.
    base = urlsplit(conf.get("api", "base_url", fallback="") or "/").path.rstrip("/")
    return f"{base}/dags/{dag_id}/runs/{run_id}"


def write_report(
    dag_id: str,
    run_id: str,
    report: dict[str, Any],
    title: str,
    summary: dict[str, Any],
    sections: dict[str, list[dict[str, Any]]],
    base_dir: pathlib.Path | None = None,
) -> str:
    """
    Write a run's report as JSON and HTML, returning the path of the HTML.

    summary is the headline counts, rendered as a definition list. sections are
    the details, each rendered as a table of its rows' keys -- so a caller adds
    a section by putting a list of dicts in it, without touching this module.
    """
    directory = run_dir(dag_id, run_id, base_dir)
    created_at = datetime.now(UTC)

    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "created_at": created_at.isoformat(),
        "summary": summary,
        **report,
    }
    (directory / "report.json").write_text(json.dumps(payload, indent=2))

    html_path = directory / "index.html"
    html_path.write_text(_render(title, dag_id, run_id, created_at, summary, sections))

    logger.info(f"wrote report to {html_path}")
    return str(html_path)


def _render(
    title: str,
    dag_id: str,
    run_id: str,
    created_at: datetime,
    summary: dict[str, Any],
    sections: dict[str, list[dict[str, Any]]],
) -> str:
    try:
        run_link = f'<a href="{html.escape(dag_run_url(dag_id, run_id))}">{html.escape(run_id)}</a>'
    except Exception as error:  # noqa: BLE001 -- a missing base_url shouldn't cost us the report
        logger.warning(f"could not build the DAG run URL: {error}")
        run_link = html.escape(run_id)

    parts = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        f"<title>{html.escape(title)}</title>",
        "<style>",
        "body { font-family: system-ui, sans-serif; margin: 2rem; }",
        "table { border-collapse: collapse; margin-bottom: 2rem; }",
        "th, td { border: 1px solid #ccc; padding: 0.25rem 0.5rem; text-align: left; }",
        "th { background: #f4f4f4; }",
        "dt { font-weight: bold; }",
        "dd { margin: 0 0 0.5rem 1rem; }",
        "</style>",
        "</head>",
        "<body>",
        f"<h1>{html.escape(title)}</h1>",
        (
            f"<p>DAG <code>{html.escape(dag_id)}</code>, run {run_link}, "
            f"{html.escape(created_at.isoformat(timespec='seconds'))}</p>"
        ),
        "<dl>",
    ]
    for label, value in summary.items():
        parts.append(
            f"<dt>{html.escape(str(label))}</dt><dd>{html.escape(str(value))}</dd>"
        )
    parts.append("</dl>")

    for heading, rows in sections.items():
        parts.append(f"<h2>{html.escape(str(heading))} ({len(rows):,})</h2>")
        if not rows:
            parts.append("<p>None.</p>")
            continue
        parts.append(_table(rows))

    parts.extend(["</body>", "</html>"])
    return "\n".join(parts)


def _table(rows: list[dict[str, Any]]) -> str:
    """Render rows as a table, with a column for every key any row uses."""
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)

    parts = ["<table>", "<tr>"]
    parts.extend(f"<th>{html.escape(str(column))}</th>" for column in columns)
    parts.append("</tr>")
    for row in rows:
        parts.append("<tr>")
        parts.extend(f"<td>{_cell(row.get(column))}</td>" for column in columns)
        parts.append("</tr>")
    parts.append("</table>")
    return "\n".join(parts)


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "<br>".join(html.escape(str(item)) for item in value)
    return html.escape(str(value))
