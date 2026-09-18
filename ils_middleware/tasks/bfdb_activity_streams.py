import json
import pathlib
import uuid
from datetime import UTC, datetime
from urllib.parse import urlparse

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ils_middleware.tasks.bfdb.schema.schema_feed import ActivityStreamsFeed, FeedItem

BFDB_CURSOR_PREFIX = "bfdb_"
RETRY_ATTEMPTS = 3
FEED_FILE_SUFFIXES = {
    "hubs": "hub",
    "works": "work",
    "instances": "instance",
}

ACTIVITY_STREAM_FEEDS = [
    {
        "feed_name": "works",
        "feed_url": "https://id.loc.gov/resources/works/activitystreams/feed/1",
    },
    {
        "feed_name": "instances",
        "feed_url": "https://id.loc.gov/resources/instances/activitystreams/feed/1",
    },
    {
        "feed_name": "hubs",
        "feed_url": "https://id.loc.gov/resources/hubs/activitystreams/feed/1",
    },
]


def create_activity_stream_run(airflow_path: str = "/opt/airflow/uploads") -> str:
    run_id = str(uuid.uuid4())
    return run_id


def feed_run_path(airflow_path: str, run_id: str, feed_name: str) -> pathlib.Path:
    return pathlib.Path(airflow_path) / run_id


def combine_downloads(feed_downloads: list[list[str]]) -> list[str]:
    """Combine different feed types for the same LC ID into one JSON array."""
    grouped: dict[tuple[pathlib.Path, str], list[pathlib.Path]] = {}
    for downloads in feed_downloads:
        for file_path in downloads:
            path = pathlib.Path(file_path)
            grouped.setdefault((path.parent, path.stem), []).append(path)

    collected: list[str] = []
    for (parent, lc_id), paths in grouped.items():
        if len(paths) == 1:
            collected.append(str(paths[0]))
            continue
        collected.append(str(_combine_json_arrays(parent, lc_id, paths)))
    return collected


def _combine_json_arrays(
    parent: pathlib.Path, lc_id: str, paths: list[pathlib.Path]
) -> pathlib.Path:
    combined: list[object] = []
    for path in paths:
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, list):
            raise TypeError(f"{path} does not contain a JSON array")
        combined.extend(document)

    combined_path = parent / f"{lc_id}.json"
    staged_path = parent / f".{lc_id}.json.tmp"
    staged_path.write_text(json.dumps(combined), encoding="utf-8")
    staged_path.replace(combined_path)
    for path in paths:
        path.unlink()
    return combined_path


def current_run_date() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def cursor_name_for(feed_name: str) -> str:
    return f"{BFDB_CURSOR_PREFIX}{feed_name}"


def get_last_cursor(bluecore_db: str, cursor_name: str) -> str | None:
    """Return the most recent cursor recorded for the given cursor name."""
    engine = create_engine(bluecore_db)
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    'SELECT max("cursor") FROM activity_streams_cursor '
                    "WHERE cursor_name = :cursor_name"
                ),
                {"cursor_name": cursor_name},
            )
            return result.scalar()
    finally:
        engine.dispose()


@retry(
    retry=retry_if_exception_type(OperationalError),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
def save_cursor(bluecore_db: str, cursor_name: str, cursor: str) -> None:
    engine = create_engine(bluecore_db)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    'INSERT INTO activity_streams_cursor (cursor_name, "cursor") '
                    "VALUES (:cursor_name, :cursor) ON CONFLICT (cursor_name) "
                    'DO UPDATE SET "cursor" = excluded."cursor"'
                ),
                {"cursor_name": cursor_name, "cursor": cursor},
            )
    finally:
        engine.dispose()


def process_activity_stream_feed(
    bluecore_db: str,
    url: str,
    feed_name: str,
    current_date: str,
    run_id: str | None = None,
    airflow_path: str = "/opt/airflow",
) -> list[str]:
    """Download a feed's new objects and advance that feed's cursor."""
    run_id = run_id or create_activity_stream_run(airflow_path)
    cursor_name = cursor_name_for(feed_name)
    cursor = get_last_cursor(bluecore_db, cursor_name)
    downloaded_files, newest_published = ingest_activity_stream_feed(
        url, run_id, feed_name, cursor, current_date, airflow_path
    )
    if newest_published is not None:
        save_cursor(bluecore_db, cursor_name, newest_published)
    return downloaded_files


def ingest_activity_stream_feed(
    url: str,
    run_id: str,
    feed_name: str,
    cursor: str | None,
    current_date: str,
    airflow_path: str = "/opt/airflow",
) -> tuple[list[str], str | None]:
    """Download feed objects published after the cursor and on or before the run date."""
    # Without a cursor there is no stop condition, so paging would walk the whole feed.
    if not cursor:
        raise ValueError(
            f"No cursor found for {cursor_name_for(feed_name)}; "
            "seed activity_streams_cursor before running this feed"
        )

    run_path = feed_run_path(airflow_path, run_id, feed_name)
    run_path.mkdir(parents=True, exist_ok=True)

    used_file_names: set[str] = set()
    downloaded_files: list[str] = []
    newest_published: str | None = None
    next_url: str | None = url
    visited_urls: set[str] = set()

    while next_url and next_url not in visited_urls:
        visited_urls.add(next_url)
        feed = _fetch_feed(next_url)
        reached_cursor = False

        for item in feed.orderedItems:
            # published is ISO-8601, so its leading date compares correctly as a string.
            published_date = item.published[:10]
            # LC posts future-dated entries at the head of the feed, so skip past them.
            if published_date > current_date:
                continue
            # Entries are newest first, so the first one at or before the cursor ends the feed.
            if published_date <= cursor:
                reached_cursor = True
                break
            file_path = _download_feed_item(item, feed_name, run_path, used_file_names)
            downloaded_files.append(str(file_path))
            if newest_published is None or published_date > newest_published:
                newest_published = published_date

        if reached_cursor:
            break
        next_url = feed.next

    return downloaded_files, newest_published


# def _fetch_feed(url: str) -> ActivityStreamsFeed:
#     response = httpx.get(url + ".json")
#     response.raise_for_status()
#     return ActivityStreamsFeed.model_validate(response.json())


def _local(url: str) -> str:
    return url.replace("https://", "http://").replace(
        "id.loc.gov/resources", "nginx/bfdb"
    )


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
def _fetch_feed(url: str) -> ActivityStreamsFeed:
    response = httpx.get(_local(url) + ".json")
    response.raise_for_status()
    return ActivityStreamsFeed.model_validate(response.json())


# def _download_feed_item(
#     item: FeedItem, run_path: pathlib.Path, used_file_names: set[str]
# ) -> pathlib.Path:
#     response = httpx.get(item.object.id.replace("http://", "https://") + ".json")
#     response.raise_for_status()
#     file_path = run_path / _local_file_name(item.object.id, used_file_names)
#     file_path.write_bytes(response.content)
#     return file_path


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
def _download_feed_item(
    item: FeedItem, feed_name: str, run_path: pathlib.Path, used_file_names: set[str]
) -> pathlib.Path:
    response = httpx.get(_local(item.object.id) + ".bibframe.json")
    response.raise_for_status()
    file_path = run_path / _local_file_name(
        item.object.id, FEED_FILE_SUFFIXES[feed_name], used_file_names
    )
    file_path.write_bytes(response.content)
    return file_path


def _local_file_name(url: str, suffix: str, used_file_names: set[str]) -> str:
    parsed_path = pathlib.PurePosixPath(urlparse(url).path)
    stem = pathlib.PurePosixPath(parsed_path.name or str(uuid.uuid4())).stem
    file_name = f"{stem}.{suffix}"

    if file_name not in used_file_names:
        used_file_names.add(file_name)
        return file_name

    index = 2
    while f"{stem}-{index}.{suffix}" in used_file_names:
        index += 1
    unique_file_name = f"{stem}-{index}.{suffix}"
    used_file_names.add(unique_file_name)
    return unique_file_name
