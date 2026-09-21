import json
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError

from ils_middleware.tasks import bcdb_activity_streams as feeds
from ils_middleware.tasks import bfdb_activity_streams


def version_entry(index: int, *, is_first: bool = False) -> feeds.VersionEntry:
    return feeds.VersionEntry(
        id=index,
        uri=f"https://bcld.info/works/{index}",
        data={"@type": ["bf:Text"]},
        actor="cataloger",
        created_at=datetime(2026, 9, 15, tzinfo=UTC) - timedelta(minutes=index),
        is_first=is_first,
    )


def test_new_versions_uses_correlated_not_exists(monkeypatch):
    statements = []

    class FakeSession:
        def __init__(self, engine):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

        def execute(self, statement):
            statements.append(statement)
            return []

    monkeypatch.setattr(feeds, "get_engine", lambda bluecore_db: object())
    monkeypatch.setattr(feeds, "Session", FakeSession)

    assert feeds.new_versions("postgresql://bluecore", 42) == {
        "hubs": [],
        "works": [],
        "instances": [],
    }
    sql = str(
        statements[0].compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    ).upper()
    assert "NOT (EXISTS" in sql
    assert "GROUP BY" not in sql
    assert "VERSIONS.ID > 42" in sql


def test_new_versions_applies_batch_limit(monkeypatch):
    statements = []

    class FakeSession:
        def __init__(self, engine):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            pass

        def execute(self, statement):
            statements.append(statement)
            return []

    monkeypatch.setattr(feeds, "get_engine", lambda bluecore_db: object())
    monkeypatch.setattr(feeds, "Session", FakeSession)
    feeds.new_versions("postgresql://bluecore", 0, batch_size=50)

    sql = str(
        statements[0].compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    ).upper()
    assert "LIMIT 50" in sql


def test_stage_feed_limits_pages_and_links_them(tmp_path):
    staging = tmp_path / "staging"
    paths, position = feeds.stage_feed(
        [version_entry(index, is_first=index == 0) for index in range(101)],
        "works",
        feeds.FeedPosition(),
        tmp_path / "published",
        staging,
        "https://example.test",
    )

    first = feeds.load_page(paths[0])
    second = feeds.load_page(paths[1])
    assert len(first.orderedItems) == 100
    assert len(second.orderedItems) == 1
    assert position == feeds.FeedPosition(page=2, count=1)
    assert first.next == "https://example.test/works/activitystreams/page/2"
    assert second.prev == "https://example.test/works/activitystreams/page/1"
    assert first.orderedItems[0].type == "Add"
    assert first.orderedItems[1].type == "Update"
    assert first.orderedItems[0].object.type == ["bf:Work"]


def test_build_collection_describes_first_last_and_total_items():
    collection = feeds.build_collection(
        "works", feeds.FeedPosition(page=3, count=27), "http://localhost"
    )

    assert collection.id == "http://localhost/works/activitystreams/feed"
    assert collection.first.id == "http://localhost/works/activitystreams/page/1"
    assert collection.last.id == "http://localhost/works/activitystreams/page/3"
    assert collection.totalItems == 227
    assert collection.context == [
        "https://www.w3.org/ns/activitystreams",
        "https://emm-spec.org/1.0/context.json",
    ]


def test_page_path_uses_two_level_shards(tmp_path):
    feed_directory = tmp_path / "works"

    assert feeds.page_path(feed_directory, 1) == feed_directory / "000/000/1.json"
    assert feeds.page_path(feed_directory, 123) == feed_directory / "000/000/123.json"
    assert (
        feeds.page_path(feed_directory, 1_000) == feed_directory / "000/001/1000.json"
    )
    assert (
        feeds.page_path(feed_directory, 1_000_000)
        == feed_directory / "001/000/1000000.json"
    )

    got = feeds.page_path(feed_directory, 1)
    assert got == feed_directory / "000/000/1.json"
    got = feeds.page_path(feed_directory, 12)
    assert got == feed_directory / "000/000/12.json"
    got = feeds.page_path(feed_directory, 123)
    assert got == feed_directory / "000/000/123.json"
    got = feeds.page_path(feed_directory, 1234)
    assert got == feed_directory / "000/001/1234.json"
    got = feeds.page_path(feed_directory, 12345)
    assert got == feed_directory / "000/012/12345.json"
    got = feeds.page_path(feed_directory, 123456)
    assert got == feed_directory / "000/123/123456.json"
    got = feeds.page_path(feed_directory, 1234567)
    assert got == feed_directory / "001/234/1234567.json"
    got = feeds.page_path(feed_directory, 12345678)
    assert got == feed_directory / "012/345/12345678.json"
    got = feeds.page_path(feed_directory, 123456789)
    assert got == feed_directory / "123/456/123456789.json"
    got = feeds.page_path(feed_directory, 1234567890)
    assert got == feed_directory / "1234/567/1234567890.json"


def test_stage_feed_appends_without_changing_full_pages(tmp_path):
    destination = tmp_path / "published"
    initial_staging = tmp_path / "initial"
    initial_paths, position = feeds.stage_feed(
        [version_entry(index) for index in range(150)],
        "works",
        feeds.FeedPosition(),
        destination,
        initial_staging,
        "https://example.test",
    )
    for path in initial_paths:
        target = destination / path.relative_to(initial_staging)
        target.parent.mkdir(parents=True, exist_ok=True)
        path.replace(target)
    first_page_before = feeds.page_path(destination / "works", 1).read_text()

    paths, position = feeds.stage_feed(
        [version_entry(index) for index in range(150, 175)],
        "works",
        position,
        destination,
        tmp_path / "next",
        "https://example.test",
    )

    assert [path.name for path in paths] == ["2.json"]
    assert len(feeds.load_page(paths[0]).orderedItems) == 75
    assert position == feeds.FeedPosition(page=2, count=75)
    assert feeds.page_path(destination / "works", 1).read_text() == first_page_before


def test_generate_feeds_publishes_then_advances_cursors(tmp_path, monkeypatch):
    query_count = 0

    def new_versions(bluecore_db, current_cursor, batch_size=None):
        nonlocal query_count
        query_count += 1
        assert batch_size == 10_000
        if query_count == 1:
            assert current_cursor == 0
            return {"hubs": [], "works": [version_entry(1)], "instances": []}
        assert current_cursor == 1
        return {"hubs": [], "works": [], "instances": []}

    monkeypatch.setattr(feeds, "new_versions", new_versions)

    result = feeds.generate_feeds(
        "postgresql://bluecore", str(tmp_path), "https://example.test"
    )

    assert result == 1
    assert query_count == 2
    state = feeds.load_state(tmp_path)
    assert state.version_id == 1
    assert state.feeds["works"] == feeds.FeedPosition(page=1, count=1)
    collection = feeds.ActivityStreamsCollection.model_validate_json(
        (tmp_path / "works/feed.json").read_text()
    )
    assert collection.totalItems == 1
    assert (
        feeds.load_page(feeds.page_path(tmp_path / "works", 1)).orderedItems[0].id
        == "urn:bluecore:version:1"
    )


def test_populate_feeds_processes_bounded_batches(tmp_path, monkeypatch):
    batches = [
        {"hubs": [], "works": [version_entry(1)], "instances": []},
        {"hubs": [], "works": [version_entry(2)], "instances": []},
        {"hubs": [], "works": [], "instances": []},
    ]

    def new_versions(bluecore_db, after_id, batch_size):
        assert batch_size == 1
        batch = batches.pop(0)
        if batch["works"]:
            assert batch["works"][0].id == after_id + 1
        return batch

    monkeypatch.setattr(feeds, "new_versions", new_versions)

    assert (
        feeds.populate_feeds("postgresql://bluecore", str(tmp_path), batch_size=1) == 2
    )
    assert feeds.load_state(tmp_path).version_id == 2
    assert (
        feeds.load_page(feeds.page_path(tmp_path / "works", 1))
        .orderedItems[0]
        .id.endswith(":1")
    )
    assert (
        feeds.load_page(feeds.page_path(tmp_path / "works", 1))
        .orderedItems[1]
        .id.endswith(":2")
    )


def test_generate_feeds_backfills_missing_collections_without_changes(
    tmp_path, monkeypatch
):
    (tmp_path / feeds.STATE_FILE_NAME).write_text(
        feeds.FeedState().model_dump_json(), encoding="utf-8"
    )
    monkeypatch.setattr(
        feeds,
        "new_versions",
        lambda bluecore_db, current_cursor, batch_size=None: {
            "hubs": [],
            "works": [],
            "instances": [],
        },
    )

    assert feeds.generate_feeds("postgresql://bluecore", str(tmp_path)) == 0

    for name in feeds.RESOURCE_TYPES:
        collection = feeds.ActivityStreamsCollection.model_validate_json(
            (tmp_path / name / "feed.json").read_text()
        )
        assert collection.totalItems == 0


def test_stale_manifest_rebuilds_without_duplicate_items(tmp_path):
    destination = tmp_path / "published"
    initial_paths, _ = feeds.stage_feed(
        [version_entry(index) for index in range(75)],
        "works",
        feeds.FeedPosition(),
        destination,
        tmp_path / "initial",
        "https://example.test",
    )
    target = feeds.page_path(destination / "works", 1)
    target.parent.mkdir(parents=True)
    initial_paths[0].replace(target)

    paths, position = feeds.stage_feed(
        [version_entry(index) for index in range(50, 75)],
        "works",
        feeds.FeedPosition(page=1, count=50),
        destination,
        tmp_path / "retry",
        "https://example.test",
    )

    rebuilt = feeds.load_page(paths[0])
    assert len(rebuilt.orderedItems) == 75
    assert len({item.id for item in rebuilt.orderedItems}) == 75
    assert position == feeds.FeedPosition(page=1, count=75)


class MockHTTPXResponse:
    def __init__(self, payload=None, content: bytes = b""):
        self.payload = payload
        self.content = content

    def json(self):
        return self.payload

    def raise_for_status(self):
        return


CREATE_CURSOR_TABLE = (
    "CREATE TABLE activity_streams_cursor ("
    "cursor_name VARCHAR(25), "
    '"cursor" VARCHAR(10), '
    "PRIMARY KEY (cursor_name))"
)


def _feed_payload(
    feed_url: str, items: list[tuple[str, str]], next_url: str | None = None
) -> dict:
    return {
        "id": feed_url,
        "type": "OrderedCollectionPage",
        "partOf": "https://id.loc.gov/resources/works/activitystreams/feed.json",
        "next": next_url,
        "orderedItems": [
            {
                "type": "Add",
                "object": {"id": object_url, "type": ["Object"]},
                "published": published,
            }
            for object_url, published in items
        ],
    }


def _feed_request_url(url: str) -> str:
    return f"{bfdb_activity_streams._local(url)}.json"


def _object_request_url(url: str) -> str:
    return f"{bfdb_activity_streams._local(url)}.bibframe.json"


def test_ingest_activity_stream_feed_downloads_item_objects(mocker, tmp_path):
    run_uuid = uuid.UUID("11111111-1111-1111-1111-111111111111")
    run_id = str(run_uuid)
    feed_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    object_url = "https://id.loc.gov/resources/works/123456.json"

    feed_payload = _feed_payload(feed_url, [(object_url, "2026-09-11T00:00:00Z")])

    def mock_get(url: str):
        if url == _feed_request_url(feed_url):
            return MockHTTPXResponse(payload=feed_payload)
        if url == _object_request_url(object_url):
            return MockHTTPXResponse(content=b'{"id": "123456"}')
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.uuid, "uuid4", return_value=run_uuid)
    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    assert (
        bfdb_activity_streams.create_activity_stream_run(airflow_path=str(tmp_path))
        == run_id
    )
    assert not (tmp_path / run_id).exists()

    downloaded_files, newest_published = (
        bfdb_activity_streams.ingest_activity_stream_feed(
            feed_url,
            run_id,
            "works",
            cursor="2026-09-10",
            current_date="2026-09-14",
            airflow_path=str(tmp_path),
        )
    )

    run_path = tmp_path / run_id
    assert downloaded_files == [str(run_path / "123456.work")]
    assert newest_published == "2026-09-11"
    assert (run_path / "123456.work").read_bytes() == b'{"id": "123456"}'


def test_fetch_feed_retries_transient_http_errors(mocker):
    url = "https://id.loc.gov/resources/works/activitystreams/feed/1"
    response = MockHTTPXResponse(payload=_feed_payload(url, []))
    get = mocker.patch.object(
        bfdb_activity_streams.httpx,
        "get",
        side_effect=[httpx.ConnectError("temporary"), response],
    )
    mocker.patch.object(
        bfdb_activity_streams, "wait_exponential", return_value=lambda _: 0
    )

    feed = bfdb_activity_streams._fetch_feed(url)

    assert feed.id == url
    assert get.call_count == 2


def test_download_feed_item_retries_transient_http_errors(mocker, tmp_path):
    item = bfdb_activity_streams.FeedItem(
        type="Add",
        published="2026-09-15",
        object={"id": "https://id.loc.gov/resources/works/1", "type": ["Object"]},
    )
    get = mocker.patch.object(
        bfdb_activity_streams.httpx,
        "get",
        side_effect=[httpx.ConnectError("temporary"), MockHTTPXResponse(content=b"ok")],
    )
    mocker.patch.object(
        bfdb_activity_streams, "wait_exponential", return_value=lambda _: 0
    )

    path = bfdb_activity_streams._download_feed_item(item, "works", tmp_path, set())

    assert path.read_bytes() == b"ok"
    assert get.call_count == 2


def test_save_cursor_retries_operational_errors(mocker):
    attempts = 0

    class Engine:
        def begin(self):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise OperationalError("INSERT", {}, Exception("temporary"))
            return _CursorConnection()

        def dispose(self):
            pass

    mocker.patch.object(bfdb_activity_streams, "create_engine", return_value=Engine())
    bfdb_activity_streams.save_cursor(
        "postgresql://bluecore", "bfdb_works", "2026-09-15"
    )

    assert attempts == 3


class _CursorConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def execute(self, statement, params):
        pass


def test_ingest_activity_stream_feed_skips_outside_cursor_window(mocker, tmp_path):
    run_id = "22222222-2222-2222-2222-222222222222"
    feed_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    future_url = "https://id.loc.gov/resources/works/future.json"
    current_url = "https://id.loc.gov/resources/works/current.json"
    stale_url = "https://id.loc.gov/resources/works/stale.json"

    feed_payload = _feed_payload(
        feed_url,
        [
            (future_url, "2026-09-20T00:00:00Z"),
            (current_url, "2026-09-12T00:00:00Z"),
            (stale_url, "2026-09-03T00:00:00Z"),
        ],
    )

    def mock_get(url: str):
        if url == _feed_request_url(feed_url):
            return MockHTTPXResponse(payload=feed_payload)
        if url == _object_request_url(current_url):
            return MockHTTPXResponse(content=b'{"id": "current"}')
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    downloaded_files, newest_published = (
        bfdb_activity_streams.ingest_activity_stream_feed(
            feed_url,
            run_id,
            "works",
            cursor="2026-09-10",
            current_date="2026-09-14",
            airflow_path=str(tmp_path),
        )
    )

    run_path = tmp_path / run_id
    assert downloaded_files == [str(run_path / "current.work")]
    assert newest_published == "2026-09-12"
    assert not (run_path / "stale.work").exists()
    assert not (run_path / "future.work").exists()


def test_ingest_activity_stream_feed_skips_cursor_date(mocker, tmp_path):
    run_id = "99999999-9999-9999-9999-999999999999"
    feed_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    object_url = "https://id.loc.gov/resources/works/late.json"
    feed_payload = _feed_payload(feed_url, [(object_url, "2026-09-10T23:59:59Z")])

    def mock_get(url: str):
        if url == _feed_request_url(feed_url):
            return MockHTTPXResponse(payload=feed_payload)
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    downloaded_files, newest_published = (
        bfdb_activity_streams.ingest_activity_stream_feed(
            feed_url,
            run_id,
            "works",
            cursor="2026-09-10",
            current_date="2026-09-17",
            airflow_path=str(tmp_path),
        )
    )

    assert downloaded_files == []
    assert newest_published is None


def test_ingest_activity_stream_feed_ignores_1969_sentinel(mocker, tmp_path):
    run_id = "55555555-5555-5555-5555-555555555555"
    feed_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    future_url = "https://id.loc.gov/resources/works/future.json"
    current_url = "https://id.loc.gov/resources/works/current.json"
    sentinel_url = "https://id.loc.gov/resources/works/sentinel.json"
    after_stop_url = "https://id.loc.gov/resources/works/after-stop.json"

    feed_payload = _feed_payload(
        feed_url,
        [
            (future_url, "2026-09-20T00:00:00Z"),
            (current_url, "2026-09-12T00:00:00Z"),
            (sentinel_url, "1969-01-01T00:00:00Z"),
            (after_stop_url, "2026-09-13T00:00:00Z"),
        ],
    )

    def mock_get(url: str):
        if url == _feed_request_url(feed_url):
            return MockHTTPXResponse(payload=feed_payload)
        objects = {
            _object_request_url(current_url): b'{"id": "current"}',
            _object_request_url(after_stop_url): b'{"id": "after-stop"}',
        }
        if url in objects:
            return MockHTTPXResponse(content=objects[url])
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    downloaded_files, newest_published = (
        bfdb_activity_streams.ingest_activity_stream_feed(
            feed_url,
            run_id,
            "works",
            cursor="2026-09-10",
            current_date="2026-09-14",
            airflow_path=str(tmp_path),
        )
    )

    run_path = tmp_path / run_id
    assert downloaded_files == [
        str(run_path / "current.work"),
        str(run_path / "after-stop.work"),
    ]
    assert newest_published == "2026-09-13"


def test_ingest_activity_stream_feed_follows_next_until_stale_pages(mocker, tmp_path):
    run_id = "66666666-6666-6666-6666-666666666666"
    page_one_url = "https://id.loc.gov/resources/works/activitystreams/feed/3.json"
    page_two_url = "https://id.loc.gov/resources/works/activitystreams/feed/2.json"
    page_three_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    newest_url = "https://id.loc.gov/resources/works/newest.json"
    older_url = "https://id.loc.gov/resources/works/older.json"
    at_cursor_url = "https://id.loc.gov/resources/works/at-cursor.json"
    unreached_url = "https://id.loc.gov/resources/works/unreached.json"

    pages = {
        _feed_request_url(page_one_url): _feed_payload(
            page_one_url, [(newest_url, "2026-09-13T00:00:00Z")], next_url=page_two_url
        ),
        _feed_request_url(page_two_url): _feed_payload(
            page_two_url, [(older_url, "2026-09-12T00:00:00Z")], next_url=page_three_url
        ),
        _feed_request_url(page_three_url): _feed_payload(
            page_three_url,
            [
                (at_cursor_url, "2026-09-10T00:00:00Z"),
                (unreached_url, "2026-09-09T00:00:00Z"),
            ],
        ),
    }
    objects = {
        _object_request_url(newest_url): b'{"id": "newest"}',
        _object_request_url(older_url): b'{"id": "older"}',
    }

    def mock_get(url: str):
        if url in pages:
            return MockHTTPXResponse(payload=pages[url])
        if url in objects:
            return MockHTTPXResponse(content=objects[url])
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    downloaded_files, newest_published = (
        bfdb_activity_streams.ingest_activity_stream_feed(
            page_one_url,
            run_id,
            "works",
            cursor="2026-09-10",
            current_date="2026-09-14",
            airflow_path=str(tmp_path),
        )
    )

    run_path = tmp_path / run_id
    assert downloaded_files == [
        str(run_path / "newest.work"),
        str(run_path / "older.work"),
    ]
    assert newest_published == "2026-09-13"


def test_ingest_activity_stream_feed_requires_cursor(mocker, tmp_path):
    mock_get = mocker.patch.object(bfdb_activity_streams.httpx, "get")

    with pytest.raises(ValueError, match="bfdb_works"):
        bfdb_activity_streams.ingest_activity_stream_feed(
            "https://id.loc.gov/resources/works/activitystreams/feed/1.json",
            "77777777-7777-7777-7777-777777777777",
            "works",
            cursor=None,
            current_date="2026-09-14",
            airflow_path=str(tmp_path),
        )

    mock_get.assert_not_called()


def test_get_last_cursor_returns_latest_cursor(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'bluecore.db'}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(CREATE_CURSOR_TABLE))
        connection.execute(
            text(
                'INSERT INTO activity_streams_cursor (cursor_name, "cursor") '
                "VALUES (:cursor_name, :cursor)"
            ),
            [
                {"cursor_name": "bfdb_works", "cursor": "2026-09-12"},
                {"cursor_name": "bfdb_hubs", "cursor": "2026-09-13"},
            ],
        )
    engine.dispose()

    assert (
        bfdb_activity_streams.get_last_cursor(database_url, "bfdb_works")
        == "2026-09-12"
    )
    assert (
        bfdb_activity_streams.get_last_cursor(database_url, "bfdb_hubs") == "2026-09-13"
    )


def test_save_cursor_overwrites_existing_cursor(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'bluecore.db'}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(CREATE_CURSOR_TABLE))
    engine.dispose()

    bfdb_activity_streams.save_cursor(database_url, "bfdb_works", "2026-09-10")
    bfdb_activity_streams.save_cursor(database_url, "bfdb_works", "2026-09-12")

    engine = create_engine(database_url)
    with engine.connect() as connection:
        rows = connection.execute(
            text('SELECT cursor_name, "cursor" FROM activity_streams_cursor')
        ).all()
    engine.dispose()

    assert rows == [("bfdb_works", "2026-09-12")]


def test_get_last_cursor_without_rows(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'empty.db'}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(CREATE_CURSOR_TABLE))
    engine.dispose()

    assert bfdb_activity_streams.get_last_cursor(database_url, "bfdb_works") is None


def test_feed_run_path_uses_shared_run_directory(tmp_path):
    run_id = "88888888-8888-8888-8888-888888888888"

    assert bfdb_activity_streams.feed_run_path(str(tmp_path), run_id, "works") == (
        tmp_path / run_id
    )
    assert bfdb_activity_streams.feed_run_path(str(tmp_path), run_id, "instances") == (
        tmp_path / run_id
    )
    assert bfdb_activity_streams.feed_run_path(str(tmp_path), run_id, "hubs") == (
        tmp_path / run_id
    )


def test_local_file_name_uses_feed_suffix():
    used_file_names: set[str] = set()

    assert (
        bfdb_activity_streams._local_file_name(
            "http://id.loc.gov/resources/works/1234", "work", used_file_names
        )
        == "1234.work"
    )
    assert (
        bfdb_activity_streams._local_file_name(
            "http://id.loc.gov/resources/works/1234", "work", used_file_names
        )
        == "1234-2.work"
    )


def test_combine_downloads_merges_same_lc_id_across_feed_types(tmp_path):
    work_path = tmp_path / "1234.work"
    instance_path = tmp_path / "1234.instance"
    hub_path = tmp_path / "5678.hub"
    work_path.write_text('[{"@id": "work"}]', encoding="utf-8")
    instance_path.write_text('[{"@id": "instance"}]', encoding="utf-8")
    hub_path.write_text('[{"@id": "hub"}]', encoding="utf-8")

    collected = bfdb_activity_streams.combine_downloads(
        [[str(work_path)], [str(instance_path)], [str(hub_path)]]
    )

    combined_path = tmp_path / "1234.json"
    assert collected == [str(combined_path), str(hub_path)]
    assert json.loads(combined_path.read_text(encoding="utf-8")) == [
        {"@id": "work"},
        {"@id": "instance"},
    ]
    assert not work_path.exists()
    assert not instance_path.exists()
    assert hub_path.exists()


def test_combine_downloads_rejects_non_array_documents(tmp_path):
    work_path = tmp_path / "1234.work"
    instance_path = tmp_path / "1234.instance"
    work_path.write_text('{"@id": "work"}', encoding="utf-8")
    instance_path.write_text('[{"@id": "instance"}]', encoding="utf-8")

    with pytest.raises(TypeError, match="does not contain a JSON array"):
        bfdb_activity_streams.combine_downloads(
            [[str(work_path)], [str(instance_path)]]
        )

    assert work_path.exists()
    assert instance_path.exists()


def test_cursor_name_for_feed():
    assert [
        bfdb_activity_streams.cursor_name_for(feed["feed_name"])
        for feed in bfdb_activity_streams.ACTIVITY_STREAM_FEEDS
    ] == ["bfdb_works", "bfdb_instances", "bfdb_hubs"]


def test_process_activity_stream_feed_advances_feed_cursor(mocker, tmp_path):
    run_uuid = uuid.UUID("33333333-3333-3333-3333-333333333333")
    run_id = str(run_uuid)
    feed_url = "https://id.loc.gov/resources/works/activitystreams/feed/1.json"
    object_url = "https://id.loc.gov/resources/works/654321.json"

    database_url = f"sqlite:///{tmp_path / 'bluecore.db'}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(CREATE_CURSOR_TABLE))
        connection.execute(
            text(
                'INSERT INTO activity_streams_cursor (cursor_name, "cursor") '
                "VALUES (:cursor_name, :cursor)"
            ),
            {"cursor_name": "bfdb_works", "cursor": "2026-09-10"},
        )
    engine.dispose()

    feed_payload = _feed_payload(feed_url, [(object_url, "2026-09-12T00:00:00Z")])

    def mock_get(url: str):
        if url == _feed_request_url(feed_url):
            return MockHTTPXResponse(payload=feed_payload)
        if url == _object_request_url(object_url):
            return MockHTTPXResponse(content=b'{"id": "654321"}')
        raise AssertionError(f"Unexpected URL {url}")

    mocker.patch.object(bfdb_activity_streams.httpx, "get", side_effect=mock_get)

    downloaded_files = bfdb_activity_streams.process_activity_stream_feed(
        database_url,
        feed_url,
        "works",
        "2026-09-14",
        run_id,
        airflow_path=str(tmp_path),
    )

    assert downloaded_files == [str(tmp_path / run_id / "654321.work")]
    assert (
        bfdb_activity_streams.get_last_cursor(database_url, "bfdb_works")
        == "2026-09-12"
    )
    assert bfdb_activity_streams.get_last_cursor(database_url, "bfdb_hubs") is None


def test_process_activity_stream_feed_keeps_cursor_when_nothing_processed(
    mocker, tmp_path
):
    run_uuid = uuid.UUID("44444444-4444-4444-4444-444444444444")
    feed_url = "https://id.loc.gov/resources/hubs/activitystreams/feed/1.json"

    database_url = f"sqlite:///{tmp_path / 'bluecore.db'}"
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(CREATE_CURSOR_TABLE))
        connection.execute(
            text(
                'INSERT INTO activity_streams_cursor (cursor_name, "cursor") '
                "VALUES (:cursor_name, :cursor)"
            ),
            {"cursor_name": "bfdb_hubs", "cursor": "2026-09-13"},
        )
    engine.dispose()

    feed_payload = _feed_payload(
        feed_url,
        [("https://id.loc.gov/resources/hubs/old.json", "2026-09-06T00:00:00Z")],
    )
    mocker.patch.object(
        bfdb_activity_streams.httpx,
        "get",
        side_effect=lambda url: MockHTTPXResponse(payload=feed_payload),
    )
    downloaded_files = bfdb_activity_streams.process_activity_stream_feed(
        database_url,
        feed_url,
        "hubs",
        "2026-09-14",
        str(run_uuid),
        airflow_path=str(tmp_path),
    )

    assert downloaded_files == []
    assert (
        bfdb_activity_streams.get_last_cursor(database_url, "bfdb_hubs") == "2026-09-13"
    )
