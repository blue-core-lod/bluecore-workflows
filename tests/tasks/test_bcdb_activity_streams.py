from datetime import UTC, datetime, timedelta

from sqlalchemy.dialects import postgresql

from ils_middleware.tasks import bcdb_activity_streams as feeds


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
