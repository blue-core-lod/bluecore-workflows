"""Generate static, incrementally paginated feeds from Blue Core versions."""

import pathlib
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bluecore_models.models import ResourceBase, Version
from pydantic import BaseModel, Field
from sqlalchemy import exists, select
from sqlalchemy.orm import Session, aliased

from ils_middleware.tasks.bfdb.schema.schema_feed import (
    ActivityStreamsCollection,
    ActivityStreamsFeed,
    CollectionPageReference,
    FeedItem,
    FeedObject,
)
from ils_middleware.tasks.bluecore import get_engine

PAGE_SIZE = 100
ACTIVITY_STREAMS_CONTEXT: list[Any] = [
    "https://www.w3.org/ns/activitystreams",
    "https://emm-spec.org/1.0/context.json",
    {"bf": "http://id.loc.gov/ontologies/bibframe/"},
]
COLLECTION_CONTEXT = ACTIVITY_STREAMS_CONTEXT[:2]
RESOURCE_TYPES = {
    "hubs": "bf:Hub",
    "works": "bf:Work",
    "instances": "bf:Instance",
}
EVENT_ID_PREFIX = "urn:bluecore:version:"
STATE_FILE_NAME = "feed-state.json"


@dataclass(frozen=True)
class VersionEntry:
    id: int
    uri: str
    data: dict[str, Any]
    actor: str | None
    created_at: datetime
    is_first: bool


class FeedPosition(BaseModel):
    page: int = Field(default=1, ge=1)
    count: int = Field(default=0, ge=0, le=PAGE_SIZE)


class FeedState(BaseModel):
    version_id: int = Field(default=0, ge=0)
    feeds: dict[str, FeedPosition] = Field(
        default_factory=lambda: {name: FeedPosition() for name in RESOURCE_TYPES}
    )


def new_versions(
    bluecore_db: str, after_id: int, batch_size: int | None = None
) -> dict[str, list[VersionEntry]]:
    """Return new versions for all feeds using one ordered database query."""
    earlier_version = aliased(Version)
    # An Add is the first Version for its resource. NOT EXISTS limits the work to
    # an indexed lookup per new row instead of grouping the full versions table.
    is_first = ~exists().where(
        earlier_version.resource_id == Version.resource_id,
        earlier_version.id < Version.id,
    )
    statement = (
        select(
            ResourceBase.type,
            Version.id,
            ResourceBase.uri,
            Version.data,
            Version.keycloak_user_id,
            Version.created_at,
            is_first.label("is_first"),
        )
        .join(ResourceBase, Version.resource_id == ResourceBase.id)
        .where(ResourceBase.type.in_(RESOURCE_TYPES))
        .where(Version.id > after_id)
        .order_by(Version.id)
    )
    if batch_size is not None:
        statement = statement.limit(batch_size)

    entries: dict[str, list[VersionEntry]] = {name: [] for name in RESOURCE_TYPES}
    with Session(get_engine(bluecore_db)) as session:
        for row in session.execute(statement):
            entries[row[0]].append(VersionEntry(*row[1:]))
    return entries


def load_state(destination: pathlib.Path, *, required: bool = False) -> FeedState:
    state_path = destination / STATE_FILE_NAME
    if not state_path.exists():
        if required:
            raise FileNotFoundError(
                f"{state_path} is missing; run the initial population DAG first"
            )
        return FeedState()
    state = FeedState.model_validate_json(state_path.read_text(encoding="utf-8"))
    if set(state.feeds) != set(RESOURCE_TYPES):
        raise ValueError(f"{state_path} does not contain exactly the supported feeds")
    return state


def page_url(base_url: str, feed_name: str, page_number: int) -> str:
    return f"{base_url.rstrip('/')}/{feed_name}/activitystreams/page/{page_number}"


def build_page(
    items: list[FeedItem],
    feed_name: str,
    page_number: int,
    final_page_number: int,
    base_url: str,
) -> ActivityStreamsFeed:
    feed_root = f"{base_url.rstrip('/')}/{feed_name}/activitystreams"
    return ActivityStreamsFeed.model_validate(
        {
            "@context": ACTIVITY_STREAMS_CONTEXT,
            "id": page_url(base_url, feed_name, page_number),
            "type": "OrderedCollectionPage",
            "partOf": feed_root,
            "prev": (
                page_url(base_url, feed_name, page_number - 1)
                if page_number > 1
                else None
            ),
            "next": (
                page_url(base_url, feed_name, page_number + 1)
                if page_number < final_page_number
                else None
            ),
            "orderedItems": items,
        }
    )


def build_collection(
    feed_name: str, position: FeedPosition, base_url: str
) -> ActivityStreamsCollection:
    """Build the stable entry point for one resource feed."""
    feed_root = f"{base_url.rstrip('/')}/{feed_name}/activitystreams"
    reference_type = "OrderedCollectionPage"
    return ActivityStreamsCollection.model_validate(
        {
            "@context": COLLECTION_CONTEXT,
            "summary": "Bluecore Activity Streams Entry Point",
            "type": "OrderedCollection",
            "id": f"{feed_root}/feed",
            "first": CollectionPageReference(
                id=page_url(base_url, feed_name, 1), type=reference_type
            ),
            "last": CollectionPageReference(
                id=page_url(base_url, feed_name, position.page),
                type=reference_type,
            ),
            # Every page before the current page is full by construction.
            "totalItems": (position.page - 1) * PAGE_SIZE + position.count,
        }
    )


def feed_item(entry: VersionEntry, feed_name: str) -> FeedItem:
    published = entry.created_at.isoformat()
    return FeedItem(
        id=f"{EVENT_ID_PREFIX}{entry.id}",
        type="Add" if entry.is_first else "Update",
        actor=entry.actor,
        published=published,
        object=FeedObject(
            id=entry.uri,
            type=[RESOURCE_TYPES[feed_name]],
            updated=published,
        ),
    )


def load_page(path: pathlib.Path) -> ActivityStreamsFeed:
    return ActivityStreamsFeed.model_validate_json(path.read_text(encoding="utf-8"))


def page_path(feed_directory: pathlib.Path, page_number: int) -> pathlib.Path:
    """Keep page directories bounded while leaving public page URLs unchanged."""
    bucket = page_number // 1000
    shard = str(bucket // 1000).zfill(3)
    subshard = str(bucket % 1000).zfill(3)
    return feed_directory / shard / subshard / f"{page_number}.json"


def stage_feed(
    entries: list[VersionEntry],
    feed_name: str,
    position: FeedPosition,
    destination: pathlib.Path,
    staging: pathlib.Path,
    base_url: str,
) -> tuple[list[pathlib.Path], FeedPosition]:
    """Stage the current partial page and any new pages."""
    feed_directory = destination / feed_name
    current_page_number = position.page
    current_path = page_path(feed_directory, current_page_number)
    current_items: list[FeedItem] = []
    if position.count:
        stored_items = load_page(current_path).orderedItems
        if len(stored_items) < position.count:
            raise ValueError(
                f"{current_path} contains fewer items than {STATE_FILE_NAME} records"
            )
        # The manifest marks the committed prefix. If page publication succeeded
        # but manifest publication failed, discard the uncommitted suffix and
        # deterministically rebuild it from Version rows returned after version_id.
        current_items = stored_items[: position.count]

    # Page numbers increase with Version IDs. Only the last page is extended;
    # earlier pages retain their entries and are never rebuilt from the database.
    groups: list[list[FeedItem]] = [current_items]
    for entry in entries:
        if len(groups[-1]) == PAGE_SIZE:
            groups.append([])
        groups[-1].append(feed_item(entry, feed_name))

    final_page_number = current_page_number + len(groups) - 1
    staged_paths: list[pathlib.Path] = []
    for offset, items in enumerate(groups):
        page_number = current_page_number + offset
        page = build_page(items, feed_name, page_number, final_page_number, base_url)
        staged_path = page_path(staging / feed_name, page_number)
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.write_text(
            page.model_dump_json(by_alias=True, exclude_none=True, indent=2) + "\n",
            encoding="utf-8",
        )
        load_page(staged_path)
        staged_paths.append(staged_path)
    return staged_paths, FeedPosition(
        page=final_page_number,
        count=len(groups[-1]),
    )


def generate_feeds(
    bluecore_db: str,
    output_directory: str = "/opt/airflow/bcdb",
    base_url: str = "https://bcld.info",
    batch_size: int = 10_000,
) -> int:
    """Stage and publish all versions in bounded, resumable batches."""
    return populate_feeds(bluecore_db, output_directory, base_url, batch_size)


def changed_feeds(
    queried: dict[str, list[VersionEntry]],
) -> dict[str, list[VersionEntry]]:
    return {name: entries for name, entries in queried.items() if entries}


def missing_collections(destination: pathlib.Path) -> set[str]:
    return {
        name
        for name in RESOURCE_TYPES
        if not (destination / name / "feed.json").exists()
    }


def next_version_id(
    queried: dict[str, list[VersionEntry]], current_version_id: int
) -> int:
    return max(
        (entry.id for entries in queried.values() for entry in entries),
        default=current_version_id,
    )


def collections_to_publish(
    changed: dict[str, list[VersionEntry]], destination: pathlib.Path
) -> set[str]:
    return set(changed) | missing_collections(destination)


def stage_collections(
    feed_names: set[str],
    positions: dict[str, FeedPosition],
    staging: pathlib.Path,
    base_url: str,
) -> list[pathlib.Path]:
    staged_paths: list[pathlib.Path] = []
    for name in feed_names:
        staged_collection = staging / name / "feed.json"
        staged_collection.parent.mkdir(parents=True, exist_ok=True)
        staged_collection.write_text(
            build_collection(name, positions[name], base_url).model_dump_json(
                by_alias=True, indent=2
            )
            + "\n",
            encoding="utf-8",
        )
        staged_paths.append(staged_collection)
    return staged_paths


def publish_staged_paths(
    staged_paths: list[pathlib.Path], staging: pathlib.Path, destination: pathlib.Path
) -> None:
    for staged_path in staged_paths:
        published_path = destination / staged_path.relative_to(staging)
        published_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.replace(published_path)


def stage_and_publish_batch(
    state: FeedState,
    changed: dict[str, list[VersionEntry]],
    new_cursor: int,
    destination: pathlib.Path,
    base_url: str,
) -> FeedState:
    with tempfile.TemporaryDirectory(
        prefix=".activity-streams-", dir=destination
    ) as tmp:
        staging = pathlib.Path(tmp)
        positions = dict(state.feeds)
        staged_paths: list[pathlib.Path] = []
        for name, entries in changed.items():
            feed_paths, positions[name] = stage_feed(
                entries, name, state.feeds[name], destination, staging, base_url
            )
            staged_paths.extend(feed_paths)

        staged_paths.extend(
            stage_collections(
                collections_to_publish(changed, destination),
                positions,
                staging,
                base_url,
            )
        )

        staged_state = staging / STATE_FILE_NAME
        staged_state.write_text(
            FeedState(version_id=new_cursor, feeds=positions).model_dump_json(indent=2)
            + "\n",
            encoding="utf-8",
        )
        publish_staged_paths(staged_paths, staging, destination)
        staged_state.replace(destination / STATE_FILE_NAME)

    return FeedState(version_id=new_cursor, feeds=positions)


def populate_feeds(
    bluecore_db: str,
    output_directory: str = "/opt/airflow/bcdb",
    base_url: str = "https://bcld.info",
    batch_size: int = 10_000,
) -> int:
    """Populate the feeds in bounded, resumable Version batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    destination = pathlib.Path(output_directory)
    destination.mkdir(parents=True, exist_ok=True)
    state = load_state(destination)

    while True:
        queried = new_versions(bluecore_db, state.version_id, batch_size)
        if not any(queried.values()) and not missing_collections(destination):
            return state.version_id

        changed = changed_feeds(queried)
        state = stage_and_publish_batch(
            state,
            changed,
            next_version_id(queried, state.version_id),
            destination,
            base_url,
        )
        if not queried:
            return state.version_id
