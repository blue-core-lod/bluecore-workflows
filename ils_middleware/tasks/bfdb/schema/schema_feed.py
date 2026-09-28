from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FeedBaseModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )


class FeedObject(FeedBaseModel):
    id: str
    type: list[str]
    updated: str | None = None


class FeedItem(FeedBaseModel):
    id: str | None = None
    type: Literal["Add", "Update", "Delete", "Depreacate"] | None = None
    actor: str | None = None
    object: FeedObject
    published: str


class ActivityStreamsFeed(FeedBaseModel):
    context: str | list[Any] | dict[str, Any] | None = Field(
        default=None, alias="@context"
    )
    id: str
    type: str
    partOf: str
    next: str | None = None
    prev: str | None = None
    orderedItems: list[FeedItem]


class CollectionPageReference(FeedBaseModel):
    id: str
    type: str


class ActivityStreamsCollection(FeedBaseModel):
    context: str | list[Any] | dict[str, Any] | None = Field(
        default=None, alias="@context"
    )
    summary: str
    type: str
    id: str
    first: CollectionPageReference
    last: CollectionPageReference
    totalItems: int
