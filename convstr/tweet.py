from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, cast, get_type_hints
from datetime import date

from neo4j import Record


@dataclass
class Tweet:
    id: str
    text: str
    sentiment_label: str
    sentiment_score: float
    created_at: date
    negative: float
    neutral: float
    positive: float
    reply_to: str | None = None


class TweetData(TypedDict):
    id: str
    text: str
    sentiment_label: str
    sentiment_score: float
    created_at: date
    negative: float
    neutral: float
    positive: float
    reply_to: NotRequired[str]


def make_tweet(data: TweetData) -> Tweet:
    return Tweet(**data)


def filter_to_tweet_fields(
    data: dict[Any, Any],  # pyright: ignore[reportExplicitAny]
) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]
    allowed_keys = get_type_hints(TweetData).keys()
    return {k: data[k] for k in allowed_keys if k in data}


def make_conversation(record: Record) -> tuple[str, list[Tweet]]:
    conversation: list[Tweet] = []
    data = record.data()
    airline_id: str = cast(str, data["airline_id"])

    parent_data: TweetData = filter_to_tweet_fields(data["parent"])  # pyright: ignore[reportAny, reportAssignmentType]
    parent = make_tweet(parent_data)
    conversation.append(parent)

    tree_nodes = data["tree_nodes"]  # pyright: ignore[reportAny]
    for node in tree_nodes:  # pyright: ignore[reportAny]
        reply_data: TweetData = filter_to_tweet_fields(node)  # pyright: ignore[reportAny, reportAssignmentType]
        reply = make_tweet(reply_data)
        conversation.append(reply)

    return (airline_id, conversation)
