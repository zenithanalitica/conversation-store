from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, get_type_hints
from datetime import date, timedelta
import time
import pandas as pd

from neo4j import Record

import db


@dataclass
class Tweet:
    id: str
    sentiment_label: str
    created_at: date
    negative: float
    neutral: float
    positive: float
    reply_to: str | None = None


class TweetData(TypedDict):
    id: str
    sentiment_label: str
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


def make_conversation(record: Record) -> list[Tweet]:
    conversation: list[Tweet] = []
    data = record.data()

    parent_data: TweetData = filter_to_tweet_fields(data["parent"])  # pyright: ignore[reportAny, reportAssignmentType]
    parent = make_tweet(parent_data)
    conversation.append(parent)

    tree_nodes = data["tree_nodes"]  # pyright: ignore[reportAny]
    for node in tree_nodes:  # pyright: ignore[reportAny]
        reply_data: TweetData = filter_to_tweet_fields(node)  # pyright: ignore[reportAny, reportAssignmentType]
        reply = make_tweet(reply_data)
        conversation.append(reply)

    return conversation


def parse_to_df(conversations: list[list[Tweet]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []  # pyright: ignore[reportExplicitAny]
    index: list[tuple[int, int]] = []

    for group_idx, conv in enumerate(conversations):
        for position, obj in enumerate(conv):
            index.append((group_idx, position))  # MultiIndex keys
            rows.append(
                vars(obj)
            )  # Or manually: {'name': obj.name, 'value': obj.value}

    multi_index = pd.MultiIndex.from_tuples(index, names=["conversation", "tweet"])
    df = pd.DataFrame(rows, index=multi_index)
    return df


def main():
    conversations: list[list[Tweet]] = []
    start_time = time.time()
    records = db.get_conversations()
    print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")

    for record in records:
        conversations.append(make_conversation(record))

    print(f"Number of conversations: {len(conversations)}")

    df = parse_to_df(conversations)
    pd.to_pickle(df, "conversations.pkl")


if __name__ == "__main__":
    main()
