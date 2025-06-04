from typing import Any
from datetime import timedelta
import time
import pandas as pd

import db
from lib.tweet import Tweet
import lib.tweet as tweet


def parse_to_df(conversations: list[list[Tweet]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []  # pyright: ignore[reportExplicitAny]
    index: list[tuple[int, int]] = []

    for group_idx, conv in enumerate(conversations):
        for position, obj in enumerate(conv):
            index.append((group_idx, position))  # MultiIndex keys
            rows.append(vars(obj))

    multi_index = pd.MultiIndex.from_tuples(index, names=["conversation", "tweet"])
    df = pd.DataFrame(rows, index=multi_index)
    return df


def main():
    conversations: list[list[Tweet]] = []
    start_time = time.time()
    records = db.get_conversations()
    print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")

    for record in records:
        conversations.append(tweet.make_conversation(record))

    print(f"Number of conversations: {len(conversations)}")

    start_time = time.time()
    df = parse_to_df(conversations)
    pd.to_pickle(df, "test.pkl")
    print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")


if __name__ == "__main__":
    main()
