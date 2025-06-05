import logging
from typing import Any
from datetime import timedelta
import time
import pandas as pd

from . import db
from .tweet import Tweet
from . import tweet as tweet


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


def load_conversations(logger: logging.Logger) -> None:
    conversations: list[list[Tweet]] = []

    records = db.get_conversations(logger)

    for record in records:
        conversations.append(tweet.make_conversation(record))

    logger.info("Parsing conversations to dataframe...")
    df = parse_to_df(conversations)

    logger.info("Saving conversations to file...")
    pd.to_pickle(df, "conversations.pkl")


def main():
    conversations: list[list[Tweet]] = []
    logger = create_logger()
    start_time = time.time()

    records = db.get_conversations(logger)
    print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")

    for record in records:
        conversations.append(tweet.make_conversation(record))

    print(f"Number of conversations: {len(conversations)}")

    start_time = time.time()
    df = parse_to_df(conversations)
    pd.to_pickle(df, "conversations.pkl")
    print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")


def create_logger() -> logging.Logger:
    logger: logging.Logger = logging.getLogger("logger")
    logger.setLevel(logging.DEBUG)

    # Create a console handler and set its level
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    # Create a formatter
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger


if __name__ == "__main__":
    main()
