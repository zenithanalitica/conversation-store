import logging
from typing import Any
from datetime import timedelta
import time
import pandas as pd

from . import db
from .tweet import Tweet
from . import tweet as tweet


def main():
    logger = create_logger()
    load_conversations(logger)


def parse_to_df(conversations: list[tuple[str, list[Tweet]]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []  # pyright: ignore[reportExplicitAny]
    index: list[tuple[str, int, int]] = []

    for group_idx, (airline_id, conv) in enumerate(conversations):
        for position, obj in enumerate(conv):
            index.append((airline_id, group_idx, position))  # MultiIndex keys
            rows.append(vars(obj))

    multi_index = pd.MultiIndex.from_tuples(
        index, names=["airline", "conversation", "tweet"]
    )
    df = pd.DataFrame(rows, index=multi_index)
    return df


def load_conversations(logger: logging.Logger) -> None:
    conversations: list[tuple[str, list[Tweet]]] = []
    start_time = time.time()

    records = db.get_conversations(logger)
    logger.debug(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")

    for record in records:
        conversations.append(tweet.make_conversation(record))

    logger.info("Parsing conversations to dataframe...")
    df = parse_to_df(conversations)

    logger.info("Saving conversations to file...")
    pd.to_pickle(df, "conversations.pkl")
    logger.debug(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")


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
