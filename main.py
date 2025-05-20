from dataclasses import dataclass
from typing import Self
from datetime import date, timedelta
import time

import db


@dataclass
class Tweet:
    id: str
    labels: list[str]
    sentiment_label: str
    created_at: date
    reply_to: Self


def main():
    start_time = time.time()
    records = db.get_conversations()
    end_time = time.time()
    time_spent = end_time - start_time
    print(f"Time taken: {str(timedelta(seconds=time_spent))}")


if __name__ == "__main__":
    main()
