from dataclasses import dataclass
from datetime import date
from typing import Self
import db


@dataclass
class Tweet:
    id: str
    labels: list[str]
    sentiment_label: str
    created_at: date
    reply_to: Self


def main():
    records = db.get_conversations()


if __name__ == "__main__":
    main()
