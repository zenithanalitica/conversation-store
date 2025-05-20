from dataclasses import dataclass
from datetime import date, timedelta
import pickle
import time


@dataclass
class Tweet:
    id: str
    sentiment_label: str
    created_at: date
    negative: float
    neutral: float
    positive: float
    reply_to: str | None = None


start_time = time.time()
with open("./conversations.pkl", "rb") as f:
    conversations: list[list[Tweet]] = pickle.load(f)
    print(f"Number of conversations: {len(conversations)}")
print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")
