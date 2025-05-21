from dataclasses import dataclass
from datetime import date, timedelta
import pandas as pd
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
df = pd.read_pickle("./conversations.pkl")
print(f"Number of conversations: {df.index.get_level_values('conversation').nunique()}")
print(f"Time taken: {str(timedelta(seconds=time.time() - start_time))}")
