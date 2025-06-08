import pandas as pd
import numpy as np

df = pd.read_pickle(r'C:\Users\marcv\OneDrive - TU Eindhoven\Escritorio\Data Science\Year 1\Q4\DBL Data Challenge\conversation-store\categorized_conversations.pkl')

def compute_change(conv: pd.DataFrame):
    initial: int = conv.iloc[0]['sentiment_score']
    latter: int = np.mean(conv.iloc[2:]['sentiment_score'])

    conv['sentiment_change'] = latter - initial
    return conv

df = df.groupby('conversation', group_keys=False).apply(compute_change)
df.to_pickle('sentiment_conversations.pkl')

df.head(20)