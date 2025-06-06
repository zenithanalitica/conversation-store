import pandas as pd

df = pd.read_pickle("./conversations.pkl")

category_keywords = {
    "flight delays and cancellations": [
        "delay",
        "delayed",
        "cancelled",
        "cancel",
        "rescheduled",
        "late",
        "no information",
        "boarding closed",
    ],
    "baggage and check in issues": [
        "baggage",
        "luggage",
        "lost bag",
        "missing bag",
        "check-in",
        "check in",
        "boarding denied",
        "boarding closed early",
    ],
    "booking app payment problems": [
        "book",
        "booking",
        "app",
        "seat",
        "payment",
        "ticket",
        "website",
        "log in",
        "error",
    ],
    "customer service and response quality": [
        "no reply",
        "ignored",
        "unhelpful",
        "bad service",
        "rude",
        "no response",
        "dm",
        "support",
        "mail",
        "customer-service",
    ],
    "refunds and compensation requests": [
        "refund",
        "compensation",
        "claim",
        "voucher",
        "money back",
        "eu261",
        "reimbursement",
    ],
}

# Initialize counter
categories = {category: 0 for category in category_keywords}
categories["other"] = 0

for index, conv in df.groupby("conversation"):
    text = conv.iloc[0]["text"].lower()

    for category, keywords in category_keywords.items():
        if any(keyword in text for keyword in keywords):
            categories[category] += 1
            df.loc[conv.index, "category"] = category.split()[0]
            break
    else:
        categories["other"] += 1


# Printing results
for category in categories:
    share = round(
        categories[category]
        / sum(categories[category] for category in categories)
        * 100,
        1,
    )
    print(f"{category}: {categories[category]} ({share}%)")

