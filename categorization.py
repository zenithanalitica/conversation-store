import pandas as pd

#Categorie lists of keywords

flight_delays_and_cancellations = [
    "delay", "delayed", "cancelled", "cancel", "rescheduled", 
    "late", "no information", "boarding closed"
]

baggage_and_check_in_issues = [
    "baggage", "luggage", "lost bag", "missing bag", "check-in", 
    "check in", "boarding denied", "boarding closed early"
]

booking_app_payment_problems = [
    "book", "booking", "app", "seat", "payment", "ticket", 
    "website", "log in", "error"
]

customer_service_and_response_quality = [
    "no reply", "ignored", "unhelpful", "bad service", "rude", 
    "no response", "dm", "support", "mail", "customer-service"
]

refunds_and_compensation_requests = [
    "refund", "compensation", "claim", "voucher", "money back", 
    "eu261", "reimbursement"
]

df = pd.read_pickle(r'C:\Users\marcv\OneDrive - TU Eindhoven\Escritorio\Data Science\Year 1\Q4\DBL Data Challenge\test.pkl')
a = 0

#Count of occurences

categories = {
    "flight delays and cancellations": 0,
    "baggage and check in issues": 0,
    "booking app payment problems": 0,
    "customer service and response quality": 0,
    "refunds and compensation requests": 0,
    "other": 0
    }

for index, conv in df.groupby('conversation'):
    text = (conv.iloc[0]['']).lower()
    
    for keyword in flight_delays_and_cancellations:
        if keyword in text:
            categories['flight delays and cancellations'] += 1
            pass

    for keyword in baggage_and_check_in_issues:
        if keyword in text:
            categories['baggage and check in issues'] += 1
            pass

    for keyword in booking_app_payment_problems:
        if keyword in text:
            categories['booking app payment problems'] += 1
            pass

    for keyword in customer_service_and_response_quality:
        if keyword in text:
            categories['customer service and response quality'] += 1
            pass

    for keyword in refunds_and_compensation_requests:
        if keyword in text: 
            categories['refunds and compensation requests'] += 1
            pass
    
    categories['other'] += 1

    # a+=1
    # if a == 10:
    #     break