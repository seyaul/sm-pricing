# graph_ingestion/fetcher.py
import requests
from graph_ingestion.auth import get_graph_token

GRAPH_API = "https://graph.microsoft.com/v1.0"
USER_EMAIL = "vendorfeed@streetsmarket.com"

def fetch_latest_emails(limit=5):
    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}"}
    
    url = f"{GRAPH_API}/users/{USER_EMAIL}/messages?$top={limit}&$orderby=receivedDateTime desc"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    messages = data.get("value", [])
    
    print(f"\n📬 Found {len(messages)} emails:\n")
    for msg in messages:
        print(msg)
        print(f"Subject: {msg['subject']}")
        print(f"From:    {msg['from']['emailAddress']['address']}")
        print(f"Date:    {msg['receivedDateTime']}")
        print("-" * 40)

if __name__ == "__main__":
    fetch_latest_emails()
