import os
import requests as api
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=True)

broward_auth_key = os.getenv("BROWARD_CLERK_APIKEY")
broward_clerk_url = os.getenv("BROWARD_CLERK_URL")


def apiEventData(caseNo) -> list | None:
    broward_auth_key = os.getenv("BROWARD_CLERK_APIKEY")
    broward_clerk_url = os.getenv("BROWARD_CLERK_URL")

    url = f"{broward_clerk_url}/case/{caseNo}/events_and_documents.json"
    params = {"auth_key": broward_auth_key}

    response = api.get(url, params)
    eventData = response.json()

    if eventData is None or "EventList" not in eventData.keys():
        return None
    else:
        return eventData["EventList"]
    
