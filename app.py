import os
from fastapi import FastAPI, HTTPException, Request, Query
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional

# Load environment variables
load_dotenv()
VERIFY_TOKEN = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")

# Initialize FastAPI app
app = FastAPI()

# Webhook verification (GET)
@app.get("/webhook")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification token mismatch")

# Data models for parsing messaging events
class MessagingEntry(BaseModel):
    sender: dict
    recipient: dict
    timestamp: int
    message: Optional[dict]

class WebhookEntry(BaseModel):
    id: str
    time: int
    messaging: List[MessagingEntry]

class WebhookPayload(BaseModel):
    object: str
    entry: List[WebhookEntry]

# Incoming events handler (POST)
@app.post("/webhook")
async def receive_webhook(request: Request):
    # Parse the raw payload
    data = await request.json()
    print("Webhook payload:", data)

    # Only process supported objects
    if data.get("object") not in ("page", "instagram"):
        return {"status": "ignored"}

    # Loop through entries
    for entry in data.get("entry", []):
        # Direct messaging events if present
        messaging_events = entry.get("messaging", [])
        
        # Fallback for Instagram-style payloads under changes
        if not messaging_events:
            for change in entry.get("changes", []):
                messaging_events.extend(
                    change.get("value", {}).get("messaging", [])
                )

        # Process each messaging event
        for event in messaging_events:
            sender = event.get("sender", {})
            message = event.get("message")
            if sender and message:
                print("Received IG DM event:", event)
                # TODO: enqueue the event for downstream processing

    return {"status": "processed"}