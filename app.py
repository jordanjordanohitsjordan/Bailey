import os
from fastapi import FastAPI, HTTPException, Request, Query
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional

# 1. Load env and tokens
load_dotenv()
VERIFY_TOKEN = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")

# 2. Instantiate FastAPI
app = FastAPI()

# 3. Verification endpoint (GET)
@app.get("/webhook")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification token mismatch")

# 4. Models for POST payload
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

# 5. Incoming events handler (POST)
@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload, request: Request):
    if payload.object != "page":
        return {"status": "ignored"}

    for entry in payload.entry:
        for change in entry.messaging:
            if change.message and change.sender:
                print("Received IG DM event:", change.json())
                # TODO: enqueue for processing
    return {"status": "processed"}