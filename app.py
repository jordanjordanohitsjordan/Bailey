from fastapi import Request
from pydantic import BaseModel
from typing import List, Optional

class MessagingEntry(BaseModel):
    sender: dict
    recipient: dict
    timestamp: int
    message: Optional[dict]
    # you can add other fields like delivery, read, postback, etc.

class WebhookEntry(BaseModel):
    id: str
    time: int
    messaging: List[MessagingEntry]

class WebhookPayload(BaseModel):
    object: str
    entry: List[WebhookEntry]

@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload, request: Request):
    # Acknowledge receipt immediately
    # FastAPI will auto-return 200 OK if no exception is raised
    if payload.object != "page":
        return {"status": "ignored"}

    for entry in payload.entry:
        for change in entry.messaging:
            if change.message and change.sender:
                # Log the full event for inspection
                print("Received IG DM event:", change.json())
                # TODO: enqueue to SQS / process video jobs
    return {"status": "processed"}