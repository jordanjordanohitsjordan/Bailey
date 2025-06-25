import os
import json
import logging
from fastapi import FastAPI, HTTPException, Request, Query
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
import requests
import boto3

# Load environment variables
dotenv_loaded = load_dotenv()
VERIFY_TOKEN = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS SQS client
sqs = boto3.client("sqs")

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
    logger.info("Webhook payload: %s", data)

    # Only process relevant object types
    if data.get("object") not in ("page", "instagram"):
        return {"status": "ignored"}

    # Iterate through each entry
    for entry in data.get("entry", []):
        # Extract messaging events directly or via changes
        messaging_events = entry.get("messaging", [])
        if not messaging_events:
            for change in entry.get("changes", []):
                messaging_events.extend(change.get("value", {}).get("messaging", []))

        # Process each messaging event
        for event in messaging_events:
            sender = event.get("sender", {}).get("id")
            message = event.get("message")
            if not sender or not message:
                continue

            mid = message.get("mid")
            # Fetch attachments for this message
            try:
                resp = requests.get(
                    f"https://graph.facebook.com/v17.0/{mid}",
                    params={
                        "fields": "attachments{type,payload,url}",
                        "access_token": PAGE_ACCESS_TOKEN,
                    },
                )
                resp.raise_for_status()
            except Exception as e:
                logger.error("Error fetching attachments for %s: %s", mid, e)
                continue

            attachments = resp.json().get("attachments", [])
            # Enqueue jobs for video attachments
            for att in attachments:
                if att.get("type") == "video":
                    video_url = att.get("payload", {}).get("url")
                    job = {"video_url": video_url, "sender_id": sender, "message_id": mid}
                    try:
                        sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(job))
                        logger.info("Enqueued job: %s", job)
                    except Exception as e:
                        logger.error("Failed to enqueue job %s: %s", job, e)

    return {"status": "processed"}