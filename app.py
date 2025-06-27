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
load_dotenv()
VERIFY_TOKEN        = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")
PAGE_ACCESS_TOKEN   = os.getenv("PAGE_ACCESS_TOKEN")
SQS_QUEUE_URL       = os.getenv("SQS_QUEUE_URL")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS SQS client (uses AWS_REGION or AWS_DEFAULT_REGION)
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

# Data models (for validation, optional)
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
    data = await request.json()
    logger.info("Webhook payload: %s", data)

    # Only handle Instagram/page objects
    if data.get("object") not in ("page", "instagram"):
        return {"status": "ignored"}

    for entry in data.get("entry", []):
        # Grab inline messaging events if present
        messaging_events = entry.get("messaging", [])
        if not messaging_events:
            # Fallback for change-based payloads
            for change in entry.get("changes", []):
                messaging_events.extend(change.get("value", {}).get("messaging", []))

        for event in messaging_events:
            sender_id = event.get("sender", {}).get("id")
            message   = event.get("message", {}) or {}
            mid       = message.get("mid")
            if not sender_id or not mid:
                continue

            # 1) Use inline attachments from webhook if available
            attachments = message.get("attachments", [])

            # 2) Fallback to Graph API fetch if no inline attachments
            if not attachments:
                try:
                    resp = requests.get(
                        f"https://graph.facebook.com/v17.0/{mid}",
                        params={
                            "fields": "attachments{type,payload,url}",
                            "access_token": PAGE_ACCESS_TOKEN,
                        },
                        timeout=10,
                    )
                    resp.raise_for_status()
                    attachments = resp.json().get("attachments", [])
                except Exception as e:
                    logger.error("Error fetching attachments for %s: %s", mid, e)
                    attachments = []

            # 3) Enqueue jobs for video or reel attachments, capturing caption
            for att in attachments:
                att_type = att.get("type")
                if att_type in ("video", "ig_reel"):
                    payload    = att.get("payload", {}) or {}
                    video_url  = payload.get("url")
                    # user-typed text fallback
                    user_text  = message.get("text", "")
                    # for reels, the post's caption lives in payload.title
                    reel_title = payload.get("title", "")
                    # prefer the Reel's caption, otherwise user text
                    caption    = reel_title or user_text

                    if video_url:
                        job = {
                            "video_url":  video_url,
                            "sender_id":  sender_id,
                            "message_id": mid,
                            "caption":    caption,
                        }
                        logger.info("Enqueuing job with caption: %r", caption)
                        try:
                            sqs.send_message(
                                QueueUrl=SQS_QUEUE_URL,
                                MessageBody=json.dumps(job)
                            )
                            logger.info("Enqueued job: %s", job)
                        except Exception as e:
                            logger.error("Failed to enqueue job %s: %s", job, e)

    return {"status": "processed"}