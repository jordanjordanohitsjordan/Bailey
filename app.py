import os
from fastapi import FastAPI, HTTPException, Request, Query
from dotenv import load_dotenv

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

# Incoming events handler (POST)
@app.post("/webhook")
async def receive_webhook(request: Request):
    # Parse the raw payload
    data = await request.json()
    print("Webhook payload:", data)

    # Only process page events
    if data.get("object") != "page":
        return {"status": "ignored"}

    for entry in data.get("entry", []):
        # First, look for direct messaging events
        messaging_events = entry.get("messaging", [])

        # Fallback: some payloads use changes[].value.messaging
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