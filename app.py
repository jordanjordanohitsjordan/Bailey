import os
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv

load_dotenv()  # loads IG_WEBHOOK_VERIFY_TOKEN

VERIFY_TOKEN = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")

app = FastAPI()

@app.get("/webhook")
async def verify_webhook(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    if hub_mode == "subscribe" and hub_verify_token == VERIFY_TOKEN:
        return int(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification token mismatch")